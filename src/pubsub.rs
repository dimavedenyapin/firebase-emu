//! Local google.pubsub.v1 Publisher and Subscriber gRPC services.
//!
//! The broker deliberately implements a bounded, at-least-once subset. Every
//! subscription owns an independent delivery row. Persistent mode uses SQLite
//! as the source of truth; only stream bookkeeping and wakeups live in memory.

use crate::{persistence::Persistence, BoxError};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use prost::Message;
use rusqlite::{params, OptionalExtension};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, Mutex, Notify};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Request, Response, Status};

pub(crate) mod google {
    pub mod pubsub {
        pub mod v1 {
            tonic::include_proto!("google.pubsub.v1");
        }
    }
}

use google::pubsub::v1::{
    publisher_server::{Publisher, PublisherServer},
    subscriber_server::{Subscriber, SubscriberServer},
    *,
};

const MAX_MESSAGE_BYTES: usize = 10 * 1024 * 1024;
const MAX_PUBLISH_BYTES: usize = 10 * 1024 * 1024;
// The public limit applies before the server adds message_id and publish_time.
// Reserve bounded response space for those fields and delivery wrappers.
const MAX_DELIVERY_MESSAGE_BYTES: usize = MAX_MESSAGE_BYTES + 1024;
const MAX_TRANSPORT_BYTES: usize = 11 * 1024 * 1024;
const MAX_PENDING_DELIVERIES: i64 = 10_000;
const MAX_PENDING_BYTES: i64 = 64 * 1024 * 1024;
const RETENTION_SECONDS: i64 = 24 * 60 * 60;
const DEFAULT_ACK_DEADLINE: i32 = 10;
const MAX_PAGE_SIZE: i32 = 1_000;

#[derive(Clone)]
struct Delivery {
    subscription: String,
    state: DeliveryState,
    ack_id: Option<String>,
    deadline_ms: Option<i64>,
    attempts: i32,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum DeliveryState {
    Available,
    InFlight,
}

#[derive(Default)]
struct MemoryState {
    topics: BTreeMap<String, Topic>,
    subscriptions: BTreeMap<String, Subscription>,
    messages: BTreeMap<String, PubsubMessage>,
    message_topics: BTreeMap<String, String>,
    message_sizes: BTreeMap<String, i64>,
    message_expiry: BTreeMap<String, i64>,
    deliveries: BTreeMap<(String, String), Delivery>,
}

#[derive(Clone)]
pub(crate) struct PubSubService {
    persistence: Option<Persistence>,
    memory: Arc<Mutex<MemoryState>>,
    notify: Arc<Notify>,
}

#[derive(Clone)]
pub(crate) struct BrokerMessage {
    pub ack_id: String,
    pub message: PubsubMessage,
    pub size_bytes: i64,
}

pub(crate) fn message_json(message: &PubsubMessage) -> serde_json::Value {
    use base64::engine::general_purpose::STANDARD;
    let publish_time = message
        .publish_time
        .as_ref()
        .map(timestamp_rfc3339)
        .unwrap_or_else(|| "1970-01-01T00:00:00.000Z".into());
    serde_json::json!({
        "data": STANDARD.encode(&message.data),
        "attributes": message.attributes,
        "messageId": message.message_id,
        "publishTime": publish_time,
        "orderingKey": message.ordering_key,
    })
}

fn timestamp_rfc3339(value: &prost_types::Timestamp) -> String {
    let seconds = value.seconds.max(0);
    let days = seconds / 86_400;
    let day_seconds = seconds % 86_400;
    // Howard Hinnant's civil_from_days, with Unix epoch adjustment.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        day_seconds / 3_600,
        (day_seconds % 3_600) / 60,
        day_seconds % 60,
        value.nanos.max(0) / 1_000_000
    )
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn timestamp_now() -> prost_types::Timestamp {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    prost_types::Timestamp {
        seconds: duration.as_secs().try_into().unwrap_or(i64::MAX),
        nanos: duration.subsec_nanos().try_into().unwrap_or_default(),
    }
}

fn status(error: impl std::fmt::Display) -> Status {
    Status::internal(format!("Pub/Sub persistence error: {error}"))
}

fn parse_resource<'a>(value: &'a str, kind: &str) -> Result<(&'a str, &'a str), Status> {
    let mut parts = value.split('/');
    let valid = parts.next() == Some("projects");
    let project = parts.next().unwrap_or_default();
    let valid = valid && parts.next() == Some(kind);
    let id = parts.next().unwrap_or_default();
    if !valid || parts.next().is_some() || project.is_empty() || !valid_id(id) {
        return Err(Status::invalid_argument(format!(
            "expected projects/{{project}}/{kind}/{{id}}"
        )));
    }
    Ok((project, id))
}

fn parse_project(value: &str) -> Result<&str, Status> {
    let mut parts = value.split('/');
    if parts.next() != Some("projects") {
        return Err(Status::invalid_argument("expected projects/{project}"));
    }
    let project = parts.next().unwrap_or_default();
    if project.is_empty() || parts.next().is_some() {
        return Err(Status::invalid_argument("expected projects/{project}"));
    }
    Ok(project)
}

fn valid_id(id: &str) -> bool {
    (3..=255).contains(&id.len())
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.~+%".contains(&byte))
        && id.as_bytes()[0].is_ascii_alphabetic()
        && !id.to_ascii_lowercase().starts_with("goog")
}

fn topic_supported(topic: &Topic) -> Result<(), Status> {
    if topic.message_storage_policy.is_some()
        || !topic.kms_key_name.is_empty()
        || topic.schema_settings.is_some()
        || topic.message_retention_duration.is_some()
        || !topic.ingestion_data_source_settings.is_empty()
    {
        return Err(Status::unimplemented(
            "topic storage policies, KMS, schemas, ingestion, and configurable retention are unsupported",
        ));
    }
    Ok(())
}

fn normalize_subscription(mut subscription: Subscription) -> Result<Subscription, Status> {
    if subscription.push_config.is_some() {
        return Err(Status::unimplemented(
            "push subscriptions are unsupported; use a pull subscription",
        ));
    }
    if subscription.bigquery_config.is_some() || subscription.cloud_storage_config.is_some() {
        return Err(Status::unimplemented(
            "BigQuery and Cloud Storage subscriptions are unsupported",
        ));
    }
    if subscription.enable_message_ordering
        || subscription.retain_acked_messages
        || subscription.message_retention_duration.is_some()
        || subscription.expiration_policy.is_some()
        || !subscription.filter.is_empty()
        || subscription.dead_letter_policy.is_some()
        || subscription.retry_policy.is_some()
        || subscription.enable_exactly_once_delivery
        || subscription.topic_message_retention_duration.is_some()
        || !subscription.analytics_hub_subscription_info.is_empty()
    {
        return Err(Status::unimplemented(
            "ordering, filters, snapshots/retention, dead-letter/retry policies, and exactly-once delivery are unsupported",
        ));
    }
    subscription.ack_deadline_seconds = match subscription.ack_deadline_seconds {
        0 => DEFAULT_ACK_DEADLINE,
        10..=600 => subscription.ack_deadline_seconds,
        _ => {
            return Err(Status::invalid_argument(
                "ack_deadline_seconds must be zero or between 10 and 600",
            ))
        }
    };
    Ok(subscription)
}

fn page_bounds(page_size: i32, token: &str) -> Result<(usize, Option<String>), Status> {
    if page_size < 0 {
        return Err(Status::invalid_argument("page_size must not be negative"));
    }
    let size = if page_size == 0 {
        100
    } else {
        page_size.min(MAX_PAGE_SIZE)
    } as usize;
    let after = if token.is_empty() {
        None
    } else {
        Some(
            String::from_utf8(
                URL_SAFE_NO_PAD
                    .decode(token)
                    .map_err(|_| Status::invalid_argument("invalid page_token"))?,
            )
            .map_err(|_| Status::invalid_argument("invalid page_token"))?,
        )
    };
    Ok((size, after))
}

fn paginate<T, F>(
    mut values: Vec<T>,
    page_size: i32,
    token: &str,
    name: F,
) -> Result<(Vec<T>, String), Status>
where
    F: Fn(&T) -> &str,
{
    values.sort_by(|left, right| name(left).cmp(name(right)));
    let (size, after) = page_bounds(page_size, token)?;
    if let Some(after) = after {
        values.retain(|value| name(value) > after.as_str());
    }
    let has_more = values.len() > size;
    values.truncate(size);
    let next = if has_more {
        values
            .last()
            .map(|value| URL_SAFE_NO_PAD.encode(name(value)))
            .unwrap_or_default()
    } else {
        String::new()
    };
    Ok((values, next))
}

impl PubSubService {
    pub(crate) fn new(persistence: Option<Persistence>) -> Self {
        Self {
            persistence,
            memory: Arc::new(Mutex::new(MemoryState::default())),
            notify: Arc::new(Notify::new()),
        }
    }

    async fn create_topic_value(&self, topic: Topic) -> Result<Topic, Status> {
        parse_resource(&topic.name, "topics")?;
        topic_supported(&topic)?;
        if let Some(persistence) = &self.persistence {
            let saved = topic.clone();
            persistence
                .write(move |connection| {
                    connection
                        .execute(
                            "INSERT INTO pubsub_topics(name, topic) VALUES (?1, ?2)",
                            params![saved.name, saved.encode_to_vec()],
                        )
                        .map_err(crate::persistence::Error::from)?;
                    Ok(())
                })
                .await
                .map_err(|error| match error {
                    crate::persistence::Error::Sqlite(rusqlite::Error::SqliteFailure(code, _))
                        if code.code == rusqlite::ErrorCode::ConstraintViolation =>
                    {
                        Status::already_exists("topic already exists")
                    }
                    other => status(other),
                })?;
        } else {
            let mut state = self.memory.lock().await;
            if state.topics.contains_key(&topic.name) {
                return Err(Status::already_exists("topic already exists"));
            }
            state.topics.insert(topic.name.clone(), topic.clone());
        }
        Ok(topic)
    }

    async fn get_topic_value(&self, name: &str) -> Result<Topic, Status> {
        parse_resource(name, "topics")?;
        if let Some(persistence) = &self.persistence {
            let name = name.to_owned();
            persistence
                .read(move |connection| {
                    let value: Option<Vec<u8>> = connection
                        .query_row(
                            "SELECT topic FROM pubsub_topics WHERE name=?1",
                            [name],
                            |row| row.get(0),
                        )
                        .optional()?;
                    value
                        .map(|value| {
                            Topic::decode(value.as_slice()).map_err(|error| {
                                crate::persistence::Error::Corrupt(error.to_string())
                            })
                        })
                        .transpose()
                })
                .await
                .map_err(status)?
                .ok_or_else(|| Status::not_found("topic not found"))
        } else {
            self.memory
                .lock()
                .await
                .topics
                .get(name)
                .cloned()
                .ok_or_else(|| Status::not_found("topic not found"))
        }
    }

    async fn list_topic_values(&self, project: &str) -> Result<Vec<Topic>, Status> {
        parse_project(project)?;
        let prefix = format!("{project}/topics/");
        if let Some(persistence) = &self.persistence {
            persistence
                .read(move |connection| {
                    let mut statement = connection.prepare(
                        "SELECT topic FROM pubsub_topics WHERE substr(name,1,length(?1))=?1 ORDER BY name",
                    )?;
                    let rows = statement.query_map([prefix], |row| row.get::<_, Vec<u8>>(0))?;
                    let mut topics = Vec::new();
                    for row in rows {
                        topics.push(Topic::decode(row?.as_slice()).map_err(|error| {
                            crate::persistence::Error::Corrupt(error.to_string())
                        })?);
                    }
                    Ok(topics)
                })
                .await
                .map_err(status)
        } else {
            Ok(self
                .memory
                .lock()
                .await
                .topics
                .range(prefix.clone()..)
                .take_while(|(name, _)| name.starts_with(&prefix))
                .map(|(_, topic)| topic.clone())
                .collect())
        }
    }

    async fn delete_topic_value(&self, name: &str) -> Result<(), Status> {
        parse_resource(name, "topics")?;
        if let Some(persistence) = &self.persistence {
            let name = name.to_owned();
            persistence
                .write(move |connection| {
                    let transaction = connection.transaction()?;
                    let changed = transaction.execute("DELETE FROM pubsub_topics WHERE name=?1", [&name])?;
                    if changed == 0 {
                        return Err(crate::persistence::Error::Sqlite(rusqlite::Error::QueryReturnedNoRows));
                    }
                    let mut affected = Vec::new();
                    {
                        let mut statement = transaction.prepare(
                            "SELECT name, subscription FROM pubsub_subscriptions WHERE topic_name=?1",
                        )?;
                        let rows = statement.query_map([&name], |row| {
                            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                        })?;
                        for row in rows {
                            affected.push(row?);
                        }
                    }
                    for (subscription_name, encoded) in affected {
                        let mut subscription = Subscription::decode(encoded.as_slice())
                            .map_err(|error| crate::persistence::Error::Corrupt(error.to_string()))?;
                        subscription.topic = "_deleted-topic_".into();
                        transaction.execute(
                            "UPDATE pubsub_subscriptions SET topic_name='_deleted-topic_', subscription=?2 WHERE name=?1",
                            params![subscription_name, subscription.encode_to_vec()],
                        )?;
                    }
                    transaction.commit()?;
                    Ok(())
                })
                .await
                .map_err(|error| match error {
                    crate::persistence::Error::Sqlite(rusqlite::Error::QueryReturnedNoRows) => Status::not_found("topic not found"),
                    other => status(other),
                })?;
        } else {
            let mut state = self.memory.lock().await;
            if state.topics.remove(name).is_none() {
                return Err(Status::not_found("topic not found"));
            }
            for subscription in state
                .subscriptions
                .values_mut()
                .filter(|sub| sub.topic == name)
            {
                subscription.topic = "_deleted-topic_".into();
            }
        }
        Ok(())
    }

    async fn create_subscription_value(
        &self,
        subscription: Subscription,
    ) -> Result<Subscription, Status> {
        let subscription = normalize_subscription(subscription)?;
        let (subscription_project, _) = parse_resource(&subscription.name, "subscriptions")?;
        let (topic_project, _) = parse_resource(&subscription.topic, "topics")?;
        if subscription_project != topic_project {
            return Err(Status::invalid_argument(
                "cross-project subscriptions are outside local emulator scope",
            ));
        }
        if let Some(persistence) = &self.persistence {
            let saved = subscription.clone();
            persistence
                .write(move |connection| {
                    let transaction = connection.transaction()?;
                    let topic_exists: bool = transaction.query_row(
                        "SELECT EXISTS(SELECT 1 FROM pubsub_topics WHERE name=?1)",
                        [&saved.topic],
                        |row| row.get(0),
                    )?;
                    if !topic_exists {
                        return Err(crate::persistence::Error::Sqlite(
                            rusqlite::Error::QueryReturnedNoRows,
                        ));
                    }
                    transaction.execute(
                        "INSERT INTO pubsub_subscriptions(name, topic_name, subscription) VALUES (?1, ?2, ?3)",
                        params![saved.name, saved.topic, saved.encode_to_vec()],
                    )?;
                    transaction.commit()?;
                    Ok(())
                })
                .await
                .map_err(|error| match error {
                    crate::persistence::Error::Sqlite(rusqlite::Error::QueryReturnedNoRows) =>
                    {
                        Status::not_found("topic not found")
                    }
                    crate::persistence::Error::Sqlite(rusqlite::Error::SqliteFailure(code, _))
                        if code.code == rusqlite::ErrorCode::ConstraintViolation => Status::already_exists("subscription already exists"),
                    other => status(other),
                })?;
        } else {
            let mut state = self.memory.lock().await;
            if !state.topics.contains_key(&subscription.topic) {
                return Err(Status::not_found("topic not found"));
            }
            if state.subscriptions.contains_key(&subscription.name) {
                return Err(Status::already_exists("subscription already exists"));
            }
            state
                .subscriptions
                .insert(subscription.name.clone(), subscription.clone());
        }
        Ok(subscription)
    }

    async fn get_subscription_value(&self, name: &str) -> Result<Subscription, Status> {
        parse_resource(name, "subscriptions")?;
        if let Some(persistence) = &self.persistence {
            let name = name.to_owned();
            persistence
                .read(move |connection| {
                    let value: Option<Vec<u8>> = connection
                        .query_row(
                            "SELECT subscription FROM pubsub_subscriptions WHERE name=?1",
                            [name],
                            |row| row.get(0),
                        )
                        .optional()?;
                    value
                        .map(|value| {
                            Subscription::decode(value.as_slice()).map_err(|error| {
                                crate::persistence::Error::Corrupt(error.to_string())
                            })
                        })
                        .transpose()
                })
                .await
                .map_err(status)?
                .ok_or_else(|| Status::not_found("subscription not found"))
        } else {
            self.memory
                .lock()
                .await
                .subscriptions
                .get(name)
                .cloned()
                .ok_or_else(|| Status::not_found("subscription not found"))
        }
    }

    async fn list_subscription_values(&self, project: &str) -> Result<Vec<Subscription>, Status> {
        parse_project(project)?;
        let prefix = format!("{project}/subscriptions/");
        if let Some(persistence) = &self.persistence {
            persistence.read(move |connection| {
                let mut statement = connection.prepare("SELECT subscription FROM pubsub_subscriptions WHERE substr(name,1,length(?1))=?1 ORDER BY name")?;
                let rows = statement.query_map([prefix], |row| row.get::<_, Vec<u8>>(0))?;
                let mut values = Vec::new();
                for row in rows { values.push(Subscription::decode(row?.as_slice()).map_err(|error| crate::persistence::Error::Corrupt(error.to_string()))?); }
                Ok(values)
            }).await.map_err(status)
        } else {
            Ok(self
                .memory
                .lock()
                .await
                .subscriptions
                .range(prefix.clone()..)
                .take_while(|(name, _)| name.starts_with(&prefix))
                .map(|(_, value)| value.clone())
                .collect())
        }
    }

    async fn topic_subscription_names(&self, topic: &str) -> Result<Vec<String>, Status> {
        self.get_topic_value(topic).await?;
        if let Some(persistence) = &self.persistence {
            let topic = topic.to_owned();
            persistence
                .read(move |connection| {
                    let mut statement = connection.prepare(
                        "SELECT name FROM pubsub_subscriptions WHERE topic_name=?1 ORDER BY name",
                    )?;
                    let values = statement
                        .query_map([topic], |row| row.get(0))?
                        .collect::<Result<Vec<String>, _>>()?;
                    Ok(values)
                })
                .await
                .map_err(status)
        } else {
            Ok(self
                .memory
                .lock()
                .await
                .subscriptions
                .values()
                .filter(|value| value.topic == topic)
                .map(|value| value.name.clone())
                .collect())
        }
    }

    async fn delete_subscription_value(&self, name: &str) -> Result<(), Status> {
        parse_resource(name, "subscriptions")?;
        if let Some(persistence) = &self.persistence {
            let name = name.to_owned();
            persistence.write(move |connection| {
                let transaction = connection.transaction()?;
                let changed = transaction.execute("DELETE FROM pubsub_subscriptions WHERE name=?1", [&name])?;
                if changed == 0 { return Err(crate::persistence::Error::Sqlite(rusqlite::Error::QueryReturnedNoRows)); }
                transaction.execute("DELETE FROM pubsub_deliveries WHERE subscription_name=?1", [&name])?;
                transaction.execute("DELETE FROM pubsub_messages WHERE NOT EXISTS (SELECT 1 FROM pubsub_deliveries WHERE pubsub_deliveries.message_id=pubsub_messages.message_id)", [])?;
                transaction.commit()?;
                Ok(())
            }).await.map_err(|error| match error {
                crate::persistence::Error::Sqlite(rusqlite::Error::QueryReturnedNoRows) => Status::not_found("subscription not found"),
                other => status(other),
            })?;
        } else {
            let mut state = self.memory.lock().await;
            if state.subscriptions.remove(name).is_none() {
                return Err(Status::not_found("subscription not found"));
            }
            state
                .deliveries
                .retain(|(subscription, _), _| subscription != name);
            memory_gc(&mut state);
        }
        self.notify.notify_waiters();
        Ok(())
    }

    async fn publish_values(
        &self,
        topic: &str,
        mut messages: Vec<PubsubMessage>,
    ) -> Result<Vec<String>, Status> {
        parse_resource(topic, "topics")?;
        if messages.is_empty() {
            return Err(Status::invalid_argument("messages must not be empty"));
        }
        let mut request_bytes = 0usize;
        for message in &messages {
            if message.data.is_empty() && message.attributes.is_empty() {
                return Err(Status::invalid_argument(
                    "each message needs data or attributes",
                ));
            }
            if !message.message_id.is_empty() || message.publish_time.is_some() {
                return Err(Status::invalid_argument(
                    "message_id and publish_time are server assigned",
                ));
            }
            if !message.ordering_key.is_empty() {
                return Err(Status::unimplemented("ordering keys are unsupported"));
            }
            let size = message.encoded_len();
            if size > MAX_MESSAGE_BYTES {
                return Err(Status::resource_exhausted("message exceeds 10 MiB"));
            }
            request_bytes = request_bytes.saturating_add(size);
        }
        if request_bytes > MAX_PUBLISH_BYTES {
            return Err(Status::resource_exhausted("publish request exceeds 10 MiB"));
        }
        let published = timestamp_now();
        let published_ms = now_ms();
        let expiry = published_ms.saturating_add(RETENTION_SECONDS * 1_000);
        let mut ids = Vec::with_capacity(messages.len());
        for message in &mut messages {
            message.message_id = uuid::Uuid::new_v4().to_string();
            message.publish_time = Some(published);
            if message.encoded_len() > MAX_DELIVERY_MESSAGE_BYTES {
                return Err(Status::resource_exhausted(
                    "message exceeds delivery response budget",
                ));
            }
            ids.push(message.message_id.clone());
        }
        if let Some(persistence) = &self.persistence {
            let topic = topic.to_owned();
            let persisted_messages = messages.clone();
            persistence.write(move |connection| {
                let transaction = connection.transaction()?;
                cleanup_sql(&transaction, published_ms)?;
                let topic_exists: bool = transaction.query_row(
                    "SELECT EXISTS(SELECT 1 FROM pubsub_topics WHERE name=?1)",
                    [&topic],
                    |row| row.get(0),
                )?;
                if !topic_exists {
                    return Err(crate::persistence::Error::Sqlite(rusqlite::Error::QueryReturnedNoRows));
                }
                let subscriptions = {
                    let mut statement = transaction.prepare(
                        "SELECT name FROM pubsub_subscriptions WHERE topic_name=?1 ORDER BY name",
                    )?;
                    let values = statement
                        .query_map([&topic], |row| row.get::<_, String>(0))?
                        .collect::<Result<Vec<_>, _>>()?;
                    values
                };
                if subscriptions.is_empty() {
                    transaction.commit()?;
                    return Ok(());
                }
                let added_count = (subscriptions.len() * persisted_messages.len()) as i64;
                let added_bytes = persisted_messages
                    .iter()
                    .map(|message| message.encoded_len() as i64)
                    .sum::<i64>()
                    * subscriptions.len() as i64;
                let (count, bytes): (i64,i64) = transaction.query_row(
                    "SELECT count(*), coalesce(sum(pubsub_messages.size_bytes),0) FROM pubsub_deliveries JOIN pubsub_messages USING(message_id)", [], |row| Ok((row.get(0)?,row.get(1)?)))?;
                if count.saturating_add(added_count) > MAX_PENDING_DELIVERIES || bytes.saturating_add(added_bytes) > MAX_PENDING_BYTES {
                    return Err(crate::persistence::Error::Sqlite(rusqlite::Error::ExecuteReturnedResults));
                }
                for message in &persisted_messages {
                    let encoded = message.encode_to_vec();
                    transaction.execute("INSERT INTO pubsub_messages(message_id,topic_name,message,size_bytes,published_at,expire_at) VALUES (?1,?2,?3,?4,?5,?6)", params![message.message_id, topic, encoded, message.encoded_len() as i64, published_ms, expiry])?;
                    for subscription in &subscriptions {
                        transaction.execute("INSERT INTO pubsub_deliveries(subscription_name,message_id,state,attempts) VALUES (?1,?2,'available',0)", params![subscription,message.message_id])?;
                    }
                }
                transaction.commit()?;
                Ok(())
            }).await.map_err(|error| match error {
                crate::persistence::Error::Sqlite(rusqlite::Error::QueryReturnedNoRows) => Status::not_found("topic not found"),
                crate::persistence::Error::Sqlite(rusqlite::Error::ExecuteReturnedResults) => Status::resource_exhausted("Pub/Sub backlog capacity exceeded"),
                other => status(other),
            })?;
        } else {
            let mut state = self.memory.lock().await;
            cleanup_memory(&mut state, published_ms);
            if !state.topics.contains_key(topic) {
                return Err(Status::not_found("topic not found"));
            }
            let subscriptions: Vec<_> = state
                .subscriptions
                .values()
                .filter(|subscription| subscription.topic == topic)
                .map(|subscription| subscription.name.clone())
                .collect();
            if subscriptions.is_empty() {
                return Ok(ids);
            }
            let added_count = (subscriptions.len() * messages.len()) as i64;
            let added_bytes = messages
                .iter()
                .map(|message| message.encoded_len() as i64)
                .sum::<i64>()
                * subscriptions.len() as i64;
            let (count, bytes) = memory_usage(&state);
            if count.saturating_add(added_count) > MAX_PENDING_DELIVERIES
                || bytes.saturating_add(added_bytes) > MAX_PENDING_BYTES
            {
                return Err(Status::resource_exhausted(
                    "Pub/Sub backlog capacity exceeded",
                ));
            }
            for message in messages {
                let id = message.message_id.clone();
                state.message_topics.insert(id.clone(), topic.to_owned());
                state
                    .message_sizes
                    .insert(id.clone(), message.encoded_len() as i64);
                state.message_expiry.insert(id.clone(), expiry);
                state.messages.insert(id.clone(), message);
                for subscription in &subscriptions {
                    state.deliveries.insert(
                        (subscription.clone(), id.clone()),
                        Delivery {
                            subscription: subscription.clone(),
                            state: DeliveryState::Available,
                            ack_id: None,
                            deadline_ms: None,
                            attempts: 0,
                        },
                    );
                }
            }
        }
        self.notify.notify_waiters();
        Ok(ids)
    }

    pub(crate) async fn pull_one(
        &self,
        subscription: &str,
    ) -> Result<Option<BrokerMessage>, Status> {
        let mut values = self
            .claim(subscription, 1, MAX_DELIVERY_MESSAGE_BYTES as i64, None)
            .await?;
        Ok(values.pop())
    }

    pub(crate) async fn pull_one_wait(&self, subscription: &str) -> Result<BrokerMessage, Status> {
        loop {
            // Register before checking the queue so a publish cannot be lost in
            // the gap between an empty read and waiting for the wake-up.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            if let Some(message) = self.pull_one(subscription).await? {
                return Ok(message);
            }
            tokio::select! {
                _ = &mut notified => {}
                // Also revisit expired leases when there is no new publish.
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }

    async fn claim(
        &self,
        subscription: &str,
        max_count: usize,
        max_bytes: i64,
        deadline_override: Option<i32>,
    ) -> Result<Vec<BrokerMessage>, Status> {
        let configured = self
            .get_subscription_value(subscription)
            .await?
            .ack_deadline_seconds;
        let deadline_seconds = deadline_override.unwrap_or(configured).clamp(10, 600);
        let now = now_ms();
        let deadline = now.saturating_add(i64::from(deadline_seconds) * 1_000);
        if let Some(persistence) = &self.persistence {
            let subscription = subscription.to_owned();
            persistence.write(move |connection| {
                let transaction = connection.transaction()?;
                cleanup_sql(&transaction, now)?;
                transaction.execute("UPDATE pubsub_deliveries SET state='available',ack_id=NULL,deadline=NULL WHERE subscription_name=?1 AND state='in_flight' AND deadline<=?2", params![subscription,now])?;
                let mut candidates = Vec::new();
                {
                    let mut statement = transaction.prepare("SELECT d.message_id,m.message,m.size_bytes,d.attempts FROM pubsub_deliveries d JOIN pubsub_messages m USING(message_id) WHERE d.subscription_name=?1 AND d.state='available' ORDER BY m.published_at,m.message_id LIMIT ?2")?;
                    let candidate_limit = max_count.saturating_mul(4).clamp(16, 1_000);
                    let rows = statement.query_map(params![subscription,candidate_limit as i64], |row| Ok((row.get::<_,String>(0)?,row.get::<_,Vec<u8>>(1)?,row.get::<_,i64>(2)?,row.get::<_,i32>(3)?)))?;
                    for row in rows { candidates.push(row?); }
                }
                let mut result = Vec::new();
                let mut used = 0i64;
                let has_fitting_message = candidates
                    .iter()
                    .any(|(_, _, size, _)| *size <= max_bytes);
                for (message_id, encoded, size, attempts) in candidates {
                    if result.len() >= max_count { break; }
                    if size > MAX_DELIVERY_MESSAGE_BYTES as i64 {
                        return Err(crate::persistence::Error::Corrupt(format!("Pub/Sub message {message_id} exceeds delivery response budget")));
                    }
                    // Byte flow control is a target. If nothing fits, allow one
                    // valid message so it cannot stall the subscription forever.
                    if used.saturating_add(size) > max_bytes
                        && (!result.is_empty() || has_fitting_message)
                    {
                        continue;
                    }
                    let ack_id = uuid::Uuid::new_v4().to_string();
                    let changed = transaction.execute("UPDATE pubsub_deliveries SET state='in_flight',ack_id=?3,deadline=?4,attempts=attempts+1 WHERE subscription_name=?1 AND message_id=?2 AND state='available'", params![subscription,message_id,ack_id,deadline])?;
                    if changed == 1 {
                        result.push(BrokerMessage { ack_id, message: PubsubMessage::decode(encoded.as_slice()).map_err(|error| crate::persistence::Error::Corrupt(error.to_string()))?, size_bytes: size });
                        let _ = attempts;
                        used += size;
                    }
                }
                transaction.commit()?;
                Ok(result)
            }).await.map_err(status)
        } else {
            let mut state = self.memory.lock().await;
            cleanup_memory(&mut state, now);
            for delivery in state.deliveries.values_mut().filter(|delivery| {
                delivery.subscription == subscription
                    && delivery.state == DeliveryState::InFlight
                    && delivery.deadline_ms.is_some_and(|value| value <= now)
            }) {
                delivery.state = DeliveryState::Available;
                delivery.ack_id = None;
                delivery.deadline_ms = None;
            }
            let keys: Vec<_> = state
                .deliveries
                .iter()
                .filter(|((sub, _), delivery)| {
                    sub == subscription && delivery.state == DeliveryState::Available
                })
                .map(|(key, _)| key.clone())
                .collect();
            let mut result = Vec::new();
            let mut used = 0i64;
            let has_fitting_message = keys
                .iter()
                .any(|key| state.message_sizes.get(&key.1).copied().unwrap_or(0) <= max_bytes);
            for key in keys {
                if result.len() >= max_count {
                    break;
                }
                let size = *state.message_sizes.get(&key.1).unwrap_or(&0);
                if size > MAX_DELIVERY_MESSAGE_BYTES as i64 {
                    return Err(Status::internal(format!(
                        "Pub/Sub message {} exceeds delivery response budget",
                        key.1
                    )));
                }
                if used.saturating_add(size) > max_bytes
                    && (!result.is_empty() || has_fitting_message)
                {
                    continue;
                }
                let message = state
                    .messages
                    .get(&key.1)
                    .cloned()
                    .ok_or_else(|| Status::internal("orphan Pub/Sub delivery"))?;
                let ack_id = uuid::Uuid::new_v4().to_string();
                let delivery = state.deliveries.get_mut(&key).unwrap();
                delivery.state = DeliveryState::InFlight;
                delivery.ack_id = Some(ack_id.clone());
                delivery.deadline_ms = Some(deadline);
                delivery.attempts += 1;
                result.push(BrokerMessage {
                    ack_id,
                    message,
                    size_bytes: size,
                });
                used += size;
            }
            Ok(result)
        }
    }

    pub(crate) async fn acknowledge_ids(
        &self,
        subscription: &str,
        ack_ids: &[String],
    ) -> Result<(), Status> {
        self.get_subscription_value(subscription).await?;
        if ack_ids.is_empty() {
            return Err(Status::invalid_argument("ack_ids must not be empty"));
        }
        if let Some(persistence) = &self.persistence {
            let subscription = subscription.to_owned();
            let ack_ids = ack_ids.to_vec();
            persistence.write(move |connection| {
                let transaction = connection.transaction()?;
                for ack_id in ack_ids { transaction.execute("DELETE FROM pubsub_deliveries WHERE subscription_name=?1 AND ack_id=?2", params![subscription,ack_id])?; }
                transaction.execute("DELETE FROM pubsub_messages WHERE NOT EXISTS (SELECT 1 FROM pubsub_deliveries WHERE pubsub_deliveries.message_id=pubsub_messages.message_id)", [])?;
                transaction.commit()?; Ok(())
            }).await.map_err(status)?;
        } else {
            let mut state = self.memory.lock().await;
            state.deliveries.retain(|_, delivery| {
                delivery.subscription != subscription
                    || delivery
                        .ack_id
                        .as_ref()
                        .is_none_or(|ack| !ack_ids.contains(ack))
            });
            memory_gc(&mut state);
        }
        self.notify.notify_waiters();
        Ok(())
    }

    pub(crate) async fn modify_ack_ids(
        &self,
        subscription: &str,
        ack_ids: &[String],
        seconds: i32,
    ) -> Result<(), Status> {
        self.get_subscription_value(subscription).await?;
        if ack_ids.is_empty() {
            return Err(Status::invalid_argument("ack_ids must not be empty"));
        }
        if !(0..=600).contains(&seconds) {
            return Err(Status::invalid_argument(
                "ack deadline must be between 0 and 600",
            ));
        }
        let deadline = now_ms().saturating_add(i64::from(seconds) * 1_000);
        if let Some(persistence) = &self.persistence {
            let subscription = subscription.to_owned();
            let ack_ids = ack_ids.to_vec();
            persistence.write(move |connection| {
                let transaction = connection.transaction()?;
                for ack_id in ack_ids {
                    if seconds == 0 { transaction.execute("UPDATE pubsub_deliveries SET state='available',ack_id=NULL,deadline=NULL WHERE subscription_name=?1 AND ack_id=?2", params![subscription,ack_id])?; }
                    else { transaction.execute("UPDATE pubsub_deliveries SET deadline=?3 WHERE subscription_name=?1 AND ack_id=?2 AND state='in_flight'", params![subscription,ack_id,deadline])?; }
                }
                transaction.commit()?; Ok(())
            }).await.map_err(status)?;
        } else {
            let mut state = self.memory.lock().await;
            for delivery in state.deliveries.values_mut().filter(|delivery| {
                delivery.subscription == subscription
                    && delivery
                        .ack_id
                        .as_ref()
                        .is_some_and(|ack| ack_ids.contains(ack))
            }) {
                if seconds == 0 {
                    delivery.state = DeliveryState::Available;
                    delivery.ack_id = None;
                    delivery.deadline_ms = None;
                } else {
                    delivery.deadline_ms = Some(deadline);
                }
            }
        }
        self.notify.notify_waiters();
        Ok(())
    }

    async fn active_ack_ids(
        &self,
        subscription: &str,
        ack_ids: &[String],
    ) -> Result<HashSet<String>, Status> {
        self.get_subscription_value(subscription).await?;
        if ack_ids.is_empty() {
            return Ok(HashSet::new());
        }
        let requested: HashSet<_> = ack_ids.iter().cloned().collect();
        let now = now_ms();
        if let Some(persistence) = &self.persistence {
            let subscription = subscription.to_owned();
            persistence
                .read(move |connection| {
                    let mut statement = connection.prepare(
                        "SELECT ack_id FROM pubsub_deliveries \
                         WHERE subscription_name=?1 AND state='in_flight' \
                         AND ack_id IS NOT NULL AND deadline>?2",
                    )?;
                    let rows = statement
                        .query_map(params![subscription, now], |row| row.get::<_, String>(0))?;
                    let mut active = HashSet::new();
                    for row in rows {
                        let ack_id = row?;
                        if requested.contains(&ack_id) {
                            active.insert(ack_id);
                        }
                    }
                    Ok(active)
                })
                .await
                .map_err(status)
        } else {
            let state = self.memory.lock().await;
            Ok(state
                .deliveries
                .values()
                .filter(|delivery| {
                    delivery.subscription == subscription
                        && delivery.state == DeliveryState::InFlight
                        && delivery.deadline_ms.is_some_and(|deadline| deadline > now)
                })
                .filter_map(|delivery| delivery.ack_id.as_ref())
                .filter(|ack_id| requested.contains(*ack_id))
                .cloned()
                .collect())
        }
    }

    pub(crate) async fn pending_count(&self, subscriptions: &[String]) -> Result<i64, Status> {
        if subscriptions.is_empty() {
            return Ok(0);
        }
        if let Some(persistence) = &self.persistence {
            let names = subscriptions.to_vec();
            persistence
                .read(move |connection| {
                    let mut count = 0;
                    for name in names {
                        count += connection.query_row(
                            "SELECT count(*) FROM pubsub_deliveries WHERE subscription_name=?1",
                            [name],
                            |row| row.get::<_, i64>(0),
                        )?;
                    }
                    Ok(count)
                })
                .await
                .map_err(status)
        } else {
            Ok(self
                .memory
                .lock()
                .await
                .deliveries
                .values()
                .filter(|delivery| subscriptions.contains(&delivery.subscription))
                .count() as i64)
        }
    }

    pub(crate) async fn ensure_trigger_subscription(
        &self,
        project: &str,
        topic_id: &str,
        subscription_id: &str,
    ) -> Result<String, Status> {
        let topic = format!("projects/{project}/topics/{topic_id}");
        match self.get_topic_value(&topic).await {
            Ok(_) => {}
            Err(error) if error.code() == tonic::Code::NotFound => {
                self.create_topic_value(Topic {
                    name: topic.clone(),
                    ..Default::default()
                })
                .await?;
            }
            Err(error) => return Err(error),
        }
        let subscription = format!("projects/{project}/subscriptions/{subscription_id}");
        match self.get_subscription_value(&subscription).await {
            Ok(existing) if existing.topic == topic => {}
            Ok(_) => {
                return Err(Status::failed_precondition(
                    "trigger subscription exists for a different topic",
                ))
            }
            Err(error) if error.code() == tonic::Code::NotFound => {
                self.create_subscription_value(Subscription {
                    name: subscription.clone(),
                    topic,
                    ack_deadline_seconds: DEFAULT_ACK_DEADLINE,
                    ..Default::default()
                })
                .await?;
            }
            Err(error) => return Err(error),
        }
        Ok(subscription)
    }

    async fn release_ids(&self, subscription: &str, ack_ids: Vec<String>) {
        if !ack_ids.is_empty() {
            let _ = self.modify_ack_ids(subscription, &ack_ids, 0).await;
        }
    }
}

fn cleanup_sql(
    transaction: &rusqlite::Transaction<'_>,
    now: i64,
) -> Result<(), crate::persistence::Error> {
    transaction.execute("DELETE FROM pubsub_messages WHERE expire_at<=?1", [now])?;
    Ok(())
}

fn memory_usage(state: &MemoryState) -> (i64, i64) {
    let count = state.deliveries.len() as i64;
    let bytes = state
        .deliveries
        .keys()
        .map(|(_, id)| state.message_sizes.get(id).copied().unwrap_or(0))
        .sum();
    (count, bytes)
}

fn cleanup_memory(state: &mut MemoryState, now: i64) {
    let expired: Vec<_> = state
        .message_expiry
        .iter()
        .filter(|(_, expiry)| **expiry <= now)
        .map(|(id, _)| id.clone())
        .collect();
    for id in expired {
        state.messages.remove(&id);
        state.message_topics.remove(&id);
        state.message_sizes.remove(&id);
        state.message_expiry.remove(&id);
        state.deliveries.retain(|(_, message), _| message != &id);
    }
}

fn memory_gc(state: &mut MemoryState) {
    let live: std::collections::BTreeSet<_> =
        state.deliveries.keys().map(|(_, id)| id.clone()).collect();
    state.messages.retain(|id, _| live.contains(id));
    state.message_topics.retain(|id, _| live.contains(id));
    state.message_sizes.retain(|id, _| live.contains(id));
    state.message_expiry.retain(|id, _| live.contains(id));
}

#[tonic::async_trait]
impl Publisher for PubSubService {
    async fn create_topic(&self, request: Request<Topic>) -> Result<Response<Topic>, Status> {
        Ok(Response::new(
            self.create_topic_value(request.into_inner()).await?,
        ))
    }
    async fn update_topic(
        &self,
        _request: Request<UpdateTopicRequest>,
    ) -> Result<Response<Topic>, Status> {
        Err(Status::unimplemented("UpdateTopic is unsupported"))
    }
    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        let request = request.into_inner();
        Ok(Response::new(PublishResponse {
            message_ids: self
                .publish_values(&request.topic, request.messages)
                .await?,
        }))
    }
    async fn get_topic(
        &self,
        request: Request<GetTopicRequest>,
    ) -> Result<Response<Topic>, Status> {
        Ok(Response::new(
            self.get_topic_value(&request.into_inner().topic).await?,
        ))
    }
    async fn list_topics(
        &self,
        request: Request<ListTopicsRequest>,
    ) -> Result<Response<ListTopicsResponse>, Status> {
        let request = request.into_inner();
        let (topics, next_page_token) = paginate(
            self.list_topic_values(&request.project).await?,
            request.page_size,
            &request.page_token,
            |value| &value.name,
        )?;
        Ok(Response::new(ListTopicsResponse {
            topics,
            next_page_token,
        }))
    }
    async fn list_topic_subscriptions(
        &self,
        request: Request<ListTopicSubscriptionsRequest>,
    ) -> Result<Response<ListTopicSubscriptionsResponse>, Status> {
        let request = request.into_inner();
        let (subscriptions, next_page_token) = paginate(
            self.topic_subscription_names(&request.topic).await?,
            request.page_size,
            &request.page_token,
            |value| value.as_str(),
        )?;
        Ok(Response::new(ListTopicSubscriptionsResponse {
            subscriptions,
            next_page_token,
        }))
    }
    async fn list_topic_snapshots(
        &self,
        _request: Request<ListTopicSnapshotsRequest>,
    ) -> Result<Response<ListTopicSnapshotsResponse>, Status> {
        Err(Status::unimplemented("snapshots are unsupported"))
    }
    async fn delete_topic(
        &self,
        request: Request<DeleteTopicRequest>,
    ) -> Result<Response<()>, Status> {
        self.delete_topic_value(&request.into_inner().topic).await?;
        Ok(Response::new(()))
    }
    async fn detach_subscription(
        &self,
        _request: Request<DetachSubscriptionRequest>,
    ) -> Result<Response<DetachSubscriptionResponse>, Status> {
        Err(Status::unimplemented("DetachSubscription is unsupported"))
    }
}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<StreamingPullResponse, Status>> + Send>>;

#[tonic::async_trait]
impl Subscriber for PubSubService {
    async fn create_subscription(
        &self,
        request: Request<Subscription>,
    ) -> Result<Response<Subscription>, Status> {
        Ok(Response::new(
            self.create_subscription_value(request.into_inner()).await?,
        ))
    }
    async fn get_subscription(
        &self,
        request: Request<GetSubscriptionRequest>,
    ) -> Result<Response<Subscription>, Status> {
        Ok(Response::new(
            self.get_subscription_value(&request.into_inner().subscription)
                .await?,
        ))
    }
    async fn update_subscription(
        &self,
        _request: Request<UpdateSubscriptionRequest>,
    ) -> Result<Response<Subscription>, Status> {
        Err(Status::unimplemented("UpdateSubscription is unsupported"))
    }
    async fn list_subscriptions(
        &self,
        request: Request<ListSubscriptionsRequest>,
    ) -> Result<Response<ListSubscriptionsResponse>, Status> {
        let request = request.into_inner();
        let (subscriptions, next_page_token) = paginate(
            self.list_subscription_values(&request.project).await?,
            request.page_size,
            &request.page_token,
            |value| &value.name,
        )?;
        Ok(Response::new(ListSubscriptionsResponse {
            subscriptions,
            next_page_token,
        }))
    }
    async fn delete_subscription(
        &self,
        request: Request<DeleteSubscriptionRequest>,
    ) -> Result<Response<()>, Status> {
        self.delete_subscription_value(&request.into_inner().subscription)
            .await?;
        Ok(Response::new(()))
    }
    async fn modify_ack_deadline(
        &self,
        request: Request<ModifyAckDeadlineRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        self.modify_ack_ids(
            &request.subscription,
            &request.ack_ids,
            request.ack_deadline_seconds,
        )
        .await?;
        Ok(Response::new(()))
    }
    async fn acknowledge(
        &self,
        request: Request<AcknowledgeRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        self.acknowledge_ids(&request.subscription, &request.ack_ids)
            .await?;
        Ok(Response::new(()))
    }
    async fn pull(&self, request: Request<PullRequest>) -> Result<Response<PullResponse>, Status> {
        let request = request.into_inner();
        if request.max_messages <= 0 {
            return Err(Status::invalid_argument("max_messages must be positive"));
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let messages = self
                .claim(
                    &request.subscription,
                    request.max_messages.min(1000) as usize,
                    MAX_DELIVERY_MESSAGE_BYTES as i64,
                    None,
                )
                .await?;
            if !messages.is_empty()
                || request.return_immediately
                || tokio::time::Instant::now() >= deadline
            {
                return Ok(Response::new(PullResponse {
                    received_messages: messages
                        .into_iter()
                        .map(|value| ReceivedMessage {
                            ack_id: value.ack_id,
                            message: Some(value.message),
                            delivery_attempt: 0,
                        })
                        .collect(),
                }));
            }
            tokio::select! {_=self.notify.notified()=>{},_=tokio::time::sleep(Duration::from_millis(50))=>{}}
        }
    }
    type StreamingPullStream = ResponseStream;
    async fn streaming_pull(
        &self,
        request: Request<tonic::Streaming<StreamingPullRequest>>,
    ) -> Result<Response<Self::StreamingPullStream>, Status> {
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("first StreamingPull request is required"))?;
        if first.subscription.is_empty() {
            return Err(Status::invalid_argument(
                "first StreamingPull request requires subscription",
            ));
        }
        self.get_subscription_value(&first.subscription).await?;
        if !(10..=600).contains(&first.stream_ack_deadline_seconds) {
            return Err(Status::invalid_argument(
                "stream_ack_deadline_seconds must be between 10 and 600",
            ));
        }
        let subscription = first.subscription.clone();
        let max_messages = if first.max_outstanding_messages <= 0 {
            1000
        } else {
            first.max_outstanding_messages.min(10_000) as usize
        };
        let max_bytes = if first.max_outstanding_bytes <= 0 {
            MAX_PENDING_BYTES
        } else {
            first.max_outstanding_bytes.min(MAX_PENDING_BYTES)
        };
        let stream_deadline = first.stream_ack_deadline_seconds;
        let service = self.clone();
        let (sender, receiver) = mpsc::channel(16);
        tokio::spawn(async move {
            let mut outstanding: HashMap<String, i64> = HashMap::new();
            let mut pending_first = Some(first);
            let mut first_request = true;
            'stream: loop {
                let incoming = if let Some(first) = pending_first.take() {
                    Some(Ok(Some(first)))
                } else {
                    tokio::select! {biased; value=inbound.message()=>Some(value), _=service.notify.notified()=>None, _=tokio::time::sleep(Duration::from_millis(50))=>None}
                };
                if let Some(value) = incoming {
                    match value {
                        Ok(Some(value)) => {
                            if !first_request
                                && (!value.subscription.is_empty()
                                    || value.max_outstanding_messages != 0
                                    || value.max_outstanding_bytes != 0)
                            {
                                let _ = sender
                                    .send(Err(Status::invalid_argument(
                                        "subscription and flow-control limits are only valid on the first stream request",
                                    )))
                                    .await;
                                break;
                            }
                            first_request = false;
                            if value.modify_deadline_ack_ids.len()
                                != value.modify_deadline_seconds.len()
                            {
                                let _ = sender
                                    .send(Err(Status::invalid_argument(
                                        "modify deadline ID and seconds lengths differ",
                                    )))
                                    .await;
                                break;
                            }
                            if !value.ack_ids.is_empty() {
                                if let Err(error) =
                                    service.acknowledge_ids(&subscription, &value.ack_ids).await
                                {
                                    let _ = sender.send(Err(error)).await;
                                    break 'stream;
                                }
                                for ack in value.ack_ids {
                                    outstanding.remove(&ack);
                                }
                            }
                            for (ack, seconds) in value
                                .modify_deadline_ack_ids
                                .iter()
                                .zip(value.modify_deadline_seconds.iter())
                            {
                                if let Err(error) = service
                                    .modify_ack_ids(
                                        &subscription,
                                        std::slice::from_ref(ack),
                                        *seconds,
                                    )
                                    .await
                                {
                                    let _ = sender.send(Err(error)).await;
                                    break 'stream;
                                }
                                if *seconds == 0 {
                                    outstanding.remove(ack);
                                }
                            }
                        }
                        Ok(None) => break,
                        Err(error) => {
                            let _ = sender.send(Err(error)).await;
                            break;
                        }
                    }
                }
                // ACKs and deadline changes can arrive through unary RPCs or a
                // different stream. Flow control must therefore count only
                // leases that are still authoritative in the broker.
                let ack_ids: Vec<_> = outstanding.keys().cloned().collect();
                match service.active_ack_ids(&subscription, &ack_ids).await {
                    Ok(active) => outstanding.retain(|ack_id, _| active.contains(ack_id)),
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        break;
                    }
                }
                let used_bytes: i64 = outstanding.values().sum();
                if outstanding.len() < max_messages && used_bytes < max_bytes {
                    match service
                        .claim(
                            &subscription,
                            (max_messages - outstanding.len()).min(100),
                            (max_bytes - used_bytes)
                                .max(0)
                                .min(MAX_DELIVERY_MESSAGE_BYTES as i64),
                            Some(stream_deadline),
                        )
                        .await
                    {
                        Ok(messages) if !messages.is_empty() => {
                            let response = StreamingPullResponse {
                                received_messages: messages
                                    .iter()
                                    .map(|value| ReceivedMessage {
                                        ack_id: value.ack_id.clone(),
                                        message: Some(value.message.clone()),
                                        delivery_attempt: 0,
                                    })
                                    .collect(),
                            };
                            for message in messages {
                                outstanding.insert(message.ack_id, message.size_bytes);
                            }
                            if sender.send(Ok(response)).await.is_err() {
                                break;
                            }
                        }
                        Ok(_) => {}
                        Err(error) => {
                            let _ = sender.send(Err(error)).await;
                            break;
                        }
                    }
                }
            }
            service
                .release_ids(&subscription, outstanding.into_keys().collect())
                .await;
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
    async fn modify_push_config(
        &self,
        _request: Request<ModifyPushConfigRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("push subscriptions are unsupported"))
    }
    async fn get_snapshot(
        &self,
        _request: Request<GetSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        Err(Status::unimplemented("snapshots are unsupported"))
    }
    async fn list_snapshots(
        &self,
        _request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented("snapshots are unsupported"))
    }
    async fn create_snapshot(
        &self,
        _request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        Err(Status::unimplemented("snapshots are unsupported"))
    }
    async fn update_snapshot(
        &self,
        _request: Request<UpdateSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        Err(Status::unimplemented("snapshots are unsupported"))
    }
    async fn delete_snapshot(
        &self,
        _request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("snapshots are unsupported"))
    }
    async fn seek(&self, _request: Request<SeekRequest>) -> Result<Response<SeekResponse>, Status> {
        Err(Status::unimplemented("Seek is unsupported"))
    }
}

pub(crate) async fn serve(
    address: std::net::SocketAddr,
    service: PubSubService,
) -> Result<(), BoxError> {
    tonic::transport::Server::builder()
        .add_service(
            PublisherServer::new(service.clone())
                .max_decoding_message_size(MAX_TRANSPORT_BYTES)
                .max_encoding_message_size(MAX_TRANSPORT_BYTES),
        )
        .add_service(
            SubscriberServer::new(service)
                .max_decoding_message_size(MAX_TRANSPORT_BYTES)
                .max_encoding_message_size(MAX_TRANSPORT_BYTES),
        )
        .serve(address)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(project: &str, id: &str) -> Topic {
        Topic {
            name: format!("projects/{project}/topics/{id}"),
            ..Default::default()
        }
    }

    fn subscription(project: &str, id: &str, topic_id: &str) -> Subscription {
        Subscription {
            name: format!("projects/{project}/subscriptions/{id}"),
            topic: format!("projects/{project}/topics/{topic_id}"),
            ack_deadline_seconds: 10,
            ..Default::default()
        }
    }

    fn message(value: &[u8]) -> PubsubMessage {
        PubsubMessage {
            data: value.to_vec(),
            attributes: [("source".into(), "test".into())].into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn crud_pagination_and_projects_are_isolated() {
        let service = PubSubService::new(None);
        for id in ["topic-a", "topic-b", "topic-c"] {
            service
                .create_topic_value(topic("demo-one", id))
                .await
                .unwrap();
        }
        service
            .create_topic_value(topic("demo-two", "topic-a"))
            .await
            .unwrap();
        service
            .create_subscription_value(subscription("demo-one", "sub-one", "topic-a"))
            .await
            .unwrap();
        let (first, token) = paginate(
            service
                .list_topic_values("projects/demo-one")
                .await
                .unwrap(),
            2,
            "",
            |value| &value.name,
        )
        .unwrap();
        assert_eq!(first.len(), 2);
        assert!(!token.is_empty());
        let (second, token) = paginate(
            service
                .list_topic_values("projects/demo-one")
                .await
                .unwrap(),
            2,
            &token,
            |value| &value.name,
        )
        .unwrap();
        assert_eq!(second.len(), 1);
        assert!(token.is_empty());
        assert_eq!(
            service
                .list_topic_values("projects/demo-two")
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            service
                .topic_subscription_names("projects/demo-one/topics/topic-a")
                .await
                .unwrap(),
            vec!["projects/demo-one/subscriptions/sub-one"]
        );
        service
            .delete_topic_value("projects/demo-one/topics/topic-a")
            .await
            .unwrap();
        assert_eq!(
            service
                .get_subscription_value("projects/demo-one/subscriptions/sub-one")
                .await
                .unwrap()
                .topic,
            "_deleted-topic_"
        );
        service
            .delete_subscription_value("projects/demo-one/subscriptions/sub-one")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fanout_ack_nack_and_expiry_preserve_message_identity() {
        let service = PubSubService::new(None);
        service
            .create_topic_value(topic("demo-fanout", "events"))
            .await
            .unwrap();
        for id in ["sub-one", "sub-two"] {
            service
                .create_subscription_value(subscription("demo-fanout", id, "events"))
                .await
                .unwrap();
        }
        let id = service
            .publish_values(
                "projects/demo-fanout/topics/events",
                vec![message(b"bytes")],
            )
            .await
            .unwrap()
            .remove(0);
        let first = service
            .pull_one("projects/demo-fanout/subscriptions/sub-one")
            .await
            .unwrap()
            .unwrap();
        let independent = service
            .pull_one("projects/demo-fanout/subscriptions/sub-two")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.message.message_id, id);
        assert_eq!(independent.message.message_id, id);
        service
            .acknowledge_ids(
                "projects/demo-fanout/subscriptions/sub-one",
                &[first.ack_id],
            )
            .await
            .unwrap();
        service
            .modify_ack_ids(
                "projects/demo-fanout/subscriptions/sub-two",
                &[independent.ack_id],
                0,
            )
            .await
            .unwrap();
        let redelivery = service
            .pull_one("projects/demo-fanout/subscriptions/sub-two")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(redelivery.message.message_id, id);
        service
            .modify_ack_ids(
                "projects/demo-fanout/subscriptions/sub-two",
                std::slice::from_ref(&redelivery.ack_id),
                1,
            )
            .await
            .unwrap();
        {
            let mut state = service.memory.lock().await;
            state
                .deliveries
                .values_mut()
                .for_each(|delivery| delivery.deadline_ms = Some(now_ms() - 1));
        }
        let expired = service
            .pull_one("projects/demo-fanout/subscriptions/sub-two")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expired.message.message_id, id);
        service
            .acknowledge_ids(
                "projects/demo-fanout/subscriptions/sub-two",
                &[expired.ack_id],
            )
            .await
            .unwrap();
        assert_eq!(
            service
                .pending_count(&[
                    "projects/demo-fanout/subscriptions/sub-one".into(),
                    "projects/demo-fanout/subscriptions/sub-two".into()
                ])
                .await
                .unwrap(),
            0
        );

        let bounded = PubSubService::new(None);
        bounded
            .create_topic_value(topic("demo-bounded", "events"))
            .await
            .unwrap();
        bounded
            .create_subscription_value(subscription("demo-bounded", "worker", "events"))
            .await
            .unwrap();
        let large_id = bounded
            .publish_values(
                "projects/demo-bounded/topics/events",
                vec![message(&vec![1; 1_000])],
            )
            .await
            .unwrap()
            .remove(0);
        let small_id = bounded
            .publish_values(
                "projects/demo-bounded/topics/events",
                vec![message(b"small")],
            )
            .await
            .unwrap()
            .remove(0);
        let small = bounded
            .claim("projects/demo-bounded/subscriptions/worker", 1, 100, None)
            .await
            .unwrap();
        assert_eq!(small[0].message.message_id, small_id);
        let large = bounded
            .claim("projects/demo-bounded/subscriptions/worker", 1, 2_000, None)
            .await
            .unwrap();
        assert_eq!(large[0].message.message_id, large_id);
    }

    #[tokio::test]
    async fn invalid_and_unsupported_requests_fail_explicitly() {
        let service = PubSubService::new(None);
        assert_eq!(
            service
                .create_topic_value(topic("demo", "x"))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        service
            .create_topic_value(topic("demo-options", "events"))
            .await
            .unwrap();
        let mut push = subscription("demo-options", "push-sub", "events");
        push.push_config = Some(PushConfig {
            push_endpoint: "http://127.0.0.1:1".into(),
            ..Default::default()
        });
        assert_eq!(
            service
                .create_subscription_value(push)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unimplemented
        );
        let mut exactly_once = subscription("demo-options", "exact-sub", "events");
        exactly_once.enable_exactly_once_delivery = true;
        assert_eq!(
            service
                .create_subscription_value(exactly_once)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unimplemented
        );
        assert_eq!(
            service
                .publish_values(
                    "projects/demo-options/topics/events",
                    vec![PubsubMessage::default()]
                )
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn count_and_byte_capacity_reject_publish_atomically() {
        let service = PubSubService::new(None);
        service
            .create_topic_value(topic("demo-capacity", "events"))
            .await
            .unwrap();
        for id in ["sub-one", "sub-two"] {
            service
                .create_subscription_value(subscription("demo-capacity", id, "events"))
                .await
                .unwrap();
        }
        let messages = (0..5_001).map(|_| message(b"x")).collect();
        assert_eq!(
            service
                .publish_values("projects/demo-capacity/topics/events", messages)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
        assert_eq!(
            service
                .pending_count(&["projects/demo-capacity/subscriptions/sub-one".into()])
                .await
                .unwrap(),
            0
        );

        let bytes = PubSubService::new(None);
        bytes
            .create_topic_value(topic("demo-bytes", "events"))
            .await
            .unwrap();
        for id in ["sub-one", "sub-two"] {
            bytes
                .create_subscription_value(subscription("demo-bytes", id, "events"))
                .await
                .unwrap();
        }
        let large = vec![7; 9 * 1024 * 1024];
        for _ in 0..3 {
            bytes
                .publish_values("projects/demo-bytes/topics/events", vec![message(&large)])
                .await
                .unwrap();
        }
        assert_eq!(
            bytes
                .publish_values("projects/demo-bytes/topics/events", vec![message(&large)])
                .await
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
        assert_eq!(
            bytes
                .pending_count(&["projects/demo-bytes/subscriptions/sub-one".into()])
                .await
                .unwrap(),
            3
        );
    }

    #[tokio::test]
    async fn exact_publish_limit_remains_deliverable_after_server_metadata() {
        let service = PubSubService::new(None);
        service
            .create_topic_value(topic("demo-boundary", "events"))
            .await
            .unwrap();
        service
            .create_subscription_value(subscription("demo-boundary", "worker", "events"))
            .await
            .unwrap();
        let boundary = PubsubMessage {
            // One field tag plus a four-byte protobuf length prefix makes the
            // publisher-supplied message exactly 10 MiB.
            data: vec![0x62; MAX_MESSAGE_BYTES - 5],
            ..Default::default()
        };
        assert_eq!(boundary.encoded_len(), MAX_MESSAGE_BYTES);
        let id = service
            .publish_values("projects/demo-boundary/topics/events", vec![boundary])
            .await
            .unwrap()
            .remove(0);
        let delivered = service
            .pull_one("projects/demo-boundary/subscriptions/worker")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivered.message.message_id, id);
        assert!(delivered.message.encoded_len() > MAX_MESSAGE_BYTES);
        assert!(delivered.message.encoded_len() <= MAX_DELIVERY_MESSAGE_BYTES);
        service
            .acknowledge_ids(
                "projects/demo-boundary/subscriptions/worker",
                &[delivered.ack_id],
            )
            .await
            .unwrap();

        let oversized = PubsubMessage {
            data: vec![0x6f; MAX_MESSAGE_BYTES - 4],
            ..Default::default()
        };
        assert_eq!(oversized.encoded_len(), MAX_MESSAGE_BYTES + 1);
        assert_eq!(
            service
                .publish_values("projects/demo-boundary/topics/events", vec![oversized])
                .await
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
        assert_eq!(
            service
                .pending_count(&["projects/demo-boundary/subscriptions/worker".into()])
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn persistent_pending_redelivers_but_acked_does_not_resurrect() {
        let root = std::env::temp_dir().join(format!(
            "firebase-emu-pubsub-restart-{}",
            uuid::Uuid::new_v4()
        ));
        let persistence = Persistence::open(root.clone(), false).await.unwrap();
        let service = PubSubService::new(Some(persistence.clone()));
        service
            .create_topic_value(topic("demo-restart", "events"))
            .await
            .unwrap();
        service
            .create_subscription_value(subscription("demo-restart", "worker", "events"))
            .await
            .unwrap();
        let id = service
            .publish_values(
                "projects/demo-restart/topics/events",
                vec![message(b"durable")],
            )
            .await
            .unwrap()
            .remove(0);
        let first = service
            .pull_one("projects/demo-restart/subscriptions/worker")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.message.message_id, id);
        drop(service);
        drop(persistence);
        let persistence = Persistence::open(root.clone(), false).await.unwrap();
        let service = PubSubService::new(Some(persistence.clone()));
        assert!(service
            .pull_one("projects/demo-restart/subscriptions/worker")
            .await
            .unwrap()
            .is_none());
        drop(service);
        drop(persistence);
        let database = root.join("firebase-emu.sqlite3");
        let connection = rusqlite::Connection::open(&database).unwrap();
        connection
            .execute("UPDATE pubsub_deliveries SET deadline=0", [])
            .unwrap();
        drop(connection);
        let persistence = Persistence::open(root.clone(), false).await.unwrap();
        let service = PubSubService::new(Some(persistence.clone()));
        let redelivery = service
            .pull_one("projects/demo-restart/subscriptions/worker")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(redelivery.message.message_id, id);
        service
            .acknowledge_ids(
                "projects/demo-restart/subscriptions/worker",
                &[redelivery.ack_id],
            )
            .await
            .unwrap();
        drop(service);
        drop(persistence);
        let persistence = Persistence::open(root.clone(), false).await.unwrap();
        let service = PubSubService::new(Some(persistence.clone()));
        assert!(service
            .pull_one("projects/demo-restart/subscriptions/worker")
            .await
            .unwrap()
            .is_none());
        service
            .publish_values(
                "projects/demo-restart/topics/events",
                vec![message(b"delete-cleanup")],
            )
            .await
            .unwrap();
        service
            .delete_subscription_value("projects/demo-restart/subscriptions/worker")
            .await
            .unwrap();
        let remaining = persistence
            .read(|connection| {
                Ok((
                    connection.query_row("SELECT count(*) FROM pubsub_messages", [], |row| {
                        row.get::<_, i64>(0)
                    })?,
                    connection.query_row("SELECT count(*) FROM pubsub_deliveries", [], |row| {
                        row.get::<_, i64>(0)
                    })?,
                ))
            })
            .await
            .unwrap();
        assert_eq!(remaining, (0, 0));
        drop(service);
        drop(persistence);
        std::fs::remove_dir_all(root).unwrap();
    }
}
