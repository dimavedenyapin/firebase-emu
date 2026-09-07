//! Local Node Functions runtime. Mutations enter one ordered, observable queue.
use crate::{
    firestore::google::firestore::v1::Document,
    functions_config::{FunctionCodebase, FunctionsConfig},
    persistence::{OutboxRecord, Persistence},
    BoxError,
};
use axum::{
    body::{to_bytes, Body},
    extract::{Path, Request, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use prost::Message;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    path::PathBuf,
    process::Stdio,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
    sync::mpsc,
    time::{timeout, Duration},
};
use tower_http::cors::{Any, CorsLayer};

static HUB: OnceLock<Arc<Hub>> = OnceLock::new();
const TRIGGER_LOOP_LIMIT: usize = 1000;
const DURABLE_BURST_LIMIT: usize = 32;
const FAILURE_HISTORY_LIMIT: usize = 1000;
const TRANSITION_RETRIES: usize = 5;
const OUTBOX_LEASE_SECONDS: i64 = 5;
#[cfg(test)]
static TEST_ACK_FAILURES: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static TEST_OUTBOX_QUEUE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
struct Hub {
    tx: mpsc::Sender<QueueMessage>,
    persistence: Option<Persistence>,
    pending: AtomicUsize,
    sequence: AtomicU64,
    failures: Mutex<Vec<Value>>,
    completed: AtomicU64,
}

fn record_failure(hub: &Hub, event_id: Option<i64>, error: impl Into<String>) {
    let failure = json!({"eventId":event_id,"error":error.into()});
    let mut failures = hub.failures.lock().unwrap();
    if failures.iter().any(|existing| existing == &failure) {
        return;
    }
    if failures.len() >= FAILURE_HISTORY_LIMIT {
        failures.remove(0);
    }
    failures.push(failure);
}

fn clear_event_failures(hub: &Hub, event_id: i64) {
    hub.failures
        .lock()
        .unwrap()
        .retain(|failure| failure["eventId"].as_i64() != Some(event_id));
}

fn clear_system_failures(hub: &Hub) {
    hub.failures
        .lock()
        .unwrap()
        .retain(|failure| !failure["eventId"].is_null());
}

fn trigger_loop_limit() -> usize {
    std::env::var("FIREBASE_EMU_TEST_TRIGGER_LOOP_LIMIT")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0 && *value <= TRIGGER_LOOP_LIMIT)
        .unwrap_or(TRIGGER_LOOP_LIMIT)
}

fn queue_poll_delay(has_persistence: bool, durable_burst: usize) -> Duration {
    if has_persistence && durable_burst >= DURABLE_BURST_LIMIT {
        Duration::ZERO
    } else {
        Duration::from_millis(250)
    }
}
#[derive(Clone)]
struct QueuedEvent {
    source: EventSource,
    id: u64,
}
enum QueueMessage {
    Direct(Box<QueuedEvent>),
    DurableWake,
}
#[derive(Clone)]
enum EventSource {
    Firestore {
        before: Option<Document>,
        after: Option<Document>,
    },
    Storage {
        kind: &'static str,
        object: Value,
    },
    PubSub {
        topic: String,
        message: Value,
    },
    Schedule {
        function: String,
    },
}

fn enqueue(source: EventSource) -> Result<u64, String> {
    let Some(hub) = HUB.get() else {
        return Err("Functions runtime is not enabled".into());
    };
    let id = hub.sequence.fetch_add(1, Ordering::SeqCst) + 1;
    hub.pending.fetch_add(1, Ordering::SeqCst);
    if let Err(error) = hub
        .tx
        .try_send(QueueMessage::Direct(Box::new(QueuedEvent { source, id })))
    {
        hub.pending.fetch_sub(1, Ordering::SeqCst);
        let message = format!("trigger queue rejected event: {error}");
        record_failure(hub, Some(id as i64), message.clone());
        return Err(message);
    }
    Ok(id)
}

#[derive(Serialize, Deserialize)]
struct FirestoreOutboxPayload {
    before: Option<String>,
    after: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct StorageOutboxPayload {
    kind: String,
    object: Value,
}

pub(crate) fn document_outbox_record(
    before: Option<&Document>,
    after: Option<&Document>,
) -> Vec<OutboxRecord> {
    if before.map(|document| &document.fields) == after.map(|document| &document.fields) {
        return Vec::new();
    }
    let payload = FirestoreOutboxPayload {
        before: before.map(|document| BASE64.encode(document.encode_to_vec())),
        after: after.map(|document| BASE64.encode(document.encode_to_vec())),
    };
    vec![OutboxRecord {
        source: "firestore".into(),
        payload: serde_json::to_vec(&payload).expect("Firestore event payload is serializable"),
    }]
}

pub(crate) fn document_outbox_records(
    before: &BTreeMap<String, Document>,
    after: &BTreeMap<String, Document>,
) -> Vec<OutboxRecord> {
    let mut records = Vec::new();
    for (name, old) in before {
        records.extend(document_outbox_record(Some(old), after.get(name)));
    }
    for (name, new) in after {
        if !before.contains_key(name) {
            records.extend(document_outbox_record(None, Some(new)));
        }
    }
    records
}

pub(crate) fn storage_outbox_record(kind: &str, object: Value) -> OutboxRecord {
    OutboxRecord {
        source: "storage".into(),
        payload: serde_json::to_vec(&StorageOutboxPayload {
            kind: kind.into(),
            object,
        })
        .expect("Storage event payload is serializable"),
    }
}

pub(crate) fn outbox_committed() {
    if let Some(hub) = HUB.get() {
        let _ = hub.tx.try_send(QueueMessage::DurableWake);
    }
}

/// Called only after a successful mutation, while the Firestore store is locked.
/// Timestamp-only writes and missing-document deletes do not emit events.
pub(crate) fn document_changed(before: Option<&Document>, after: Option<&Document>) {
    if HUB.get().is_none() {
        return;
    }
    if before.map(|d| &d.fields) == after.map(|d| &d.fields) {
        return;
    }
    let _ = enqueue(EventSource::Firestore {
        before: before.cloned(),
        after: after.cloned(),
    });
}
pub(crate) fn documents_changed(
    before: &BTreeMap<String, Document>,
    after: &BTreeMap<String, Document>,
) {
    if HUB.get().is_none() {
        return;
    }
    for (name, old) in before {
        document_changed(Some(old), after.get(name));
    }
    for (name, new) in after {
        if !before.contains_key(name) {
            document_changed(None, Some(new));
        }
    }
}

/// Called after a successful Storage mutation. The object is the public GCS
/// JSON representation returned by the Storage emulator.
pub(crate) fn storage_changed(kind: &'static str, object: Value) {
    if HUB.get().is_some() {
        let _ = enqueue(EventSource::Storage { kind, object });
    }
}

#[derive(Clone, Deserialize)]
struct Function {
    name: String,
    regions: Vec<String>,
    trigger: Value,
}
#[derive(Deserialize)]
struct Ready {
    port: u16,
    functions: Vec<Function>,
}
#[derive(Clone)]
struct Worker {
    codebase: String,
    base: String,
    functions: Vec<Function>,
}
#[derive(Clone)]
struct Runtime {
    project: String,
    workers: Vec<Worker>,
    client: reqwest::Client,
    hub: Arc<Hub>,
}

fn wildcard_params(pattern: &str, path: &str) -> Option<Value> {
    let pattern: Vec<_> = pattern.split('/').collect();
    let path: Vec<_> = path.split('/').collect();
    if pattern.len() != path.len() {
        return None;
    }
    let mut params = serde_json::Map::new();
    for (a, b) in pattern.into_iter().zip(path) {
        if a.starts_with('{') && a.ends_with('}') && a.len() > 2 {
            params.insert(a[1..a.len() - 1].into(), json!(b));
        } else if a != b {
            return None;
        }
    }
    Some(Value::Object(params))
}
fn event_kind(before: bool, after: bool) -> &'static str {
    match (before, after) {
        (false, true) => "create",
        (true, false) => "delete",
        _ => "update",
    }
}
impl Runtime {
    async fn status(&self) -> Value {
        let volatile_pending = self.hub.pending.load(Ordering::SeqCst) as i64;
        let volatile_completed = self.hub.completed.load(Ordering::SeqCst) as i64;
        let mut failures = self.hub.failures.lock().unwrap().clone();
        let Some(persistence) = &self.hub.persistence else {
            return json!({"pending":volatile_pending,"completed":volatile_completed,"failures":failures});
        };
        match persistence
            .read(|connection| {
                let pending: i64 = connection.query_row(
                    "SELECT count(*) FROM event_outbox WHERE state IN ('pending','in_flight')",
                    [],
                    |row| row.get(0),
                )?;
                let completed: i64 = connection.query_row(
                    "SELECT value FROM counters WHERE name='event_completed'",
                    [],
                    |row| row.get(0),
                )?;
                let mut statement = connection.prepare(
                    "SELECT id, last_error FROM event_outbox WHERE state='failed' ORDER BY id LIMIT 1000",
                )?;
                let durable_failures = statement
                    .query_map([], |row| {
                        Ok(json!({"eventId":row.get::<_,i64>(0)?,"error":row.get::<_,Option<String>>(1)?.unwrap_or_else(||"delivery failed".into())}))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((pending, completed, durable_failures))
            })
            .await
        {
            Ok((pending, completed, durable_failures)) => {
                failures.extend(durable_failures);
                json!({"pending":pending + volatile_pending,"completed":completed + volatile_completed,"failures":failures})
            }
            Err(error) => {
                failures.push(json!({"eventId":null,"error":error.to_string()}));
                json!({"pending":volatile_pending,"completed":volatile_completed,"failures":failures})
            }
        }
    }
    async fn post_event(
        &self,
        worker: &Worker,
        function: &str,
        data: Value,
        context: Value,
    ) -> Result<(), String> {
        let payload = json!({"name":function,"event":{"data":data,"context":context}});
        match self
            .client
            .post(format!("{}/__/events", worker.base))
            .json(&payload)
            .send()
            .await
        {
            Ok(res) if res.status().is_success() => Ok(()),
            Ok(res) => Err(format!(
                "{}/{}: {}",
                worker.codebase,
                function,
                res.text().await.unwrap_or_default()
            )),
            Err(error) => Err(format!("{}/{function}: {error}", worker.codebase)),
        }
    }
    async fn dispatch_firestore(
        &self,
        id: u64,
        before: &Option<Document>,
        after: &Option<Document>,
    ) -> Result<(), String> {
        let doc = after.as_ref().or(before.as_ref()).unwrap();
        let prefix = format!("projects/{}/databases/(default)/documents/", self.project);
        let Some(path) = doc.name.strip_prefix(&prefix) else {
            return Ok(());
        };
        let kind = event_kind(before.is_some(), after.is_some());
        let mut errors = vec![];
        for worker in &self.workers {
            for f in &worker.functions {
                let event = &f.trigger["eventTrigger"];
                let ty = event["eventType"].as_str().unwrap_or("");
                if !ty.starts_with("providers/cloud.firestore/eventTypes/document.") {
                    continue;
                }
                if !ty.ends_with(".write") && !ty.ends_with(&format!(".{kind}")) {
                    continue;
                }
                let resource = event["resource"].as_str().unwrap_or("");
                let Some(pattern) = resource.split("/documents/").nth(1) else {
                    continue;
                };
                let Some(params) = wildcard_params(pattern, path) else {
                    continue;
                };
                let empty = json!({});
                let data = json!({"oldValue":before.as_ref().map(crate::firestore_web::document_json).unwrap_or(empty.clone()),"value":after.as_ref().map(crate::firestore_web::document_json).unwrap_or(empty)});
                let timestamp = after
                    .as_ref()
                    .or(before.as_ref())
                    .map(crate::firestore_web::document_json)
                    .unwrap()["updateTime"]
                    .clone();
                let context = json!({"eventId":format!("rust-{id}"),"timestamp":timestamp,"eventType":ty,"resource":{"service":"firestore.googleapis.com","name":doc.name},"params":params});
                if let Err(error) = self.post_event(worker, &f.name, data, context).await {
                    errors.push(error)
                }
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
    async fn dispatch_storage(&self, id: u64, kind: &str, object: &Value) -> Result<(), String> {
        let event_type = format!("google.storage.object.{kind}");
        let bucket = object["bucket"].as_str().unwrap_or("");
        let mut errors = vec![];
        for worker in &self.workers {
            for f in &worker.functions {
                let event = &f.trigger["eventTrigger"];
                if event["eventType"].as_str() != Some(&event_type) {
                    continue;
                }
                let configured = event["resource"]
                    .as_str()
                    .and_then(|r| r.split("/buckets/").nth(1))
                    .unwrap_or("");
                if !configured.is_empty() && configured != "{bucket}" && configured != bucket {
                    continue;
                }
                let timestamp = object["updated"].as_str().unwrap_or("1970-01-01T00:00:00Z");
                let context = json!({"eventId":format!("rust-{id}"),"timestamp":timestamp,"eventType":event_type,"resource":{"service":"storage.googleapis.com","name":format!("projects/_/buckets/{bucket}/objects/{}",object["name"].as_str().unwrap_or(""))},"params":{}});
                if let Err(error) = self
                    .post_event(worker, &f.name, object.clone(), context)
                    .await
                {
                    errors.push(error)
                }
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
    async fn dispatch_pubsub(&self, id: u64, topic: &str, message: &Value) -> Result<(), String> {
        let mut errors = vec![];
        for worker in &self.workers {
            for f in &worker.functions {
                let event = &f.trigger["eventTrigger"];
                if event["eventType"].as_str() != Some("google.pubsub.topic.publish")
                    || f.trigger.get("schedule").is_some()
                {
                    continue;
                }
                if event["resource"]
                    .as_str()
                    .and_then(|r| r.rsplit('/').next())
                    != Some(topic)
                {
                    continue;
                }
                let context = json!({"eventId":format!("rust-{id}"),"timestamp":message["publishTime"],"eventType":"google.pubsub.topic.publish","resource":{"service":"pubsub.googleapis.com","name":format!("projects/{}/topics/{topic}",self.project)},"params":{}});
                if let Err(error) = self
                    .post_event(worker, &f.name, message.clone(), context)
                    .await
                {
                    errors.push(error)
                }
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
    async fn dispatch_schedule(&self, id: u64, function: &str) -> Result<(), String> {
        let Some((worker, f)) = self.workers.iter().find_map(|worker| {
            worker
                .functions
                .iter()
                .find(|f| f.name == function && f.trigger.get("schedule").is_some())
                .map(|f| (worker, f))
        }) else {
            return Err(format!("Unknown scheduled function: {function}"));
        };
        let timestamp = chrono_timestamp();
        let message = json!({"data":"e30=","attributes":{},"messageId":format!("rust-{id}"),"publishTime":timestamp});
        let event_type = f.trigger["eventTrigger"]["eventType"]
            .as_str()
            .unwrap_or("google.pubsub.topic.publish");
        let context = json!({"eventId":format!("rust-{id}"),"timestamp":timestamp,"eventType":event_type,"resource":{"service":"pubsub.googleapis.com","name":format!("projects/{}/topics",self.project)},"params":{}});
        self.post_event(worker, &f.name, message, context).await
    }
    async fn dispatch(&self, event: &QueuedEvent) -> Result<(), String> {
        match &event.source {
            EventSource::Firestore { before, after } => {
                self.dispatch_firestore(event.id, before, after).await
            }
            EventSource::Storage { kind, object } => {
                self.dispatch_storage(event.id, kind, object).await
            }
            EventSource::PubSub { topic, message } => {
                self.dispatch_pubsub(event.id, topic, message).await
            }
            EventSource::Schedule { function } => self.dispatch_schedule(event.id, function).await,
        }
    }
}
fn chrono_timestamp() -> String {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let days = seconds.div_euclid(86_400);
    let day_seconds = seconds.rem_euclid(86_400);
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
    let hour = day_seconds / 3_600;
    let minute = day_seconds % 3_600 / 60;
    let second = day_seconds % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.000Z")
}

#[derive(Clone)]
struct DurableRow {
    id: i64,
    source: String,
    payload: Vec<u8>,
    attempts: i64,
    has_queued_successor: bool,
}

fn decode_durable(row: &DurableRow) -> Result<QueuedEvent, String> {
    let source = match row.source.as_str() {
        "firestore" => {
            let payload: FirestoreOutboxPayload =
                serde_json::from_slice(&row.payload).map_err(|error| error.to_string())?;
            let decode = |value: Option<String>| -> Result<Option<Document>, String> {
                value
                    .map(|value| {
                        BASE64
                            .decode(value)
                            .map_err(|error| error.to_string())
                            .and_then(|bytes| {
                                Document::decode(bytes.as_slice())
                                    .map_err(|error| error.to_string())
                            })
                    })
                    .transpose()
            };
            EventSource::Firestore {
                before: decode(payload.before)?,
                after: decode(payload.after)?,
            }
        }
        "storage" => {
            let payload: StorageOutboxPayload =
                serde_json::from_slice(&row.payload).map_err(|error| error.to_string())?;
            let kind = match payload.kind.as_str() {
                "finalize" => "finalize",
                "delete" => "delete",
                other => return Err(format!("unsupported durable Storage event kind: {other}")),
            };
            EventSource::Storage {
                kind,
                object: payload.object,
            }
        }
        other => return Err(format!("unsupported durable event source: {other}")),
    };
    Ok(QueuedEvent {
        source,
        id: row.id as u64,
    })
}

fn durable_chain_key(event: &QueuedEvent) -> String {
    match &event.source {
        EventSource::Firestore { before, after } => format!(
            "firestore:{}",
            after
                .as_ref()
                .or(before.as_ref())
                .map(|document| document.name.as_str())
                .unwrap_or_default()
        ),
        EventSource::Storage { kind, object } => format!(
            "storage:{kind}:{}:{}",
            object["bucket"].as_str().unwrap_or_default(),
            object["name"].as_str().unwrap_or_default()
        ),
        EventSource::PubSub { topic, .. } => format!("pubsub:{topic}"),
        EventSource::Schedule { function } => format!("schedule:{function}"),
    }
}

async fn recover_outbox(persistence: &Persistence) -> Result<(), String> {
    persistence
        .write(|connection| {
            connection.execute(
                "UPDATE event_outbox SET state='pending', lease_until=NULL \
                 WHERE state='in_flight'",
                [],
            )?;
            Ok(())
        })
        .await
        .map_err(|error| error.to_string())
}

async fn claim_outbox(persistence: &Persistence) -> Result<Option<DurableRow>, String> {
    persistence
        .write(|connection| {
            let transaction = connection.transaction()?;
            let now = unix_now_i64();
            let row = {
                let mut statement = transaction.prepare(
                    "SELECT candidate.id, candidate.source, candidate.payload, candidate.attempts, \
                            EXISTS(SELECT 1 FROM event_outbox AS successor \
                                   WHERE successor.id > candidate.id \
                                     AND successor.state IN ('pending','in_flight')) \
                     FROM event_outbox AS candidate \
                     WHERE (candidate.state='pending' AND candidate.available_at <= ?1) \
                        OR (candidate.state='in_flight' AND candidate.lease_until IS NOT NULL \
                            AND candidate.lease_until <= ?1) \
                     ORDER BY candidate.id LIMIT 1",
                )?;
                statement
                    .query_row([now], |row| {
                        Ok(DurableRow {
                            id: row.get(0)?,
                            source: row.get(1)?,
                            payload: row.get(2)?,
                            attempts: row.get(3)?,
                            has_queued_successor: row.get(4)?,
                        })
                    })
                    .optional()?
            };
            if let Some(row) = &row {
                transaction.execute(
                    "UPDATE event_outbox SET state='in_flight', attempts=attempts+1, \
                     lease_until=?2 WHERE id=?1",
                    rusqlite::params![row.id, now + OUTBOX_LEASE_SECONDS],
                )?;
            }
            transaction.commit()?;
            Ok(row.map(|mut row| {
                row.attempts += 1;
                row
            }))
        })
        .await
        .map_err(|error| error.to_string())
}

async fn acknowledge_outbox(persistence: &Persistence, id: i64) -> Result<(), String> {
    #[cfg(test)]
    if TEST_ACK_FAILURES
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
            remaining.checked_sub(1)
        })
        .is_ok()
    {
        return Err("injected durable acknowledgement failure".into());
    }
    persistence
        .write(move |connection| {
            let transaction = connection.transaction()?;
            let updated = transaction.execute(
                "UPDATE event_outbox SET state='delivered', delivered_at=unixepoch(), \
                 lease_until=NULL, last_error=NULL WHERE id=?1 AND state='in_flight'",
                [id],
            )?;
            if updated != 1 {
                return Err(crate::persistence::Error::Corrupt(format!(
                    "durable event {id} is not in flight during acknowledgement"
                )));
            }
            transaction.execute(
                "UPDATE counters SET value=value+1 WHERE name='event_completed'",
                [],
            )?;
            transaction.execute(
                "DELETE FROM event_outbox WHERE state='delivered' AND id NOT IN \
                 (SELECT id FROM event_outbox WHERE state='delivered' ORDER BY id DESC LIMIT 1000)",
                [],
            )?;
            transaction.commit()?;
            Ok(())
        })
        .await
        .map_err(|error| error.to_string())
}

async fn acknowledge_outbox_with_retry(persistence: &Persistence, id: i64) -> Result<(), String> {
    let mut last = String::new();
    for attempt in 0..TRANSITION_RETRIES {
        match acknowledge_outbox(persistence, id).await {
            Ok(()) => return Ok(()),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10 << attempt)).await;
    }
    Err(last)
}

async fn fail_outbox(
    persistence: &Persistence,
    row: &DurableRow,
    message: String,
) -> Result<(), String> {
    let id = row.id;
    let attempts = row.attempts;
    persistence
        .write(move |connection| {
            if attempts >= 5 {
                connection.execute(
                    "UPDATE event_outbox SET state='failed', lease_until=NULL, last_error=?2 \
                     WHERE id=?1",
                    rusqlite::params![id, message],
                )?;
                connection.execute(
                    "DELETE FROM event_outbox WHERE state='failed' AND id NOT IN \
                     (SELECT id FROM event_outbox WHERE state='failed' ORDER BY id DESC LIMIT 1000)",
                    [],
                )?;
            } else {
                let delay = 1_i64 << (attempts - 1).clamp(0, 4);
                connection.execute(
                    "UPDATE event_outbox SET state='pending', lease_until=NULL, last_error=?2, \
                     available_at=unixepoch()+?3 WHERE id=?1",
                    rusqlite::params![id, message, delay],
                )?;
            }
            Ok(())
        })
        .await
        .map_err(|error| error.to_string())
}

async fn fail_outbox_with_retry(
    persistence: &Persistence,
    row: &DurableRow,
    message: String,
) -> Result<(), String> {
    let mut last = String::new();
    for attempt in 0..TRANSITION_RETRIES {
        match fail_outbox(persistence, row, message.clone()).await {
            Ok(()) => return Ok(()),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10 << attempt)).await;
    }
    Err(last)
}

async fn terminal_fail_outbox(
    persistence: &Persistence,
    id: i64,
    message: String,
) -> Result<(), String> {
    persistence
        .write(move |connection| {
            let transaction = connection.transaction()?;
            transaction.execute(
                "UPDATE event_outbox SET state='failed', lease_until=NULL, last_error=?2 \
                 WHERE id=?1 AND state='in_flight'",
                rusqlite::params![id, message],
            )?;
            transaction.execute(
                "DELETE FROM event_outbox WHERE state='failed' AND id NOT IN \
                 (SELECT id FROM event_outbox WHERE state='failed' ORDER BY id DESC LIMIT 1000)",
                [],
            )?;
            transaction.commit()?;
            Ok(())
        })
        .await
        .map_err(|error| error.to_string())
}

fn unix_now_i64() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

async fn run_queue(runtime: Runtime, mut rx: mpsc::Receiver<QueueMessage>) {
    if let Some(persistence) = &runtime.hub.persistence {
        if let Err(error) = recover_outbox(persistence).await {
            record_failure(&runtime.hub, None, error);
        }
    }
    let mut durable_tail_key: Option<String> = None;
    let mut durable_tail_count = 0;
    let mut direct_busy_count = 0;
    let mut durable_burst = 0;
    let loop_limit = trigger_loop_limit();
    loop {
        if durable_burst < DURABLE_BURST_LIMIT {
            if let Some(persistence) = &runtime.hub.persistence {
                match claim_outbox(persistence).await {
                    Ok(Some(row)) => {
                        clear_system_failures(&runtime.hub);
                        durable_burst += 1;
                        let decoded = decode_durable(&row);
                        let chain_key = decoded.as_ref().ok().map(durable_chain_key);
                        let next_tail_count = if row.has_queued_successor {
                            0
                        } else if chain_key.is_some() && chain_key == durable_tail_key {
                            durable_tail_count + 1
                        } else {
                            1
                        };
                        if next_tail_count > loop_limit {
                            let error = format!(
                                "self-trigger chain limit exceeded ({loop_limit} matching tail events)"
                            );
                            if let Err(persistence_error) =
                                terminal_fail_outbox(persistence, row.id, error).await
                            {
                                record_failure(&runtime.hub, Some(row.id), persistence_error);
                            }
                            durable_tail_key = None;
                            durable_tail_count = 0;
                            durable_burst = 0;
                            continue;
                        }
                        let result = match decoded {
                            Ok(event) => runtime.dispatch(&event).await,
                            Err(error) => Err(error),
                        };
                        if let Err(error) = result {
                            match fail_outbox_with_retry(persistence, &row, error).await {
                                Ok(()) => clear_event_failures(&runtime.hub, row.id),
                                Err(persistence_error) => {
                                    record_failure(&runtime.hub, Some(row.id), persistence_error)
                                }
                            }
                        } else {
                            if let Some(delay) =
                                std::env::var("FIREBASE_EMU_TEST_OUTBOX_ACK_DELAY_MS")
                                    .ok()
                                    .and_then(|value| value.parse::<u64>().ok())
                            {
                                tokio::time::sleep(Duration::from_millis(delay.min(60_000))).await;
                            }
                            match acknowledge_outbox_with_retry(persistence, row.id).await {
                                Ok(()) => clear_event_failures(&runtime.hub, row.id),
                                Err(error) => record_failure(&runtime.hub, Some(row.id), error),
                            }
                        }
                        if row.has_queued_successor {
                            durable_tail_key = None;
                            durable_tail_count = 0;
                        } else {
                            durable_tail_key = chain_key;
                            durable_tail_count = next_tail_count;
                        }
                        continue;
                    }
                    Ok(None) => {
                        clear_system_failures(&runtime.hub);
                        durable_tail_key = None;
                        durable_tail_count = 0;
                        durable_burst = 0;
                    }
                    Err(error) => {
                        record_failure(&runtime.hub, None, error);
                        durable_burst = 0;
                    }
                }
            }
        }
        let wait = queue_poll_delay(runtime.hub.persistence.is_some(), durable_burst);
        let message = tokio::select! {
            biased;
            message=rx.recv()=>message,
            _=tokio::time::sleep(wait)=>Some(QueueMessage::DurableWake),
        };
        let Some(message) = message else { break };
        let QueueMessage::Direct(event) = message else {
            durable_burst = 0;
            continue;
        };
        durable_burst = 0;
        direct_busy_count += 1;
        let result = if direct_busy_count > loop_limit {
            Err(format!(
                "trigger loop limit exceeded ({loop_limit} events without an idle queue)"
            ))
        } else {
            runtime.dispatch(&event).await
        };
        if let Err(error) = result {
            record_failure(&runtime.hub, Some(event.id as i64), error);
        }
        runtime.hub.completed.fetch_add(1, Ordering::SeqCst);
        if runtime.hub.pending.fetch_sub(1, Ordering::SeqCst) == 1 {
            direct_busy_count = 0;
        }
    }
}
async fn status(State(runtime): State<Runtime>) -> Json<Value> {
    Json(runtime.status().await)
}
async fn drain(State(runtime): State<Runtime>) -> Response {
    let result = timeout(Duration::from_secs(30), async {
        loop {
            let status = runtime.status().await;
            if status["pending"].as_i64().unwrap_or(0) == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let code = if result.is_err() {
        StatusCode::REQUEST_TIMEOUT
    } else if runtime.status().await["failures"]
        .as_array()
        .is_some_and(Vec::is_empty)
    {
        StatusCode::OK
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    (code, Json(runtime.status().await)).into_response()
}
async fn inject_pubsub(
    State(runtime): State<Runtime>,
    Path(topic): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    if topic.is_empty()
        || !topic
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"Invalid topic"})),
        )
            .into_response();
    }
    let Some(data) = body.get("data") else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"Expected data"})),
        )
            .into_response();
    };
    let encoded = BASE64.encode(serde_json::to_vec(data).unwrap());
    let timestamp = chrono_timestamp();
    let message = json!({"data":encoded,"attributes":body.get("attributes").cloned().unwrap_or(json!({})),"messageId":format!("rust-{}",runtime.hub.sequence.load(Ordering::SeqCst)+1),"publishTime":timestamp});
    match enqueue(EventSource::PubSub { topic, message }) {
        Ok(id) => (StatusCode::ACCEPTED, Json(json!({"eventId":id}))).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":error})),
        )
            .into_response(),
    }
}

fn control_router() -> Router<Runtime> {
    Router::new()
        .route("/__/functions/status", get(status))
        .route("/__/functions/drain", get(drain))
        .route("/__/functions/pubsub/{topic}", post(inject_pubsub))
        .route("/__/functions/schedule/{function}", post(inject_schedule))
        .route_layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
                .allow_headers(Any),
        )
}
async fn inject_schedule(
    State(_runtime): State<Runtime>,
    Path(function): Path<String>,
) -> Response {
    match enqueue(EventSource::Schedule { function }) {
        Ok(id) => (StatusCode::ACCEPTED, Json(json!({"eventId":id}))).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":error})),
        )
            .into_response(),
    }
}
async fn proxy(State(runtime): State<Runtime>, request: Request) -> Response {
    let path = request.uri().path();
    let parts: Vec<_> = path.splitn(5, '/').collect();
    let worker = if parts.len() >= 4 && parts[1] == runtime.project {
        runtime.workers.iter().find(|worker| {
            worker.functions.iter().any(|f| {
                f.name == parts[3]
                    && f.regions.iter().any(|r| r == parts[2])
                    && f.trigger.get("httpsTrigger").is_some()
            })
        })
    } else {
        None
    };
    let Some(worker) = worker else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error":"Unknown local HTTP function"})),
        )
            .into_response();
    };
    let mut url = format!("{}/invoke/{}", worker.base, parts[3]);
    if let Some(tail) = parts.get(4) {
        url.push('/');
        url.push_str(tail);
    }
    if let Some(query) = request.uri().query() {
        url.push('?');
        url.push_str(query);
    }
    let (parts, body) = request.into_parts();
    let body = match to_bytes(body, 32 * 1024 * 1024).await {
        Ok(body) => body,
        Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
    };
    let mut outgoing = runtime.client.request(parts.method, url).body(body);
    for (name, value) in &parts.headers {
        if !matches!(
            name.as_str(),
            "host" | "connection" | "content-length" | "transfer-encoding"
        ) {
            outgoing = outgoing.header(name, value);
        }
    }
    match outgoing.send().await {
        Ok(response) => {
            let status = response.status();
            let headers = response.headers().clone();
            match response.bytes().await {
                Ok(body) => {
                    let mut result = Response::new(Body::from(body));
                    *result.status_mut() = status;
                    for (name, value) in &headers {
                        if !matches!(name.as_str(), "connection" | "transfer-encoding") {
                            result.headers_mut().insert(name.clone(), value.clone());
                        }
                    }
                    result
                }
                Err(_) => StatusCode::BAD_GATEWAY.into_response(),
            }
        }
        Err(error) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error":format!("Node runtime request failed: {error}")})),
        )
            .into_response(),
    }
}
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! {_=tokio::signal::ctrl_c()=>{},_=terminate.recv()=>{}}
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
struct ManagedChild {
    codebase: String,
    child: Child,
    logs: Vec<tokio::task::JoinHandle<()>>,
}

fn configured_node(codebase: &FunctionCodebase) -> OsString {
    let major = codebase.runtime.trim_start_matches("nodejs");
    std::env::var_os(format!("FIREBASE_FUNCTIONS_NODE_{major}"))
        .or_else(|| std::env::var_os("FIREBASE_FUNCTIONS_NODE"))
        .unwrap_or_else(|| "node".into())
}

fn normalize_node_path(value: &str) -> Cow<'_, str> {
    const VERBATIM_PREFIX: &str = "\\\\?\\";
    const VERBATIM_UNC_PREFIX: &str = "\\\\?\\UNC\\";
    if let Some(rest) = value.strip_prefix(VERBATIM_UNC_PREFIX) {
        return Cow::Owned(format!(r"\\{rest}"));
    }
    if let Some(rest) = value.strip_prefix(VERBATIM_PREFIX) {
        let bytes = rest.as_bytes();
        if bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'\\' | b'/')
        {
            return Cow::Borrowed(rest);
        }
    }
    Cow::Borrowed(value)
}

fn node_compatible_path(path: &PathBuf) -> PathBuf {
    path.to_str()
        .map(normalize_node_path)
        .map(|value| PathBuf::from(value.as_ref()))
        .unwrap_or_else(|| path.clone())
}

async fn validate_node(executable: &OsString, expected_runtime: &str) -> Result<(), BoxError> {
    let output = Command::new(executable).arg("--version").output().await?;
    if !output.status.success() {
        return Err(format!("Node runtime {:?} could not report its version", executable).into());
    }
    let version = String::from_utf8(output.stdout)?;
    let actual = version
        .trim()
        .trim_start_matches('v')
        .split('.')
        .next()
        .unwrap_or("");
    let expected = expected_runtime.trim_start_matches("nodejs");
    if actual != expected || !matches!(actual, "18" | "20" | "22") {
        return Err(format!(
            "Functions codebase requires Node {expected}, but {:?} is {}",
            executable,
            version.trim()
        )
        .into());
    }
    Ok(())
}

async fn start_worker(
    config: &FunctionsConfig,
    codebase: &FunctionCodebase,
) -> Result<(Worker, ManagedChild), BoxError> {
    let adapter = if let Some(path) = std::env::var_os("FIREBASE_FUNCTIONS_ADAPTER") {
        PathBuf::from(path)
    } else {
        std::env::current_exe()?
            .parent()
            .ok_or("Firebase emulator executable has no parent directory")?
            .join("functions-runtime/adapter.cjs")
    };
    if !adapter.is_file() {
        return Err(format!(
            "Functions runtime adapter not found at {}. Install functions-runtime next to the firebase-emu executable or set FIREBASE_FUNCTIONS_ADAPTER",
            adapter.display()
        )
        .into());
    }
    let executable = configured_node(codebase);
    validate_node(&executable, &codebase.runtime).await?;
    let node_source = node_compatible_path(&codebase.source);
    let mut command = Command::new(&executable);
    command.env_clear();
    for name in [
        "PATH",
        "SYSTEMROOT",
        "WINDIR",
        "TEMP",
        "TMP",
        "PATHEXT",
        "COMSPEC",
    ] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    command
        .envs(config.child_environment())
        .env(
            "STORAGE_EMULATOR_HOST",
            format!("http://{}", config.addresses.storage),
        )
        .env("FUNCTIONS_CODEBASE", &codebase.codebase)
        .arg(adapter)
        .arg("--source")
        .arg(node_source)
        .arg("--project")
        .arg(&config.project_id)
        .current_dir(&codebase.source)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Ok(target) = std::env::var("FIREBASE_FUNCTIONS_TARGET") {
        command.arg("--target").arg(target);
    }
    if let Ok(manifest) = std::env::var("FIREBASE_FUNCTIONS_MANIFEST") {
        command.arg("--manifest").arg(manifest);
    }
    let mut child = command.spawn()?;
    let mut lines = BufReader::new(child.stdout.take().ok_or("missing child stdout")?).lines();
    let stderr = child.stderr.take().ok_or("missing child stderr")?;
    let stderr_codebase = codebase.codebase.clone();
    let stderr_log = tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            eprintln!("[functions:{stderr_codebase}] {line}");
        }
    });
    let ready: Ready = timeout(Duration::from_secs(30), async {
        while let Some(line) = lines.next_line().await? {
            if let Some(data) = line.strip_prefix("FIREBASE_EMU_READY ") {
                return Ok::<Ready, BoxError>(serde_json::from_str(data)?);
            }
            eprintln!("[functions:{}] {line}", codebase.codebase);
        }
        Err(format!(
            "Node codebase {} exited before readiness",
            codebase.codebase
        )
        .into())
    })
    .await??;
    let stdout_codebase = codebase.codebase.clone();
    let stdout_log = tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            eprintln!("[functions:{stdout_codebase}] {line}");
        }
    });
    let worker = Worker {
        codebase: codebase.codebase.clone(),
        base: format!("http://127.0.0.1:{}", ready.port),
        functions: ready.functions,
    };
    let managed = ManagedChild {
        codebase: codebase.codebase.clone(),
        child,
        logs: vec![stdout_log, stderr_log],
    };
    Ok((worker, managed))
}

async fn monitor_children(children: &mut [ManagedChild]) -> Result<(), BoxError> {
    loop {
        for child in children.iter_mut() {
            if let Some(status) = child.child.try_wait()? {
                return Err(format!(
                    "Node Functions codebase {} exited: {status}",
                    child.codebase
                )
                .into());
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn stop_children(children: &mut [ManagedChild]) {
    for managed in children.iter_mut() {
        #[cfg(unix)]
        if let Some(pid) = managed.child.id() {
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
        }
        #[cfg(not(unix))]
        {
            let _ = managed.child.kill().await;
        }
    }
    for managed in children.iter_mut() {
        if timeout(Duration::from_secs(5), managed.child.wait())
            .await
            .is_err()
        {
            let _ = managed.child.kill().await;
        }
        for log in managed.logs.drain(..) {
            log.abort();
        }
    }
}

/// Rust owns the public endpoint and one private Node child per configured codebase.
pub(crate) async fn serve(
    config: FunctionsConfig,
    persistence: Option<Persistence>,
) -> Result<(), BoxError> {
    let listener = tokio::net::TcpListener::bind(config.addresses.functions).await?;
    let mut workers = Vec::with_capacity(config.codebases.len());
    let mut children = Vec::with_capacity(config.codebases.len());
    let mut names = BTreeSet::new();
    for codebase in &config.codebases {
        let (worker, child) = match start_worker(&config, codebase).await {
            Ok(value) => value,
            Err(error) => {
                stop_children(&mut children).await;
                return Err(error);
            }
        };
        for function in &worker.functions {
            if !names.insert(function.name.clone()) {
                stop_children(&mut children).await;
                return Err(format!(
                    "Duplicate function name across codebases: {}",
                    function.name
                )
                .into());
            }
        }
        workers.push(worker);
        children.push(child);
    }
    let (tx, rx) = mpsc::channel(4096);
    let hub = Arc::new(Hub {
        tx,
        persistence,
        pending: AtomicUsize::new(0),
        sequence: AtomicU64::new(0),
        failures: Mutex::new(vec![]),
        completed: AtomicU64::new(0),
    });
    HUB.set(hub.clone())
        .map_err(|_| "Functions already started")?;
    let runtime = Runtime {
        project: config.project_id,
        workers,
        client: reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(60))
            .build()?,
        hub,
    };
    let queue = tokio::spawn(run_queue(runtime.clone(), rx));
    let router = control_router().fallback(proxy).with_state(runtime);
    eprintln!("Functions emulator ready on {}", config.addresses.functions);
    let result = tokio::select! {
        result=axum::serve(listener,router)=>result.map_err(|e| -> BoxError{e.into()}),
        result=monitor_children(&mut children)=>result,
        _=shutdown_signal()=>Ok(())
    };
    stop_children(&mut children).await;
    queue.abort();
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{header, Request},
    };
    use tower::ServiceExt;

    fn test_runtime() -> Runtime {
        let (tx, _rx) = mpsc::channel(1);
        Runtime {
            project: "demo-functions".into(),
            workers: vec![],
            client: reqwest::Client::builder().no_proxy().build().unwrap(),
            hub: Arc::new(Hub {
                tx,
                persistence: None,
                pending: AtomicUsize::new(0),
                sequence: AtomicU64::new(0),
                failures: Mutex::new(vec![]),
                completed: AtomicU64::new(0),
            }),
        }
    }

    fn persistent_test_runtime(
        persistence: Persistence,
    ) -> (Runtime, mpsc::Receiver<QueueMessage>) {
        let (tx, rx) = mpsc::channel(8);
        (
            Runtime {
                project: "demo-functions".into(),
                workers: vec![],
                client: reqwest::Client::builder().no_proxy().build().unwrap(),
                hub: Arc::new(Hub {
                    tx,
                    persistence: Some(persistence),
                    pending: AtomicUsize::new(0),
                    sequence: AtomicU64::new(0),
                    failures: Mutex::new(vec![]),
                    completed: AtomicU64::new(0),
                }),
            },
            rx,
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn expired_in_flight_ack_failure_recovers_without_restart() {
        let _test_guard = TEST_OUTBOX_QUEUE_LOCK.lock().await;
        let root = std::env::temp_dir().join(format!(
            "firebase-emu-outbox-ack-recovery-{}",
            uuid::Uuid::new_v4()
        ));
        let persistence = Persistence::open(root.clone(), true).await.unwrap();
        let document = Document {
            name: "projects/demo-functions/databases/(default)/documents/items/recover".into(),
            ..Default::default()
        };
        let records = document_outbox_record(None, Some(&document));
        let persistence_for_write = persistence.clone();
        let event_id = persistence
            .write(move |connection| {
                let transaction = connection.transaction()?;
                let ids = persistence_for_write.insert_outbox(&transaction, &records)?;
                transaction.commit()?;
                Ok(ids[0])
            })
            .await
            .unwrap();
        let (runtime, rx) = persistent_test_runtime(persistence.clone());
        TEST_ACK_FAILURES.store(TRANSITION_RETRIES, Ordering::SeqCst);
        let queue = tokio::spawn(run_queue(runtime.clone(), rx));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if runtime.hub.failures.lock().unwrap().iter().any(|failure| {
                    failure["eventId"].as_i64() == Some(event_id)
                        && failure["error"] == "injected durable acknowledgement failure"
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        persistence
            .write(move |connection| {
                connection.execute(
                    "UPDATE event_outbox SET lease_until=unixepoch()-1 WHERE id=?1",
                    [event_id],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let state = persistence
                    .read(move |connection| {
                        connection
                            .query_row(
                                "SELECT state,attempts FROM event_outbox WHERE id=?1",
                                [event_id],
                                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
                            )
                            .map_err(crate::persistence::Error::from)
                    })
                    .await
                    .unwrap();
                if state == ("delivered".into(), 2) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(runtime.hub.failures.lock().unwrap().is_empty());
        assert_eq!(runtime.status().await["pending"], 0);
        assert_eq!(runtime.status().await["completed"], 1);
        queue.abort();
        TEST_ACK_FAILURES.store(0, Ordering::SeqCst);
        drop(runtime);
        drop(persistence);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn independent_backlog_above_loop_limit_is_fully_delivered() {
        let _test_guard = TEST_OUTBOX_QUEUE_LOCK.lock().await;
        let root = std::env::temp_dir().join(format!(
            "firebase-emu-outbox-independent-backlog-{}",
            uuid::Uuid::new_v4()
        ));
        let persistence = Persistence::open(root.clone(), true).await.unwrap();
        let backlog = TRIGGER_LOOP_LIMIT + 5;
        // Use the same resource key for every row: the pre-existing successor
        // rows, not path diversity, prove this is an ordinary backlog rather
        // than a handler creating the next tail event.
        let records: Vec<OutboxRecord> = (0..backlog)
            .flat_map(|_| {
                let document = Document {
                    name: "projects/demo-functions/databases/(default)/documents/backlog/shared"
                        .into(),
                    ..Default::default()
                };
                document_outbox_record(None, Some(&document))
            })
            .collect();
        let persistence_for_write = persistence.clone();
        persistence
            .write(move |connection| {
                let transaction = connection.transaction()?;
                let ids = persistence_for_write.insert_outbox(&transaction, &records)?;
                assert_eq!(ids.len(), backlog);
                transaction.commit()?;
                Ok(())
            })
            .await
            .unwrap();

        let (runtime, rx) = persistent_test_runtime(persistence.clone());
        let queue = tokio::spawn(run_queue(runtime.clone(), rx));
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let status = runtime.status().await;
                if status["pending"] == 0 && status["completed"] == backlog as u64 {
                    assert_eq!(status["failures"], json!([]));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let states = persistence
            .read(|connection| {
                let active: i64 = connection.query_row(
                    "SELECT count(*) FROM event_outbox WHERE state IN ('pending','in_flight')",
                    [],
                    |row| row.get(0),
                )?;
                let failed: i64 = connection.query_row(
                    "SELECT count(*) FROM event_outbox WHERE state='failed'",
                    [],
                    |row| row.get(0),
                )?;
                Ok((active, failed))
            })
            .await
            .unwrap();
        assert_eq!(states, (0, 0));
        queue.abort();
        drop(runtime);
        drop(persistence);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn runtime_failures_are_deduplicated_and_bounded() {
        let runtime = test_runtime();
        for id in 0..(FAILURE_HISTORY_LIMIT + 50) {
            record_failure(&runtime.hub, Some(id as i64), format!("failure {id}"));
        }
        assert_eq!(
            runtime.hub.failures.lock().unwrap().len(),
            FAILURE_HISTORY_LIMIT
        );
        record_failure(
            &runtime.hub,
            Some((FAILURE_HISTORY_LIMIT + 49) as i64),
            format!("failure {}", FAILURE_HISTORY_LIMIT + 49),
        );
        assert_eq!(
            runtime.hub.failures.lock().unwrap().len(),
            FAILURE_HISTORY_LIMIT
        );
    }

    #[test]
    fn in_memory_queue_keeps_its_bounded_idle_poll() {
        assert_eq!(
            queue_poll_delay(false, DURABLE_BURST_LIMIT),
            Duration::from_millis(250)
        );
        assert_eq!(queue_poll_delay(true, DURABLE_BURST_LIMIT), Duration::ZERO);
    }

    #[tokio::test]
    async fn control_routes_support_browser_cors() {
        let app = control_router().with_state(test_runtime());
        let origin = "http://127.0.0.1:35173";
        let preflight = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/__/functions/drain")
                    .header(header::ORIGIN, origin)
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(preflight.status(), StatusCode::OK);
        assert_eq!(
            preflight.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "*"
        );
        assert!(preflight.headers()[header::ACCESS_CONTROL_ALLOW_METHODS]
            .to_str()
            .unwrap()
            .split(',')
            .any(|method| method.trim() == "GET"));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/__/functions/drain")
                    .header(header::ORIGIN, origin)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({
                "completed": 0,
                "failures": [],
                "pending": 0,
            })
        );
    }
    #[test]
    fn wildcard_path_is_exact() {
        assert_eq!(
            wildcard_params(
                "companies/{company}/contacts/{id}",
                "companies/acme/contacts/one"
            ),
            Some(json!({"company":"acme","id":"one"}))
        );
        assert!(wildcard_params("companies/{id}", "companies/a/nested/b").is_none());
        assert!(wildcard_params("companies/{id}", "users/a").is_none());
    }
    #[test]
    fn transitions() {
        assert_eq!(event_kind(false, true), "create");
        assert_eq!(event_kind(true, true), "update");
        assert_eq!(event_kind(true, false), "delete");
    }
    #[test]
    fn timestamps_are_rfc3339_utc() {
        let value = chrono_timestamp();
        assert_eq!(value.len(), 24);
        assert_eq!(&value[4..5], "-");
        assert_eq!(&value[10..11], "T");
        assert!(value.ends_with(".000Z"));
    }

    #[test]
    fn node_paths_normalize_only_windows_disk_and_unc_verbatim_prefixes() {
        assert_eq!(
            normalize_node_path(r"\\?\C:\release space\functions"),
            r"C:\release space\functions"
        );
        assert_eq!(
            normalize_node_path(r"\\?\UNC\server\share\functions"),
            r"\\server\share\functions"
        );
        assert_eq!(
            normalize_node_path(r"\\?\Volume{01234567-89ab-cdef-0123-456789abcdef}\functions"),
            r"\\?\Volume{01234567-89ab-cdef-0123-456789abcdef}\functions"
        );
        assert_eq!(normalize_node_path("/tmp/functions"), "/tmp/functions");
    }
}
