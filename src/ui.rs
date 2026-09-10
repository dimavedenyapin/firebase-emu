//! Embedded, loopback-only emulator console.

use crate::{
    firestore::{
        google::firestore::v1::{
            firestore_server::Firestore, precondition, write, CommitRequest, Document,
            DocumentMask, Precondition, Write,
        },
        FirestoreService,
    },
    firestore_web::{document_json, value_proto},
    pubsub::{
        google::pubsub::v1::{Subscription, Topic},
        PubSubService,
    },
};
use axum::{
    extract::{Query, State},
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::BTreeSet, net::SocketAddr};
use tonic::Request;

#[derive(Clone)]
pub(crate) struct UiState {
    default_project: String,
    auth_addr: SocketAddr,
    firestore: FirestoreService,
    pubsub: PubSubService,
    client: reqwest::Client,
}

impl UiState {
    pub(crate) fn new(
        default_project: String,
        auth_addr: SocketAddr,
        firestore: FirestoreService,
        pubsub: PubSubService,
    ) -> Self {
        Self {
            default_project,
            auth_addr,
            firestore,
            pubsub,
            client: reqwest::Client::new(),
        }
    }
}

pub(crate) fn router(state: UiState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/app.js", get(script))
        .route("/style.css", get(style))
        .route("/favicon.ico", get(|| async { StatusCode::NO_CONTENT }))
        .route("/api/config", get(config))
        .route("/api/auth/users", get(auth_users))
        .route("/api/firestore/documents", get(firestore_documents))
        .route("/api/firestore/field", post(update_field))
        .route("/api/firestore/clone", post(clone_document))
        .route("/api/pubsub", get(pubsub_resources))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(include_str!("ui/index.html"))
}
async fn script() -> Response {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("ui/app.js"),
    )
        .into_response()
}
async fn style() -> Response {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("ui/style.css"),
    )
        .into_response()
}

async fn config(State(state): State<UiState>) -> Json<Value> {
    Json(
        json!({"defaultProject": state.default_project, "objectCopyFormat": "Firestore REST Value JSON v1"}),
    )
}

#[derive(Deserialize)]
struct ProjectQuery {
    project: String,
}

fn valid_segment(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-._:()".contains(&c))
}

fn valid_document_segment(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 1_500
        && value != "."
        && value != ".."
        && !value
            .chars()
            .any(|character| character == '/' || character == '\0')
}

fn api_error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({"error": message.into()}))).into_response()
}

async fn auth_users(State(state): State<UiState>, Query(query): Query<ProjectQuery>) -> Response {
    if !valid_segment(&query.project) {
        return api_error(StatusCode::BAD_REQUEST, "invalid project ID");
    }
    let url = format!(
        "http://{}/identitytoolkit.googleapis.com/v1/projects/{}/accounts:batchGet?maxResults=1000",
        state.auth_addr, query.project
    );
    match state.client.get(url).send().await {
        Ok(response) if response.status().is_success() => match response.json::<Value>().await {
            Ok(value) => Json(value).into_response(),
            Err(error) => api_error(
                StatusCode::BAD_GATEWAY,
                format!("Auth response was invalid: {error}"),
            ),
        },
        Ok(response) => api_error(
            StatusCode::BAD_GATEWAY,
            format!("Auth emulator returned {}", response.status()),
        ),
        Err(error) => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("Auth emulator is unavailable: {error}"),
        ),
    }
}

#[derive(Deserialize)]
struct FirestoreQuery {
    project: String,
    #[serde(default = "default_database")]
    database: String,
}
fn default_database() -> String {
    "(default)".into()
}

async fn firestore_documents(
    State(state): State<UiState>,
    Query(query): Query<FirestoreQuery>,
) -> Response {
    if !valid_segment(&query.project) || !valid_segment(&query.database) {
        return api_error(StatusCode::BAD_REQUEST, "invalid project or database ID");
    }
    let database = format!("projects/{}/databases/{}", query.project, query.database);
    match state.firestore.database_documents(&database).await {
        Ok(documents) => {
            Json(json!({"documents": documents.iter().map(document_json).collect::<Vec<_>>() }))
                .into_response()
        }
        Err(error) => api_error(StatusCode::BAD_GATEWAY, error.message()),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdateFieldRequest {
    project: String,
    #[serde(default = "default_database")]
    database: String,
    document_path: String,
    field_name: String,
    value: Value,
}

fn quoted_field_path(field_name: &str) -> String {
    format!("`{}`", field_name.replace('\\', "\\\\").replace('`', "\\`"))
}

async fn update_field(
    State(state): State<UiState>,
    Json(request): Json<UpdateFieldRequest>,
) -> Response {
    if !valid_segment(&request.project)
        || !valid_segment(&request.database)
        || !valid_document_segment(&request.field_name)
    {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid project, database, or field name",
        );
    }
    let Some(document_parts) = path_parts(&request.document_path) else {
        return api_error(StatusCode::BAD_REQUEST, "invalid document path");
    };
    if document_parts.len() % 2 != 0 {
        return api_error(StatusCode::BAD_REQUEST, "path must identify a document");
    }
    let field_value = match value_proto(&request.value) {
        Ok(value) => value,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, error.message()),
    };
    let database = format!(
        "projects/{}/databases/{}",
        request.project, request.database
    );
    let document_name = format!("{database}/documents/{}", request.document_path);
    let document = Document {
        name: document_name,
        fields: std::collections::HashMap::from([(request.field_name.clone(), field_value)]),
        ..Default::default()
    };
    let write = Write {
        operation: Some(write::Operation::Update(document)),
        update_mask: Some(DocumentMask {
            field_paths: vec![quoted_field_path(&request.field_name)],
        }),
        current_document: Some(Precondition {
            condition_type: Some(precondition::ConditionType::Exists(true)),
        }),
        ..Default::default()
    };
    match Firestore::commit(
        &state.firestore,
        Request::new(CommitRequest {
            database,
            writes: vec![write],
            ..Default::default()
        }),
    )
    .await
    {
        Ok(_) => Json(json!({"updated": true})).into_response(),
        Err(error) if error.code() == tonic::Code::FailedPrecondition => {
            api_error(StatusCode::NOT_FOUND, "document no longer exists")
        }
        Err(error) => api_error(StatusCode::BAD_REQUEST, error.message()),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CloneRequest {
    project: String,
    #[serde(default = "default_database")]
    database: String,
    source_path: String,
    destination_collection_path: String,
    destination_id: String,
    #[serde(default)]
    include_subcollections: bool,
}

fn path_parts(path: &str) -> Option<Vec<&str>> {
    let parts = path.split('/').collect::<Vec<_>>();
    (!path.starts_with('/')
        && !path.ends_with('/')
        && parts.iter().all(|v| valid_document_segment(v)))
    .then_some(parts)
}

async fn clone_document(
    State(state): State<UiState>,
    Json(request): Json<CloneRequest>,
) -> Response {
    if !valid_segment(&request.project)
        || !valid_segment(&request.database)
        || !valid_document_segment(&request.destination_id)
    {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid project, database, or destination document ID",
        );
    }
    let Some(source_parts) = path_parts(&request.source_path) else {
        return api_error(StatusCode::BAD_REQUEST, "invalid source document path");
    };
    let Some(collection_parts) = path_parts(&request.destination_collection_path) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid destination collection path",
        );
    };
    if source_parts.len() % 2 != 0 || collection_parts.len() % 2 != 1 {
        return api_error(
            StatusCode::BAD_REQUEST,
            "source must be a document path and destination must be a collection path",
        );
    }
    let database = format!(
        "projects/{}/databases/{}",
        request.project, request.database
    );
    let prefix = format!("{database}/documents/");
    let source_name = format!("{prefix}{}", request.source_path);
    let destination_name = format!(
        "{prefix}{}/{}",
        request.destination_collection_path, request.destination_id
    );
    let documents = match state.firestore.database_documents(&database).await {
        Ok(documents) => documents,
        Err(error) => return api_error(StatusCode::BAD_GATEWAY, error.message()),
    };
    let mut source_documents = documents
        .into_iter()
        .filter(|document| {
            document.name == source_name
                || (request.include_subcollections
                    && document.name.starts_with(&(source_name.clone() + "/")))
        })
        .collect::<Vec<_>>();
    source_documents.sort_by(|a, b| a.name.cmp(&b.name));
    if !source_documents
        .iter()
        .any(|document| document.name == source_name)
    {
        return api_error(StatusCode::NOT_FOUND, "source document not found");
    }
    let writes = source_documents
        .into_iter()
        .map(|mut document| {
            let suffix = document.name.strip_prefix(&source_name).unwrap_or_default();
            document.name = format!("{destination_name}{suffix}");
            document.create_time = None;
            document.update_time = None;
            Write {
                operation: Some(write::Operation::Update(document)),
                current_document: Some(Precondition {
                    condition_type: Some(precondition::ConditionType::Exists(false)),
                }),
                ..Default::default()
            }
        })
        .collect::<Vec<_>>();
    let count = writes.len();
    match Firestore::commit(&state.firestore, Request::new(CommitRequest { database, writes, ..Default::default() })).await {
        Ok(_) => Json(json!({"destinationPath": format!("{}/{}", request.destination_collection_path, request.destination_id), "documentsCloned": count, "subcollectionsIncluded": request.include_subcollections})).into_response(),
        Err(error) if error.code() == tonic::Code::FailedPrecondition || error.code() == tonic::Code::AlreadyExists => api_error(StatusCode::CONFLICT, "destination exists; cloning never overwrites documents"),
        Err(error) => api_error(StatusCode::BAD_REQUEST, error.message()),
    }
}

fn duration_json(value: &Option<prost_types::Duration>) -> Value {
    value
        .as_ref()
        .map(|v| json!({"seconds": v.seconds.to_string(), "nanos": v.nanos}))
        .unwrap_or(Value::Null)
}

fn topic_json(topic: &Topic, subscriptions: &[Subscription]) -> Value {
    json!({
        "name": topic.name,
        "labels": topic.labels,
        "messageRetentionDuration": duration_json(&topic.message_retention_duration),
        "state": topic.state,
        "subscriptions": subscriptions.iter().filter(|s| s.topic == topic.name).map(subscription_json).collect::<Vec<_>>()
    })
}

fn subscription_json(subscription: &Subscription) -> Value {
    json!({
        "name": subscription.name,
        "topic": subscription.topic,
        "ackDeadlineSeconds": subscription.ack_deadline_seconds,
        "retainAckedMessages": subscription.retain_acked_messages,
        "messageRetentionDuration": duration_json(&subscription.message_retention_duration),
        "labels": subscription.labels,
        "filter": subscription.filter,
        "enableMessageOrdering": subscription.enable_message_ordering,
        "enableExactlyOnceDelivery": subscription.enable_exactly_once_delivery,
        "state": subscription.state
    })
}

async fn pubsub_resources(
    State(state): State<UiState>,
    Query(query): Query<ProjectQuery>,
) -> Response {
    if !valid_segment(&query.project) {
        return api_error(StatusCode::BAD_REQUEST, "invalid project ID");
    }
    let project = format!("projects/{}", query.project);
    let (topics, subscriptions) = tokio::join!(
        state.pubsub.list_topic_values(&project),
        state.pubsub.list_subscription_values(&project)
    );
    match (topics, subscriptions) {
        (Ok(topics), Ok(subscriptions)) => {
            let associated = subscriptions
                .iter()
                .map(|s| s.name.clone())
                .collect::<BTreeSet<_>>();
            let detached = subscriptions
                .iter()
                .filter(|s| !topics.iter().any(|t| t.name == s.topic))
                .map(subscription_json)
                .collect::<Vec<_>>();
            Json(json!({"topics": topics.iter().map(|t| topic_json(t, &subscriptions)).collect::<Vec<_>>(), "subscriptions": associated, "detachedSubscriptions": detached})).into_response()
        }
        (Err(error), _) | (_, Err(error)) => api_error(StatusCode::BAD_GATEWAY, error.message()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        firestore::google::firestore::v1::{value::ValueType, Document, Value as FirestoreValue},
        pubsub::google::pubsub::v1::{publisher_server::Publisher, subscriber_server::Subscriber},
    };
    use axum::{
        body::{to_bytes, Body},
        http::Request as HttpRequest,
    };
    use std::collections::HashMap;
    use tower::ServiceExt;

    fn state() -> UiState {
        UiState::new(
            "demo-ui".into(),
            "127.0.0.1:9".parse().unwrap(),
            FirestoreService::default(),
            PubSubService::new(None),
        )
    }

    #[tokio::test]
    async fn embedded_console_and_empty_views_are_available() {
        let app = router(state());
        let response = app
            .clone()
            .oneshot(HttpRequest::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("Firestore"));
        for uri in [
            "/api/firestore/documents?project=demo-ui",
            "/api/pubsub?project=demo-ui",
        ] {
            let response = app
                .clone()
                .oneshot(HttpRequest::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
    }

    fn document(name: &str) -> Document {
        Document {
            name: name.into(),
            fields: std::collections::HashMap::from([
                (
                    "large".into(),
                    FirestoreValue {
                        value_type: Some(ValueType::IntegerValue(i64::MAX)),
                    },
                ),
                (
                    "bytes".into(),
                    FirestoreValue {
                        value_type: Some(ValueType::BytesValue(vec![0, 255, 7])),
                    },
                ),
            ]),
            ..Default::default()
        }
    }

    async fn request_json(app: Router, method: &str, uri: &str, body: Value) -> Response {
        app.oneshot(
            HttpRequest::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn clone_preserves_types_separates_descendants_and_refuses_overwrite() {
        let state = state();
        let root = "projects/demo-ui/databases/(default)/documents/items/source";
        for value in [document(root), document(&format!("{root}/children/child"))] {
            state
                .firestore
                .store
                .lock()
                .await
                .documents
                .insert(value.name.clone(), value);
        }
        let app = router(state.clone());
        let body = json!({"project":"demo-ui","sourcePath":"items/source","destinationCollectionPath":"copies","destinationId":"one"});
        assert_eq!(
            request_json(app.clone(), "POST", "/api/firestore/clone", body.clone())
                .await
                .status(),
            StatusCode::OK
        );
        let copied = state
            .firestore
            .database_documents("projects/demo-ui/databases/(default)")
            .await
            .unwrap();
        let clone = copied
            .iter()
            .find(|d| d.name.ends_with("/copies/one"))
            .unwrap();
        assert_eq!(clone.fields, document(root).fields);
        assert!(!copied
            .iter()
            .any(|d| d.name.contains("/copies/one/children/")));
        assert_eq!(
            request_json(app.clone(), "POST", "/api/firestore/clone", body)
                .await
                .status(),
            StatusCode::CONFLICT
        );
        let descendants = json!({"project":"demo-ui","sourcePath":"items/source","destinationCollectionPath":"copies","destinationId":"two","includeSubcollections":true});
        assert_eq!(
            request_json(app, "POST", "/api/firestore/clone", descendants)
                .await
                .status(),
            StatusCode::OK
        );
        let copied = state
            .firestore
            .database_documents("projects/demo-ui/databases/(default)")
            .await
            .unwrap();
        assert!(copied
            .iter()
            .any(|d| d.name.ends_with("/copies/two/children/child")));
    }

    #[tokio::test]
    async fn inline_field_update_preserves_type_and_literal_field_name() {
        let state = state();
        let root = "projects/demo-ui/databases/(default)/documents/items/source";
        state
            .firestore
            .store
            .lock()
            .await
            .documents
            .insert(root.into(), document(root));
        let app = router(state.clone());
        let response = request_json(
            app.clone(),
            "POST",
            "/api/firestore/field",
            json!({
                "project":"demo-ui",
                "documentPath":"items/source",
                "fieldName":"literal.dot`field",
                "value":{"integerValue":"9223372036854775806"}
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let documents = state
            .firestore
            .database_documents("projects/demo-ui/databases/(default)")
            .await
            .unwrap();
        assert_eq!(
            documents[0].fields["literal.dot`field"].value_type,
            Some(ValueType::IntegerValue(9_223_372_036_854_775_806))
        );
        assert_eq!(
            documents[0].fields["bytes"].value_type,
            document(root).fields["bytes"].value_type
        );

        let response = request_json(
            app,
            "POST",
            "/api/firestore/field",
            json!({
                "project":"demo-ui",
                "documentPath":"items/source",
                "fieldName":"bytes",
                "value":{"bytesValue":"not base64"}
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let documents = state
            .firestore
            .database_documents("projects/demo-ui/databases/(default)")
            .await
            .unwrap();
        assert_eq!(
            documents[0].fields["bytes"].value_type,
            document(root).fields["bytes"].value_type
        );
    }

    #[tokio::test]
    async fn pubsub_view_lists_configuration_without_pulling_messages() {
        let state = state();
        Publisher::create_topic(
            &state.pubsub,
            Request::new(Topic {
                name: "projects/demo-ui/topics/events".into(),
                labels: HashMap::from([("env".into(), "local".into())]),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        Subscriber::create_subscription(
            &state.pubsub,
            Request::new(Subscription {
                name: "projects/demo-ui/subscriptions/worker".into(),
                topic: "projects/demo-ui/topics/events".into(),
                ack_deadline_seconds: 30,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let response = router(state)
            .oneshot(
                HttpRequest::builder()
                    .uri("/api/pubsub?project=demo-ui")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(
            body["topics"][0]["subscriptions"][0]["ackDeadlineSeconds"],
            30
        );
        assert_eq!(body["topics"][0]["labels"]["env"], "local");
    }

    #[tokio::test]
    async fn auth_connection_failure_is_reported() {
        let response = router(state())
            .oneshot(
                HttpRequest::builder()
                    .uri("/api/auth/users?project=demo-ui")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
