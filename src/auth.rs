use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::RwLock;
use uuid::Uuid;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

fn environment_namespace() -> String {
    project_namespace(
        &std::env::var("GCLOUD_PROJECT")
            .or_else(|_| std::env::var("GOOGLE_CLOUD_PROJECT"))
            .unwrap_or_else(|_| "demo-firebase".into()),
    )
}

type UserTable = HashMap<String, BTreeMap<String, User>>;

const ID_TOKEN_TTL_SECS: u64 = 60 * 60;
const REFRESH_TOKEN_TTL_SECS: u64 = 30 * 24 * 60 * 60;

#[derive(Clone, Copy, PartialEq, Eq)]
enum SessionKind {
    IdToken,
    RefreshToken,
}

#[derive(Clone)]
struct Session {
    namespace: String,
    uid: String,
    issued_at: u64,
    expires_at: u64,
    kind: SessionKind,
}

#[derive(Clone)]
struct AuthState {
    users: Arc<RwLock<UserTable>>,
    sessions: Arc<RwLock<HashMap<String, Session>>>,
    default_namespace: String,
}

impl Default for AuthState {
    fn default() -> Self {
        Self::new(None)
    }
}

impl AuthState {
    fn new(project_id: Option<&str>) -> Self {
        Self {
            users: Default::default(),
            sessions: Default::default(),
            default_namespace: project_id
                .map(project_namespace)
                .unwrap_or_else(environment_namespace),
        }
    }
}

/// Builds an Auth emulator router with its own in-memory user store.
///
/// The project-scoped routes are the routes used by the Firebase Admin SDK.
/// Unscoped routes are also accepted for direct Identity Toolkit clients.
#[cfg(test)]
pub fn router() -> Router {
    router_for_project(None)
}

/// Builds the Auth router with an explicit project for browser Identity Toolkit
/// routes. Admin SDK project-scoped routes continue to use the project in the URL.
pub fn router_for_project(project_id: Option<&str>) -> Router {
    let state = AuthState::new(project_id);

    Router::new()
        .route(
            "/identitytoolkit.googleapis.com/v1/accounts:signUp",
            post(sign_up),
        )
        .route(
            "/identitytoolkit.googleapis.com/v1/accounts:signInWithPassword",
            post(sign_in),
        )
        .route("/securetoken.googleapis.com/v1/token", post(refresh_token))
        .route(
            "/identitytoolkit.googleapis.com/v1/accounts",
            post(create_unscoped),
        )
        .route(
            "/identitytoolkit.googleapis.com/v1/accounts:lookup",
            post(lookup_unscoped),
        )
        .route(
            "/identitytoolkit.googleapis.com/v1/accounts:update",
            post(update_unscoped),
        )
        .route(
            "/identitytoolkit.googleapis.com/v1/accounts:delete",
            post(delete_unscoped),
        )
        .route(
            "/identitytoolkit.googleapis.com/v1/accounts:query",
            post(query_unscoped),
        )
        .route(
            "/identitytoolkit.googleapis.com/v1/accounts:batchGet",
            get(batch_get_unscoped),
        )
        .route(
            "/identitytoolkit.googleapis.com/v1/projects/{project_id}/accounts",
            post(create_scoped),
        )
        .route(
            "/identitytoolkit.googleapis.com/v1/projects/{project_id}/accounts:lookup",
            post(lookup_scoped),
        )
        .route(
            "/identitytoolkit.googleapis.com/v1/projects/{project_id}/accounts:update",
            post(update_scoped),
        )
        .route(
            "/identitytoolkit.googleapis.com/v1/projects/{project_id}/accounts:delete",
            post(delete_scoped),
        )
        .route(
            "/identitytoolkit.googleapis.com/v1/projects/{project_id}/accounts:query",
            post(query_scoped),
        )
        .route(
            "/identitytoolkit.googleapis.com/v1/projects/{project_id}/accounts:batchGet",
            get(batch_get_scoped),
        )
        .layer(tower_http::cors::CorsLayer::permissive())
        .with_state(state)
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct User {
    local_id: String,
    #[serde(skip)]
    password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    email: Option<String>,
    email_verified: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    photo_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    phone_number: Option<String>,
    disabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    custom_attributes: Option<String>,
    provider_user_info: Vec<Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    mfa_info: Vec<Value>,
    valid_since: String,
    created_at: String,
    last_login_at: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WriteAccountRequest {
    local_id: Option<String>,
    email: Option<String>,
    email_verified: Option<bool>,
    display_name: Option<String>,
    photo_url: Option<String>,
    phone_number: Option<String>,
    disabled: Option<bool>,
    disable_user: Option<bool>,
    custom_attributes: Option<String>,
    valid_since: Option<String>,
    #[serde(default)]
    delete_attribute: Vec<String>,
    #[serde(default)]
    delete_provider: Vec<String>,
    #[serde(default)]
    link_provider_user_info: Vec<Value>,
    #[serde(default)]
    provider_user_info: Vec<Value>,
    #[serde(default)]
    mfa: Vec<Value>,
    #[serde(default)]
    mfa_info: Vec<Value>,
    password: Option<String>,
    id_token: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LookupRequest {
    id_token: Option<String>,
    #[serde(default)]
    local_id: Vec<String>,
    #[serde(default)]
    email: Vec<String>,
    #[serde(default)]
    phone_number: Vec<String>,
    #[serde(default)]
    federated_user_id: Vec<FederatedId>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FederatedId {
    provider_id: String,
    raw_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeleteRequest {
    #[serde(default)]
    local_id: String,
    id_token: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListRequest {
    max_results: Option<usize>,
    page_size: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListQuery {
    max_results: Option<usize>,
    #[allow(dead_code)]
    next_page_token: Option<String>,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: &'static str,
}

impl ApiError {
    fn bad_request(message: &'static str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({
            "error": {
                "code": self.status.as_u16(),
                "message": self.message,
                "errors": [{
                    "message": self.message,
                    "domain": "global",
                    "reason": "invalid"
                }]
            }
        });
        (self.status, Json(body)).into_response()
    }
}

type ApiResult = Result<Json<Value>, ApiError>;

async fn create_unscoped(
    State(state): State<AuthState>,
    Json(request): Json<WriteAccountRequest>,
) -> ApiResult {
    let namespace = state.default_namespace.clone();
    create_account(state, namespace, request).await
}

async fn create_scoped(
    State(state): State<AuthState>,
    Path(project_id): Path<String>,
    Json(request): Json<WriteAccountRequest>,
) -> ApiResult {
    create_account(state, project_namespace(&project_id), request).await
}

async fn lookup_unscoped(
    State(state): State<AuthState>,
    Json(request): Json<LookupRequest>,
) -> ApiResult {
    let namespace = state.default_namespace.clone();
    lookup_accounts(state, namespace, request).await
}

async fn lookup_scoped(
    State(state): State<AuthState>,
    Path(project_id): Path<String>,
    Json(request): Json<LookupRequest>,
) -> ApiResult {
    lookup_accounts(state, project_namespace(&project_id), request).await
}

async fn update_unscoped(
    State(state): State<AuthState>,
    Json(request): Json<WriteAccountRequest>,
) -> ApiResult {
    let namespace = state.default_namespace.clone();
    update_account(state, namespace, request).await
}

async fn update_scoped(
    State(state): State<AuthState>,
    Path(project_id): Path<String>,
    Json(request): Json<WriteAccountRequest>,
) -> ApiResult {
    update_account(state, project_namespace(&project_id), request).await
}

async fn delete_unscoped(
    State(state): State<AuthState>,
    Json(request): Json<DeleteRequest>,
) -> ApiResult {
    let namespace = state.default_namespace.clone();
    delete_account(state, namespace, request).await
}

async fn delete_scoped(
    State(state): State<AuthState>,
    Path(project_id): Path<String>,
    Json(request): Json<DeleteRequest>,
) -> ApiResult {
    delete_account(state, project_namespace(&project_id), request).await
}

async fn query_unscoped(
    State(state): State<AuthState>,
    Json(request): Json<ListRequest>,
) -> ApiResult {
    let namespace = state.default_namespace.clone();
    query_accounts(state, namespace, request).await
}

async fn query_scoped(
    State(state): State<AuthState>,
    Path(project_id): Path<String>,
    Json(request): Json<ListRequest>,
) -> ApiResult {
    query_accounts(state, project_namespace(&project_id), request).await
}

async fn batch_get_unscoped(
    State(state): State<AuthState>,
    Query(request): Query<ListQuery>,
) -> ApiResult {
    let namespace = state.default_namespace.clone();
    batch_get_accounts(state, namespace, request).await
}

async fn batch_get_scoped(
    State(state): State<AuthState>,
    Path(project_id): Path<String>,
    Query(request): Query<ListQuery>,
) -> ApiResult {
    batch_get_accounts(state, project_namespace(&project_id), request).await
}

fn project_namespace(project_id: &str) -> String {
    format!("project/{project_id}")
}

async fn create_account(
    state: AuthState,
    namespace: String,
    request: WriteAccountRequest,
) -> ApiResult {
    validate_credentials(&request)?;
    let mut tables = state.users.write().await;
    let users = tables.entry(namespace.clone()).or_default();
    let local_id = request
        .local_id
        .unwrap_or_else(|| Uuid::new_v4().simple().to_string());

    if local_id.is_empty() {
        return Err(ApiError::bad_request("INVALID_LOCAL_ID"));
    }
    if users.contains_key(&local_id) {
        return Err(ApiError::bad_request("DUPLICATE_LOCAL_ID"));
    }
    ensure_unique(
        users,
        request.email.as_deref(),
        request.phone_number.as_deref(),
        None,
    )?;

    let now = now_millis();
    let user = User {
        local_id: local_id.clone(),
        password: request.password,
        email: request.email,
        email_verified: request.email_verified.unwrap_or(false),
        display_name: request.display_name,
        photo_url: request.photo_url,
        phone_number: request.phone_number,
        disabled: request.disable_user.or(request.disabled).unwrap_or(false),
        custom_attributes: request.custom_attributes,
        provider_user_info: if request.link_provider_user_info.is_empty() {
            request.provider_user_info
        } else {
            request.link_provider_user_info
        },
        mfa_info: if request.mfa.is_empty() {
            request.mfa_info
        } else {
            request.mfa
        },
        valid_since: request.valid_since.unwrap_or_else(|| "0".to_owned()),
        created_at: now.clone(),
        last_login_at: now,
    };

    let mut user = user;
    sync_password_provider(&mut user);
    users.insert(local_id, user.clone());
    Ok(Json(
        serde_json::to_value(user).expect("User serialization cannot fail"),
    ))
}

async fn lookup_accounts(
    state: AuthState,
    namespace: String,
    mut request: LookupRequest,
) -> ApiResult {
    if let Some(token) = request.id_token.take() {
        request.local_id = vec![session_user(&state, &namespace, &token).await?.local_id];
    }
    let tables = state.users.read().await;
    let Some(users) = tables.get(&namespace) else {
        return Ok(Json(json!({"users": []})));
    };

    let ids: HashSet<&str> = request.local_id.iter().map(String::as_str).collect();
    let emails: HashSet<String> = request.email.iter().map(|s| s.to_lowercase()).collect();
    let phones: HashSet<&str> = request.phone_number.iter().map(String::as_str).collect();
    let found: Vec<&User> = users
        .values()
        .filter(|user| {
            ids.contains(user.local_id.as_str())
                || user
                    .email
                    .as_ref()
                    .is_some_and(|email| emails.contains(&email.to_lowercase()))
                || user
                    .phone_number
                    .as_deref()
                    .is_some_and(|phone| phones.contains(phone))
                || request.federated_user_id.iter().any(|wanted| {
                    user.provider_user_info.iter().any(|provider| {
                        provider.get("providerId").and_then(Value::as_str)
                            == Some(wanted.provider_id.as_str())
                            && provider.get("rawId").and_then(Value::as_str)
                                == Some(wanted.raw_id.as_str())
                    })
                })
        })
        .collect();

    Ok(Json(json!({"users": found})))
}

async fn update_account(
    state: AuthState,
    namespace: String,
    mut request: WriteAccountRequest,
) -> ApiResult {
    let browser = request.id_token.is_some();
    if let Some(token) = request.id_token.take() {
        request.local_id = Some(session_user(&state, &namespace, &token).await?.local_id);
    }
    validate_credentials(&request)?;
    let local_id = request
        .local_id
        .take()
        .ok_or_else(|| ApiError::bad_request("MISSING_LOCAL_ID"))?;
    let mut tables = state.users.write().await;
    let users = tables.entry(namespace.clone()).or_default();
    if !users.contains_key(&local_id) {
        return Err(ApiError::bad_request("USER_NOT_FOUND"));
    }
    ensure_unique(
        users,
        request.email.as_deref(),
        request.phone_number.as_deref(),
        Some(&local_id),
    )?;

    let user = users
        .get_mut(&local_id)
        .expect("user existence checked above");
    apply_deletions(user, &request.delete_attribute, &request.delete_provider);

    if request.password.is_some() {
        user.password = request.password;
    }
    if request.email.is_some() {
        user.email = request.email;
    }
    if let Some(value) = request.email_verified {
        user.email_verified = value;
    }
    if request.display_name.is_some() {
        user.display_name = request.display_name;
    }
    if request.photo_url.is_some() {
        user.photo_url = request.photo_url;
    }
    if request.phone_number.is_some() {
        user.phone_number = request.phone_number;
    }
    if let Some(value) = request.disable_user.or(request.disabled) {
        user.disabled = value;
    }
    if request.custom_attributes.is_some() {
        user.custom_attributes = request.custom_attributes;
    }
    if let Some(value) = request.valid_since {
        user.valid_since = value;
    }
    for provider in request.link_provider_user_info {
        upsert_provider(&mut user.provider_user_info, provider);
    }
    if !request.provider_user_info.is_empty() {
        user.provider_user_info = request.provider_user_info;
    }
    if !request.mfa.is_empty() {
        user.mfa_info = request.mfa;
    } else if !request.mfa_info.is_empty() {
        user.mfa_info = request.mfa_info;
    }

    sync_password_provider(user);
    let user = user.clone();
    drop(tables);
    let mut value = serde_json::to_value(&user).unwrap();
    if browser {
        let session = issue_session(&state, &namespace, &user).await;
        value
            .as_object_mut()
            .unwrap()
            .extend(session.as_object().unwrap().clone());
    }
    Ok(Json(value))
}

async fn delete_account(
    state: AuthState,
    namespace: String,
    mut request: DeleteRequest,
) -> ApiResult {
    if let Some(token) = request.id_token.take() {
        request.local_id = session_user(&state, &namespace, &token).await?.local_id;
    }
    let mut tables = state.users.write().await;
    let deleted = tables
        .entry(namespace.clone())
        .or_default()
        .remove(&request.local_id);
    if deleted.is_none() {
        return Err(ApiError::bad_request("USER_NOT_FOUND"));
    }
    drop(tables);
    state
        .sessions
        .write()
        .await
        .retain(|_, session| session.namespace != namespace || session.uid != request.local_id);
    Ok(Json(json!({})))
}

async fn query_accounts(state: AuthState, namespace: String, request: ListRequest) -> ApiResult {
    let limit = request.max_results.or(request.page_size).unwrap_or(1000);
    let users = list_users(&state, &namespace, limit).await;
    // projects.accounts.query calls the field userInfo. Including users as an
    // alias also makes this useful to small direct clients.
    Ok(Json(json!({"userInfo": users, "users": users})))
}

async fn batch_get_accounts(state: AuthState, namespace: String, request: ListQuery) -> ApiResult {
    let users = list_users(&state, &namespace, request.max_results.unwrap_or(1000)).await;
    Ok(Json(json!({"users": users})))
}

async fn list_users(state: &AuthState, namespace: &str, limit: usize) -> Vec<User> {
    let tables = state.users.read().await;
    tables
        .get(namespace)
        .into_iter()
        .flat_map(|users| users.values())
        .take(limit.min(1000))
        .cloned()
        .collect()
}

fn ensure_unique(
    users: &BTreeMap<String, User>,
    email: Option<&str>,
    phone: Option<&str>,
    except_local_id: Option<&str>,
) -> Result<(), ApiError> {
    for user in users.values() {
        if except_local_id == Some(user.local_id.as_str()) {
            continue;
        }
        if email.is_some_and(|candidate| {
            user.email
                .as_deref()
                .is_some_and(|current| current.eq_ignore_ascii_case(candidate))
        }) {
            return Err(ApiError::bad_request("EMAIL_EXISTS"));
        }
        if phone.is_some_and(|candidate| user.phone_number.as_deref() == Some(candidate)) {
            return Err(ApiError::bad_request("PHONE_NUMBER_EXISTS"));
        }
    }
    Ok(())
}

fn apply_deletions(user: &mut User, attributes: &[String], providers: &[String]) {
    for attribute in attributes {
        match attribute.as_str() {
            "EMAIL" => {
                user.email = None;
                user.email_verified = false;
            }
            "DISPLAY_NAME" => user.display_name = None,
            "PHOTO_URL" => user.photo_url = None,
            "PHONE_NUMBER" => user.phone_number = None,
            "CUSTOM_ATTRIBUTES" => user.custom_attributes = None,
            _ => {}
        }
    }
    if !providers.is_empty() {
        user.provider_user_info.retain(|provider| {
            provider
                .get("providerId")
                .and_then(Value::as_str)
                .is_none_or(|id| !providers.iter().any(|removed| removed == id))
        });
    }
}

fn upsert_provider(providers: &mut Vec<Value>, new_provider: Value) {
    let provider_id = new_provider.get("providerId").and_then(Value::as_str);
    if let Some(index) = providers
        .iter()
        .position(|provider| provider.get("providerId").and_then(Value::as_str) == provider_id)
    {
        providers[index] = new_provider;
    } else {
        providers.push(new_provider);
    }
}

fn now_millis() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_request(local_id: &str, email: &str) -> WriteAccountRequest {
        WriteAccountRequest {
            local_id: Some(local_id.to_owned()),
            email: Some(email.to_owned()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn create_lookup_update_list_delete_round_trip() {
        let state = AuthState::default();
        let namespace = project_namespace("demo-test");

        let created = create_account(
            state.clone(),
            namespace.clone(),
            write_request("user-1", "old@example.test"),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(created["localId"], "user-1");
        assert_eq!(created["emailVerified"], false);
        assert!(created["createdAt"].as_str().is_some());

        let looked_up = lookup_accounts(
            state.clone(),
            namespace.clone(),
            LookupRequest {
                local_id: vec!["user-1".to_owned()],
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .0;
        assert_eq!(looked_up["users"][0]["email"], "old@example.test");

        let updated = update_account(
            state.clone(),
            namespace.clone(),
            WriteAccountRequest {
                local_id: Some("user-1".to_owned()),
                email: Some("new@example.test".to_owned()),
                display_name: Some("Test User".to_owned()),
                email_verified: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .0;
        assert_eq!(updated["email"], "new@example.test");
        assert_eq!(updated["displayName"], "Test User");
        assert_eq!(updated["emailVerified"], true);

        let listed = batch_get_accounts(
            state.clone(),
            namespace.clone(),
            ListQuery {
                max_results: Some(100),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .0;
        assert_eq!(listed["users"].as_array().unwrap().len(), 1);

        let _ = delete_account(
            state.clone(),
            namespace.clone(),
            DeleteRequest {
                local_id: "user-1".to_owned(),
                id_token: None,
            },
        )
        .await
        .unwrap();

        let missing = lookup_accounts(
            state,
            namespace,
            LookupRequest {
                local_id: vec!["user-1".to_owned()],
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .0;
        assert!(missing["users"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn enforces_unique_email_case_insensitively() {
        let state = AuthState::default();
        let namespace = project_namespace("demo-test");
        let _ = create_account(
            state.clone(),
            namespace.clone(),
            write_request("one", "User@Example.test"),
        )
        .await
        .unwrap();

        let error = create_account(state, namespace, write_request("two", "user@example.test"))
            .await
            .unwrap_err();
        assert_eq!(error.message, "EMAIL_EXISTS");
    }

    #[tokio::test]
    async fn keeps_projects_isolated() {
        let state = AuthState::default();
        let _ = create_account(
            state.clone(),
            project_namespace("project-a"),
            write_request("same-uid", "same@example.test"),
        )
        .await
        .unwrap();
        let _ = create_account(
            state.clone(),
            project_namespace("project-b"),
            write_request("same-uid", "same@example.test"),
        )
        .await
        .unwrap();

        let users = list_users(&state, &project_namespace("project-a"), 100).await;
        assert_eq!(users.len(), 1);
    }

    #[test]
    fn exposes_admin_sdk_routes() {
        let _ = router();
    }
}

fn validate_credentials(request: &WriteAccountRequest) -> Result<(), ApiError> {
    if request.email.as_ref().is_some_and(|email| {
        let parts: Vec<_> = email.split('@').collect();
        parts.len() != 2
            || parts[0].is_empty()
            || parts[1].is_empty()
            || email.chars().any(char::is_whitespace)
    }) {
        return Err(ApiError::bad_request("INVALID_EMAIL"));
    }
    if request
        .password
        .as_ref()
        .is_some_and(|password| password.len() < 6)
    {
        return Err(ApiError::bad_request(
            "WEAK_PASSWORD : Password should be at least 6 characters",
        ));
    }
    Ok(())
}

fn sync_password_provider(user: &mut User) {
    if let (Some(email), Some(_)) = (&user.email, &user.password) {
        upsert_provider(
            &mut user.provider_user_info,
            json!({"providerId":"password", "rawId":email,"email":email,"displayName":user.display_name,"photoUrl":user.photo_url}),
        );
    }
}

async fn sign_up(
    State(state): State<AuthState>,
    Json(request): Json<WriteAccountRequest>,
) -> ApiResult {
    if request.email.is_some() && request.password.is_none() {
        return Err(ApiError::bad_request("MISSING_PASSWORD"));
    }
    let namespace = state.default_namespace.clone();
    let created = create_account(state.clone(), namespace.clone(), request)
        .await?
        .0;
    let user = state.users.read().await[&namespace][created["localId"].as_str().unwrap()].clone();
    Ok(Json(issue_session(&state, &namespace, &user).await))
}

async fn sign_in(
    State(state): State<AuthState>,
    Json(request): Json<WriteAccountRequest>,
) -> ApiResult {
    let namespace = state.default_namespace.clone();
    let email = request
        .email
        .ok_or_else(|| ApiError::bad_request("MISSING_EMAIL"))?;
    let password = request
        .password
        .ok_or_else(|| ApiError::bad_request("MISSING_PASSWORD"))?;
    let mut tables = state.users.write().await;
    let user = tables
        .entry(namespace.clone())
        .or_default()
        .values_mut()
        .find(|u| {
            u.email
                .as_deref()
                .is_some_and(|e| e.eq_ignore_ascii_case(&email))
        })
        .ok_or_else(|| ApiError::bad_request("EMAIL_NOT_FOUND"))?;
    if user.disabled {
        return Err(ApiError::bad_request("USER_DISABLED"));
    }
    if user.password.as_deref() != Some(password.as_str()) {
        return Err(ApiError::bad_request("INVALID_PASSWORD"));
    }
    user.last_login_at = now_millis();
    let user = user.clone();
    drop(tables);
    Ok(Json(issue_session(&state, &namespace, &user).await))
}

async fn issue_session(state: &AuthState, namespace: &str, user: &User) -> Value {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let project = namespace.strip_prefix("project/").unwrap_or(namespace);
    let provider = if user.password.is_some() {
        "password"
    } else {
        "anonymous"
    };
    let mut claims = user
        .custom_attributes
        .as_deref()
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .filter(Value::is_object)
        .unwrap_or(json!({}));
    let mut identities = json!({});
    if let Some(email) = &user.email {
        identities["email"] = json!([email]);
    }
    let standard = json!({"iss":format!("https://securetoken.google.com/{project}"),"aud":project,"auth_time":now,"user_id":user.local_id,"sub":user.local_id,"iat":now,"exp":now+ID_TOKEN_TTL_SECS,"email_verified":user.email_verified,"firebase":{"identities":identities,"sign_in_provider":provider}});
    claims
        .as_object_mut()
        .unwrap()
        .extend(standard.as_object().unwrap().clone());
    if let Some(email) = &user.email {
        claims["email"] = json!(email);
    }
    if let Some(name) = &user.display_name {
        claims["name"] = json!(name);
    }
    let token = format!(
        "{}.{}.",
        URL_SAFE_NO_PAD.encode(br#"{"alg":"none","typ":"JWT"}"#),
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap())
    );
    let refresh = Uuid::new_v4().to_string();
    let mut sessions = state.sessions.write().await;
    sessions.retain(|_, session| session.expires_at > now);
    sessions.insert(
        token.clone(),
        Session {
            namespace: namespace.into(),
            uid: user.local_id.clone(),
            issued_at: now,
            expires_at: now + ID_TOKEN_TTL_SECS,
            kind: SessionKind::IdToken,
        },
    );
    sessions.insert(
        refresh.clone(),
        Session {
            namespace: namespace.into(),
            uid: user.local_id.clone(),
            issued_at: now,
            expires_at: now + REFRESH_TOKEN_TTL_SECS,
            kind: SessionKind::RefreshToken,
        },
    );
    json!({"localId":user.local_id,"email":user.email,"displayName":user.display_name,"idToken":token,"refreshToken":refresh,"expiresIn":"3600","registered":true})
}

async fn session_user(state: &AuthState, namespace: &str, token: &str) -> Result<User, ApiError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let session = state
        .sessions
        .read()
        .await
        .get(token)
        .cloned()
        .ok_or_else(|| ApiError::bad_request("INVALID_ID_TOKEN"))?;
    if session.kind != SessionKind::IdToken || session.namespace != namespace {
        return Err(ApiError::bad_request("INVALID_ID_TOKEN"));
    }
    if session.expires_at <= now {
        state.sessions.write().await.remove(token);
        return Err(ApiError::bad_request("TOKEN_EXPIRED"));
    }
    let claims = token
        .split('.')
        .nth(1)
        .and_then(|v| URL_SAFE_NO_PAD.decode(v).ok())
        .and_then(|v| serde_json::from_slice::<Value>(&v).ok())
        .ok_or_else(|| ApiError::bad_request("INVALID_ID_TOKEN"))?;
    if claims["exp"].as_u64().unwrap_or(0) <= now {
        return Err(ApiError::bad_request("TOKEN_EXPIRED"));
    }
    let user = state
        .users
        .read()
        .await
        .get(namespace)
        .and_then(|users| users.get(&session.uid))
        .cloned()
        .ok_or_else(|| ApiError::bad_request("USER_NOT_FOUND"))?;
    if user.disabled {
        return Err(ApiError::bad_request("USER_DISABLED"));
    }
    if user.valid_since.parse::<u64>().unwrap_or(0) > claims["auth_time"].as_u64().unwrap_or(0) {
        return Err(ApiError::bad_request("TOKEN_EXPIRED"));
    }
    Ok(user)
}

async fn refresh_token(
    State(state): State<AuthState>,
    axum::Form(request): axum::Form<HashMap<String, String>>,
) -> ApiResult {
    if request.get("grant_type").map(String::as_str) != Some("refresh_token") {
        return Err(ApiError::bad_request("INVALID_GRANT_TYPE"));
    }
    let refresh = request
        .get("refresh_token")
        .ok_or_else(|| ApiError::bad_request("MISSING_REFRESH_TOKEN"))?;
    if Uuid::parse_str(refresh).is_err() {
        return Err(ApiError::bad_request("INVALID_REFRESH_TOKEN"));
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let session = state
        .sessions
        .read()
        .await
        .get(refresh)
        .cloned()
        .ok_or_else(|| ApiError::bad_request("INVALID_REFRESH_TOKEN"))?;
    if session.kind != SessionKind::RefreshToken || session.expires_at <= now {
        state.sessions.write().await.remove(refresh);
        return Err(ApiError::bad_request("INVALID_REFRESH_TOKEN"));
    }
    let namespace = session.namespace;
    let uid = session.uid;
    let user = state
        .users
        .read()
        .await
        .get(&namespace)
        .and_then(|users| users.get(&uid))
        .cloned()
        .ok_or_else(|| ApiError::bad_request("USER_NOT_FOUND"))?;
    if user.disabled {
        return Err(ApiError::bad_request("USER_DISABLED"));
    }
    if user.valid_since.parse::<u64>().unwrap_or(0) > session.issued_at {
        return Err(ApiError::bad_request("TOKEN_EXPIRED"));
    }
    let session = issue_session(&state, &namespace, &user).await;
    Ok(Json(
        json!({"access_token":session["idToken"],"id_token":session["idToken"],"refresh_token":session["refreshToken"],"expires_in":"3600","token_type":"Bearer","user_id":uid,"project_id":namespace.strip_prefix("project/").unwrap_or(&namespace)}),
    ))
}

#[cfg(test)]
mod session_tests {
    use super::*;
    fn token_claims(token: &str) -> Value {
        let payload = token.split('.').nth(1).unwrap();
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).unwrap()).unwrap()
    }
    fn credentials(password: &str) -> WriteAccountRequest {
        WriteAccountRequest {
            email: Some("session@example.test".into()),
            password: Some(password.into()),
            ..Default::default()
        }
    }
    #[tokio::test]
    async fn browser_and_admin_share_users_and_reject_invalid_credentials() {
        let state = AuthState::default();
        let ns = state.default_namespace.clone();
        let session = sign_up(State(state.clone()), Json(credentials("secret123")))
            .await
            .unwrap()
            .0;
        let uid = session["localId"].as_str().unwrap().to_owned();
        assert_eq!(
            sign_in(State(state.clone()), Json(credentials("incorrect")))
                .await
                .unwrap_err()
                .message,
            "INVALID_PASSWORD"
        );
        assert_eq!(
            sign_up(State(state.clone()), Json(credentials("secret123")))
                .await
                .unwrap_err()
                .message,
            "EMAIL_EXISTS"
        );
        let token = session["idToken"].as_str().unwrap();
        let found = lookup_accounts(
            state.clone(),
            ns.clone(),
            LookupRequest {
                id_token: Some(token.into()),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .0;
        assert_eq!(found["users"][0]["localId"], uid);
        assert!(found["users"][0].get("password").is_none());
        let _ = update_account(
            state.clone(),
            ns.clone(),
            WriteAccountRequest {
                local_id: Some(uid.clone()),
                display_name: Some("Admin edit".into()),
                password: Some("newsecret".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            session_user(&state, &ns, token)
                .await
                .unwrap()
                .display_name
                .as_deref(),
            Some("Admin edit")
        );
        assert!(
            sign_in(State(state.clone()), Json(credentials("secret123")))
                .await
                .is_err()
        );
        assert!(
            sign_in(State(state.clone()), Json(credentials("newsecret")))
                .await
                .is_ok()
        );
        assert!(session_user(&state, &project_namespace("other"), token)
            .await
            .is_err());
        assert!(session_user(&state, &ns, "forged.token.").await.is_err());
        let _ = delete_account(
            state.clone(),
            ns.clone(),
            DeleteRequest {
                local_id: uid,
                id_token: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            session_user(&state, &ns, token).await.unwrap_err().message,
            "INVALID_ID_TOKEN"
        );
    }

    #[tokio::test]
    async fn configured_project_claims_survive_signup_signin_and_refresh() {
        let project = "demo-real-app-auth";
        let state = AuthState::new(Some(project));
        let assert_project = |session: &Value, token_key: &str| {
            let claims = token_claims(session[token_key].as_str().unwrap());
            assert_eq!(
                claims["iss"],
                format!("https://securetoken.google.com/{project}")
            );
            assert_eq!(claims["aud"], project);
        };

        let signup = sign_up(State(state.clone()), Json(credentials("secret123")))
            .await
            .unwrap()
            .0;
        assert_project(&signup, "idToken");

        let signin = sign_in(State(state.clone()), Json(credentials("secret123")))
            .await
            .unwrap()
            .0;
        assert_project(&signin, "idToken");

        let refreshed = refresh_token(
            State(state),
            axum::Form(HashMap::from([
                ("grant_type".into(), "refresh_token".into()),
                (
                    "refresh_token".into(),
                    signin["refreshToken"].as_str().unwrap().into(),
                ),
            ])),
        )
        .await
        .unwrap()
        .0;
        assert_project(&refreshed, "id_token");
        assert_eq!(refreshed["project_id"], project);
    }
    #[tokio::test]
    async fn refresh_rejects_id_tokens_and_revoked_sessions() {
        let state = AuthState::default();
        let session = sign_up(State(state.clone()), Json(credentials("secret123")))
            .await
            .unwrap()
            .0;
        let request = |token: &str| {
            axum::Form(HashMap::from([
                ("grant_type".into(), "refresh_token".into()),
                ("refresh_token".into(), token.into()),
            ]))
        };
        assert_eq!(
            refresh_token(
                State(state.clone()),
                request(session["idToken"].as_str().unwrap())
            )
            .await
            .unwrap_err()
            .message,
            "INVALID_REFRESH_TOKEN"
        );
        let _ = refresh_token(
            State(state.clone()),
            request(session["refreshToken"].as_str().unwrap()),
        )
        .await
        .unwrap();
        let _ = update_account(
            state.clone(),
            state.default_namespace.clone(),
            WriteAccountRequest {
                local_id: Some(session["localId"].as_str().unwrap().into()),
                valid_since: Some("9999999999".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            refresh_token(
                State(state.clone()),
                request(session["refreshToken"].as_str().unwrap())
            )
            .await
            .unwrap_err()
            .message,
            "TOKEN_EXPIRED"
        );
        assert_eq!(
            session_user(
                &state,
                &state.default_namespace,
                session["idToken"].as_str().unwrap()
            )
            .await
            .unwrap_err()
            .message,
            "TOKEN_EXPIRED"
        );
    }

    #[tokio::test]
    async fn browser_updates_and_deletes_admin_user() {
        let state = AuthState::default();
        let ns = state.default_namespace.clone();
        let _ = create_account(state.clone(), ns.clone(), credentials("secret123"))
            .await
            .unwrap();
        let session = sign_in(State(state.clone()), Json(credentials("secret123")))
            .await
            .unwrap()
            .0;
        let token = session["idToken"].as_str().unwrap();
        let _ = update_account(
            state.clone(),
            ns.clone(),
            WriteAccountRequest {
                id_token: Some(token.into()),
                display_name: Some("Browser edit".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            list_users(&state, &ns, 10).await[0].display_name.as_deref(),
            Some("Browser edit")
        );
        let _ = delete_account(
            state.clone(),
            ns.clone(),
            DeleteRequest {
                local_id: String::new(),
                id_token: Some(token.into()),
            },
        )
        .await
        .unwrap();
        assert!(list_users(&state, &ns, 10).await.is_empty());
        assert!(state.sessions.read().await.is_empty());
    }

    #[tokio::test]
    async fn expired_sessions_are_pruned_and_refresh_expiry_is_explicit() {
        let state = AuthState::default();
        let ns = state.default_namespace.clone();
        let session = sign_up(State(state.clone()), Json(credentials("secret123")))
            .await
            .unwrap()
            .0;
        let id_token = session["idToken"].as_str().unwrap().to_owned();
        let refresh = session["refreshToken"].as_str().unwrap().to_owned();
        {
            let mut sessions = state.sessions.write().await;
            sessions.get_mut(&id_token).unwrap().expires_at = 0;
            sessions.get_mut(&refresh).unwrap().expires_at = 0;
        }
        assert_eq!(
            session_user(&state, &ns, &id_token)
                .await
                .unwrap_err()
                .message,
            "TOKEN_EXPIRED"
        );
        assert_eq!(
            refresh_token(
                State(state.clone()),
                axum::Form(HashMap::from([
                    ("grant_type".into(), "refresh_token".into()),
                    ("refresh_token".into(), refresh),
                ])),
            )
            .await
            .unwrap_err()
            .message,
            "INVALID_REFRESH_TOKEN"
        );
        assert!(state.sessions.read().await.is_empty());
    }
}
