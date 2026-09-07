//! Small Google Cloud Storage compatible REST emulator.

use crate::persistence::{Error as PersistenceError, Persistence};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use rusqlite::OptionalExtension;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::RwLock;
use uuid::Uuid;

const RESUMABLE_UPLOAD_TTL_SECS: u64 = 60 * 60;
/// Maximum accepted object size and cumulative incomplete resumable upload.
/// This bounds request and session buffering while retaining current SDK fixtures.
const MAX_UPLOAD_BYTES: usize = 64 * 1024 * 1024;

fn crc32c_base64(bytes: &[u8]) -> String {
    BASE64.encode(crc32c::crc32c(bytes).to_be_bytes())
}

#[derive(Clone, Default)]
struct StorageState {
    data: Arc<RwLock<StorageData>>,
    persistence: Option<Persistence>,
    blob_lifecycle: Arc<RwLock<()>>,
    #[cfg(test)]
    media_read_barrier: Option<Arc<std::sync::Barrier>>,
}

#[derive(Default)]
struct StorageData {
    next_generation: u64,
    objects: HashMap<(String, String), StoredObject>,
    uploads: HashMap<Uuid, PendingUpload>,
}

#[derive(Clone)]
struct StoredObject {
    bucket: String,
    name: String,
    bytes: Option<Bytes>,
    blob_name: Option<String>,
    size: u64,
    crc32c: String,
    content_type: String,
    metadata: HashMap<String, String>,
    cache_control: Option<String>,
    content_disposition: Option<String>,
    content_encoding: Option<String>,
    generation: u64,
    created: String,
    updated: String,
    download_token: String,
}

#[derive(Serialize, Deserialize)]
struct StoredMetadata {
    content_type: String,
    metadata: HashMap<String, String>,
    cache_control: Option<String>,
    content_disposition: Option<String>,
    content_encoding: Option<String>,
    download_token: String,
    size: u64,
    crc32c: String,
}

#[derive(Clone)]
struct PendingUpload {
    bucket: String,
    name: String,
    metadata: UploadMetadata,
    bytes: Vec<u8>,
    created_at: u64,
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn prune_expired_uploads(data: &mut StorageData, now: u64) {
    data.uploads
        .retain(|_, upload| upload.created_at.saturating_add(RESUMABLE_UPLOAD_TTL_SECS) > now);
}

fn persistence_failure(error: impl std::fmt::Display) -> Response {
    api_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        &format!("Storage persistence failed: {error}"),
    )
}

fn blob_path(directory: &FsPath, name: &str) -> Result<PathBuf, PersistenceError> {
    let id = Uuid::parse_str(name)
        .map_err(|_| PersistenceError::Corrupt(format!("invalid Storage blob id {name:?}")))?;
    if id.to_string() != name {
        return Err(PersistenceError::Corrupt(format!(
            "non-canonical Storage blob id {name:?}"
        )));
    }
    Ok(directory.join(name))
}

fn metadata_json(object: &StoredObject) -> Result<String, PersistenceError> {
    serde_json::to_string(&StoredMetadata {
        content_type: object.content_type.clone(),
        metadata: object.metadata.clone(),
        cache_control: object.cache_control.clone(),
        content_disposition: object.content_disposition.clone(),
        content_encoding: object.content_encoding.clone(),
        download_token: object.download_token.clone(),
        size: object.size,
        crc32c: object.crc32c.clone(),
    })
    .map_err(|error| PersistenceError::Corrupt(format!("invalid Storage metadata: {error}")))
}

fn stored_object(
    bucket: String,
    name: String,
    blob_name: String,
    metadata: String,
    generation: i64,
    created: String,
    updated: String,
) -> Result<StoredObject, PersistenceError> {
    let metadata: StoredMetadata = serde_json::from_str(&metadata).map_err(|error| {
        PersistenceError::Corrupt(format!(
            "invalid Storage metadata for {bucket}/{name}: {error}"
        ))
    })?;
    if generation <= 0 {
        return Err(PersistenceError::Corrupt(format!(
            "negative Storage generation for {bucket}/{name}"
        )));
    }
    Ok(StoredObject {
        bucket,
        name,
        bytes: None,
        blob_name: Some(blob_name),
        size: metadata.size,
        crc32c: metadata.crc32c,
        content_type: metadata.content_type,
        metadata: metadata.metadata,
        cache_control: metadata.cache_control,
        content_disposition: metadata.content_disposition,
        content_encoding: metadata.content_encoding,
        generation: generation as u64,
        created,
        updated,
        download_token: metadata.download_token,
    })
}

async fn recover_blobs(persistence: &Persistence) -> Result<(), PersistenceError> {
    let referenced: HashSet<String> = persistence
        .read(|connection| {
            let mut statement = connection.prepare("SELECT blob_name FROM storage_objects")?;
            let names = statement
                .query_map([], |row| row.get(0))?
                .collect::<Result<HashSet<String>, _>>()?;
            Ok(names)
        })
        .await?;
    for name in &referenced {
        let path = blob_path(persistence.blobs_dir(), name)?;
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            PersistenceError::Corrupt(format!(
                "Storage metadata references missing blob {name}: {error}"
            ))
        })?;
        if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
            return Err(PersistenceError::Corrupt(format!(
                "Storage metadata references non-file blob {name}"
            )));
        }
    }
    let blobs = persistence.blobs_dir().to_owned();
    let temporary = persistence.temporary_dir().to_owned();
    tokio::task::spawn_blocking(move || -> Result<(), PersistenceError> {
        for entry in fs::read_dir(&temporary)? {
            let entry = entry?;
            if entry.file_type()?.is_file() || entry.file_type()?.is_symlink() {
                fs::remove_file(entry.path())?;
            }
        }
        for entry in fs::read_dir(&blobs)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if !(file_type.is_file() || file_type.is_symlink()) {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if !referenced.contains(&name) {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    })
    .await
    .map_err(|error| PersistenceError::Join(error.to_string()))??;
    Ok(())
}

async fn persistent_object(
    persistence: &Persistence,
    bucket: String,
    name: String,
) -> Result<Option<StoredObject>, PersistenceError> {
    persistence
        .read(move |connection| {
            let row = connection
                .query_row(
                    "SELECT blob_name, metadata_json, generation, created, updated \
                     FROM storage_objects WHERE bucket=?1 AND name=?2",
                    rusqlite::params![bucket, name],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                        ))
                    },
                )
                .optional()?;
            row.map(|(blob, metadata, generation, created, updated)| {
                stored_object(bucket, name, blob, metadata, generation, created, updated)
            })
            .transpose()
        })
        .await
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UploadMetadata {
    name: Option<String>,
    content_type: Option<String>,
    cache_control: Option<String>,
    content_disposition: Option<String>,
    content_encoding: Option<String>,
    #[serde(default)]
    metadata: HashMap<String, String>,
}

#[derive(Default, Deserialize)]
struct UploadQuery {
    #[serde(rename = "uploadType")]
    upload_type: Option<String>,
    name: Option<String>,
    upload_id: Option<String>,
}

#[derive(Default, Deserialize)]
struct ObjectQuery {
    alt: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListQuery {
    prefix: Option<String>,
    delimiter: Option<String>,
    max_results: Option<usize>,
    page_token: Option<String>,
}

/// Build a Storage REST router. `None` preserves the historical in-memory mode.
pub async fn router(persistence: Option<Persistence>) -> Result<Router, PersistenceError> {
    if let Some(store) = &persistence {
        recover_blobs(store).await?;
    }
    let state = StorageState {
        persistence,
        ..StorageState::default()
    };

    Ok(Router::new()
        .route("/storage/v1/b/{bucket}", get(bucket_metadata))
        .route("/storage/v1/b/{bucket}/o", get(list_objects))
        .route(
            "/storage/v1/b/{bucket}/o/{*object}",
            get(get_object).delete(delete_object),
        )
        .route(
            "/upload/storage/v1/b/{bucket}/o",
            post(upload_object).put(resumable_put),
        )
        // @google-cloud/storage uses the emulator origin directly as its
        // base URL, without the production /storage/v1 prefix.
        .route("/b/{bucket}", get(bucket_metadata))
        .route("/b/{bucket}/o", get(list_objects))
        .route(
            "/b/{bucket}/o/{*object}",
            get(get_object).delete(delete_object),
        )
        // Firebase clients use these aliases for the same JSON operations.
        .route("/v0/b/{bucket}", get(bucket_metadata))
        .route("/v0/b/{bucket}/o", get(list_objects).post(upload_object))
        .route(
            "/v0/b/{bucket}/o/{*object}",
            get(get_object).delete(delete_object),
        )
        // Locally generated signed URLs commonly target the XML API shape.
        // Signature and expiry query parameters are deliberately not read.
        .route(
            "/{bucket}/{*object}",
            get(get_signed_object)
                .put(put_signed_object)
                .delete(delete_object),
        )
        .layer(DefaultBodyLimit::max(MAX_UPLOAD_BYTES))
        .layer(tower_http::cors::CorsLayer::permissive())
        .with_state(state))
}

async fn bucket_metadata(Path(bucket): Path<String>) -> Response {
    Json(json!({
        "kind": "storage#bucket",
        "id": bucket,
        "name": bucket,
        "location": "US",
        "storageClass": "STANDARD",
        "metageneration": "1",
    }))
    .into_response()
}

async fn upload_object(
    State(state): State<StorageState>,
    Path(bucket): Path<String>,
    Query(query): Query<UploadQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if query.upload_id.is_some() {
        return resumable_put(State(state), Path(bucket), Query(query), headers, body).await;
    }
    let protocol = header_string(&headers, "x-goog-upload-protocol");
    match query
        .upload_type
        .as_deref()
        .or(protocol.as_deref())
        .unwrap_or("media")
    {
        "resumable" => {
            let mut metadata = if body.is_empty() {
                UploadMetadata::default()
            } else {
                match serde_json::from_slice::<UploadMetadata>(&body) {
                    Ok(value) => value,
                    Err(error) => return api_error(StatusCode::BAD_REQUEST, &error.to_string()),
                }
            };
            let name = match query.name.or_else(|| metadata.name.take()) {
                Some(name) if !name.is_empty() => name,
                _ => return api_error(StatusCode::BAD_REQUEST, "missing object name"),
            };
            if metadata.content_type.is_none() {
                metadata.content_type = header_string(&headers, "x-upload-content-type");
            }
            let id = Uuid::new_v4();
            let mut data = state.data.write().await;
            let now = unix_now();
            prune_expired_uploads(&mut data, now);
            data.uploads.insert(
                id,
                PendingUpload {
                    bucket: bucket.clone(),
                    name,
                    metadata,
                    bytes: Vec::new(),
                    created_at: now,
                },
            );
            let location = format!(
                "http://{}/upload/storage/v1/b/{}/o?uploadType=resumable&upload_id={id}",
                header_string(&headers, "host").unwrap_or_else(|| "127.0.0.1:9199".into()),
                percent_encode(&bucket)
            );
            let mut response = StatusCode::OK.into_response();
            response.headers_mut().insert(
                header::LOCATION,
                HeaderValue::from_str(&location).expect("generated upload location is valid"),
            );
            response.headers_mut().insert(
                HeaderName::from_static("x-guploader-uploadid"),
                HeaderValue::from_str(&id.to_string()).expect("UUID is a valid header value"),
            );
            response.headers_mut().insert(
                "x-goog-upload-url",
                HeaderValue::from_str(&location).unwrap(),
            );
            response
                .headers_mut()
                .insert("x-goog-upload-status", HeaderValue::from_static("active"));
            response
        }
        "multipart" => {
            let content_type =
                header_string(&headers, header::CONTENT_TYPE.as_str()).unwrap_or_default();
            let (metadata, media, media_type) = match parse_multipart(&content_type, &body) {
                Ok(parts) => parts,
                Err(message) => return api_error(StatusCode::BAD_REQUEST, message),
            };
            let name = match query.name.or_else(|| metadata.name.clone()) {
                Some(name) if !name.is_empty() => name,
                _ => return api_error(StatusCode::BAD_REQUEST, "missing object name"),
            };
            save_object(&state, bucket, name, media, metadata, media_type).await
        }
        "media" => {
            let name = match query.name {
                Some(name) if !name.is_empty() => name,
                _ => return api_error(StatusCode::BAD_REQUEST, "missing object name"),
            };
            let mut metadata = metadata_from_headers(&headers);
            metadata.content_type = header_string(&headers, header::CONTENT_TYPE.as_str());
            save_object(&state, bucket, name, body, metadata, None).await
        }
        other => api_error(
            StatusCode::BAD_REQUEST,
            &format!("unsupported uploadType: {other}"),
        ),
    }
}

async fn resumable_put(
    State(state): State<StorageState>,
    Path(_bucket): Path<String>,
    Query(query): Query<UploadQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let id = match query
        .upload_id
        .as_deref()
        .and_then(|value| Uuid::parse_str(value).ok())
    {
        Some(id) => id,
        None => return api_error(StatusCode::BAD_REQUEST, "missing upload_id"),
    };
    let mut data = state.data.write().await;
    prune_expired_uploads(&mut data, unix_now());
    let pending = match data.uploads.get_mut(&id) {
        Some(upload) if upload.bucket == _bucket => upload,
        _ => return api_error(StatusCode::NOT_FOUND, "upload session not found"),
    };
    let command = header_string(&headers, "x-goog-upload-command").unwrap_or_default();
    if command == "cancel" {
        data.uploads.remove(&id);
        return StatusCode::OK.into_response();
    }
    if command == "query" {
        let mut response = StatusCode::OK.into_response();
        response
            .headers_mut()
            .insert("x-goog-upload-status", HeaderValue::from_static("active"));
        response.headers_mut().insert(
            "x-goog-upload-size-received",
            HeaderValue::from_str(&pending.bytes.len().to_string()).unwrap(),
        );
        return response;
    }
    let offset =
        header_string(&headers, "x-goog-upload-offset").and_then(|s| s.parse::<usize>().ok());
    if offset.is_some_and(|offset| offset != pending.bytes.len()) {
        return api_error(
            StatusCode::BAD_REQUEST,
            "upload offset does not match received size",
        );
    }
    if pending.bytes.len().saturating_add(body.len()) > MAX_UPLOAD_BYTES {
        return api_error(StatusCode::PAYLOAD_TOO_LARGE, "object exceeds 64 MiB limit");
    }
    pending.bytes.extend_from_slice(&body);
    if !command.is_empty() && !command.contains("finalize") {
        let mut response = StatusCode::OK.into_response();
        response
            .headers_mut()
            .insert("x-goog-upload-status", HeaderValue::from_static("active"));
        response.headers_mut().insert(
            "x-goog-upload-size-received",
            HeaderValue::from_str(&pending.bytes.len().to_string()).unwrap(),
        );
        return response;
    }
    let pending = data.uploads.remove(&id).unwrap();
    drop(data);
    let size = pending.bytes.len();
    let content_type = header_string(&headers, header::CONTENT_TYPE.as_str());
    let mut response = save_object(
        &state,
        pending.bucket,
        pending.name,
        Bytes::from(pending.bytes),
        pending.metadata,
        content_type,
    )
    .await;
    response
        .headers_mut()
        .insert("x-goog-upload-status", HeaderValue::from_static("final"));
    response.headers_mut().insert(
        "x-goog-upload-size-received",
        HeaderValue::from_str(&size.to_string()).unwrap(),
    );
    response
}

async fn put_signed_object(
    State(state): State<StorageState>,
    Path((bucket, object)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut metadata = metadata_from_headers(&headers);
    metadata.content_type = header_string(&headers, header::CONTENT_TYPE.as_str());
    save_object(&state, bucket, object, body, metadata, None).await
}

async fn save_object(
    state: &StorageState,
    bucket: String,
    name: String,
    bytes: Bytes,
    metadata: UploadMetadata,
    fallback_content_type: Option<String>,
) -> Response {
    if let Some(persistence) = &state.persistence {
        return save_persistent_object(
            persistence.clone(),
            state.blob_lifecycle.clone(),
            bucket,
            name,
            bytes,
            metadata,
            fallback_content_type,
        )
        .await;
    }
    let now = timestamp_now();
    let mut data = state.data.write().await;
    data.next_generation += 1;
    let generation = data.next_generation;
    let existing_created = data
        .objects
        .get(&(bucket.clone(), name.clone()))
        .map(|object| object.created.clone());
    let object = StoredObject {
        bucket: bucket.clone(),
        name: name.clone(),
        size: bytes.len() as u64,
        crc32c: crc32c_base64(&bytes),
        bytes: Some(bytes),
        blob_name: None,
        content_type: metadata
            .content_type
            .or(fallback_content_type)
            .unwrap_or_else(|| "application/octet-stream".to_owned()),
        metadata: metadata.metadata,
        cache_control: metadata.cache_control,
        content_disposition: metadata.content_disposition,
        content_encoding: metadata.content_encoding,
        generation,
        created: existing_created.unwrap_or_else(|| now.clone()),
        updated: now,
        download_token: Uuid::new_v4().to_string(),
    };
    let value = object_json(&object);
    data.objects.insert((bucket, name), object);
    crate::functions::storage_changed("finalize", value.clone());
    Json(value).into_response()
}

async fn save_persistent_object(
    persistence: Persistence,
    blob_lifecycle: Arc<RwLock<()>>,
    bucket: String,
    name: String,
    bytes: Bytes,
    metadata: UploadMetadata,
    fallback_content_type: Option<String>,
) -> Response {
    let blob_name = Uuid::new_v4().to_string();
    let temporary_name = format!("upload-{}.tmp", Uuid::new_v4());
    let temporary_path = persistence.temporary_dir().join(temporary_name);
    let blob_path = persistence.blobs_dir().join(&blob_name);
    let contents = bytes.to_vec();
    let write_path = temporary_path.clone();
    let final_path = blob_path.clone();
    if let Err(error) = tokio::task::spawn_blocking(move || -> Result<(), std::io::Error> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&write_path)?;
        file.write_all(&contents)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&write_path, &final_path)?;
        sync_parent_directory(&final_path)?;
        Ok(())
    })
    .await
    .map_err(|error| PersistenceError::Join(error.to_string()))
    .and_then(|result| result.map_err(PersistenceError::Io))
    {
        let _ = tokio::fs::remove_file(&temporary_path).await;
        return persistence_failure(error);
    }

    let now = timestamp_now();
    let size = bytes.len() as u64;
    let checksum = crc32c_base64(&bytes);
    let content_type = metadata
        .content_type
        .or(fallback_content_type)
        .unwrap_or_else(|| "application/octet-stream".to_owned());
    let custom_metadata = metadata.metadata;
    let cache_control = metadata.cache_control;
    let content_disposition = metadata.content_disposition;
    let content_encoding = metadata.content_encoding;
    let persistence_for_write = persistence.clone();
    let committed_blob_name = blob_name.clone();
    // Readers hold the shared side of this guard from metadata selection
    // through the final byte read. Keep the exclusive side through commit and
    // old-file cleanup so no acknowledged generation can disappear beneath a
    // GET, including on Windows where an open file may reject unlinking.
    let _blob_guard = blob_lifecycle.write().await;
    let result = persistence
        .write(move |connection| {
            let transaction = connection.transaction()?;
            let old = transaction
                .query_row(
                    "SELECT blob_name, created FROM storage_objects WHERE bucket=?1 AND name=?2",
                    rusqlite::params![bucket, name],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()?;
            transaction.execute(
                "UPDATE counters SET value=value+1 WHERE name='storage_generation'",
                [],
            )?;
            let generation: i64 = transaction.query_row(
                "SELECT value FROM counters WHERE name='storage_generation'",
                [],
                |row| row.get(0),
            )?;
            let object = StoredObject {
                bucket: bucket.clone(),
                name: name.clone(),
                bytes: None,
                blob_name: Some(committed_blob_name.clone()),
                size,
                crc32c: checksum,
                content_type,
                metadata: custom_metadata,
                cache_control,
                content_disposition,
                content_encoding,
                generation: generation as u64,
                created: old
                    .as_ref()
                    .map(|(_, created)| created.clone())
                    .unwrap_or_else(|| now.clone()),
                updated: now,
                download_token: Uuid::new_v4().to_string(),
            };
            let public = object_json(&object);
            let encoded = metadata_json(&object)?;
            transaction.execute(
                "INSERT INTO storage_objects(bucket,name,blob_name,metadata_json,generation,created,updated) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7) \
                 ON CONFLICT(bucket,name) DO UPDATE SET blob_name=excluded.blob_name, \
                 metadata_json=excluded.metadata_json,generation=excluded.generation,updated=excluded.updated",
                rusqlite::params![
                    object.bucket,
                    object.name,
                    committed_blob_name,
                    encoded,
                    generation,
                    object.created,
                    object.updated
                ],
            )?;
            persistence_for_write.insert_outbox(
                &transaction,
                &[crate::functions::storage_outbox_record("finalize", public)],
            )?;
            transaction.commit()?;
            Ok((object, old.map(|(blob, _)| blob)))
        })
        .await;
    let (object, old_blob) = match result {
        Ok(value) => value,
        Err(error) => {
            let _ = tokio::fs::remove_file(&blob_path).await;
            return persistence_failure(error);
        }
    };
    crate::functions::outbox_committed();
    if let Some(old_blob) = old_blob.filter(|old| old != &blob_name) {
        if let Ok(path) = blob_path_for_cleanup(&persistence, &old_blob) {
            let _ = tokio::fs::remove_file(path).await;
        }
    }
    Json(object_json(&object)).into_response()
}

#[cfg(unix)]
fn sync_parent_directory(path: &FsPath) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &FsPath) -> Result<(), std::io::Error> {
    // Windows has no stable Rust API for flushing directory metadata. The
    // uniquely named destination avoids replace-in-place semantics there;
    // the blob itself was flushed before rename and startup reconciliation
    // removes any rename that was not followed by a SQLite commit.
    Ok(())
}

fn blob_path_for_cleanup(
    persistence: &Persistence,
    name: &str,
) -> Result<PathBuf, PersistenceError> {
    blob_path(persistence.blobs_dir(), name)
}

async fn load_blob(
    persistence: &Persistence,
    object: &StoredObject,
) -> Result<Bytes, PersistenceError> {
    let name = object
        .blob_name
        .as_deref()
        .ok_or_else(|| PersistenceError::Corrupt("persistent object has no blob id".into()))?;
    let path = blob_path(persistence.blobs_dir(), name)?;
    let metadata = tokio::fs::symlink_metadata(&path).await.map_err(|error| {
        PersistenceError::Corrupt(format!("failed to inspect Storage blob {name}: {error}"))
    })?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(PersistenceError::Corrupt(format!(
            "Storage blob {name} is not a regular file"
        )));
    }
    let bytes = tokio::fs::read(&path).await.map_err(|error| {
        PersistenceError::Corrupt(format!("failed to read Storage blob {name}: {error}"))
    })?;
    if bytes.len() as u64 != object.size || crc32c_base64(&bytes) != object.crc32c {
        return Err(PersistenceError::Corrupt(format!(
            "Storage blob {name} does not match committed metadata"
        )));
    }
    Ok(Bytes::from(bytes))
}

async fn persistent_media(
    state: &StorageState,
    bucket: String,
    name: String,
) -> Result<Option<StoredObject>, PersistenceError> {
    let _blob_guard = state.blob_lifecycle.read().await;
    let Some(mut object) = persistent_object(
        state
            .persistence
            .as_ref()
            .expect("persistent Storage state"),
        bucket,
        name,
    )
    .await?
    else {
        return Ok(None);
    };
    #[cfg(test)]
    if let Some(barrier) = &state.media_read_barrier {
        barrier.wait();
        barrier.wait();
    }
    object.bytes = Some(
        load_blob(
            state
                .persistence
                .as_ref()
                .expect("persistent Storage state"),
            &object,
        )
        .await?,
    );
    Ok(Some(object))
}

async fn delete_persistent_object(
    persistence: Persistence,
    blob_lifecycle: Arc<RwLock<()>>,
    bucket: String,
    name: String,
) -> Response {
    let _blob_guard = blob_lifecycle.write().await;
    let persistence_for_write = persistence.clone();
    let result = persistence
        .write(move |connection| {
            let transaction = connection.transaction()?;
            let row = transaction
                .query_row(
                    "SELECT blob_name, metadata_json, generation, created, updated \
                     FROM storage_objects WHERE bucket=?1 AND name=?2",
                    rusqlite::params![bucket, name],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                        ))
                    },
                )
                .optional()?;
            let Some((blob, metadata, generation, created, updated)) = row else {
                return Ok(None);
            };
            let object = stored_object(
                bucket.clone(),
                name.clone(),
                blob.clone(),
                metadata,
                generation,
                created,
                updated,
            )?;
            transaction.execute(
                "DELETE FROM storage_objects WHERE bucket=?1 AND name=?2",
                rusqlite::params![bucket, name],
            )?;
            persistence_for_write.insert_outbox(
                &transaction,
                &[crate::functions::storage_outbox_record(
                    "delete",
                    object_json(&object),
                )],
            )?;
            transaction.commit()?;
            Ok(Some(blob))
        })
        .await;
    match result {
        Ok(Some(blob)) => {
            crate::functions::outbox_committed();
            if let Ok(path) = blob_path_for_cleanup(&persistence, &blob) {
                let _ = tokio::fs::remove_file(path).await;
            }
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(None) => api_error(StatusCode::NOT_FOUND, "object not found"),
        Err(error) => persistence_failure(error),
    }
}

async fn get_object(
    State(state): State<StorageState>,
    Path((bucket, object)): Path<(String, String)>,
    Query(query): Query<ObjectQuery>,
) -> Response {
    if let Some(persistence) = &state.persistence {
        if query.alt.as_deref() == Some("media") {
            return match persistent_media(&state, bucket, object).await {
                Ok(Some(object)) => media_response(object),
                Ok(None) => api_error(StatusCode::NOT_FOUND, "object not found"),
                Err(error) => persistence_failure(error),
            };
        }
        return match persistent_object(persistence, bucket, object).await {
            Ok(Some(object)) => Json(object_json(&object)).into_response(),
            Ok(None) => api_error(StatusCode::NOT_FOUND, "object not found"),
            Err(error) => persistence_failure(error),
        };
    }
    let stored = state
        .data
        .read()
        .await
        .objects
        .get(&(bucket, object))
        .cloned();
    match stored {
        Some(object) if query.alt.as_deref() == Some("media") => media_response(object),
        Some(object) => Json(object_json(&object)).into_response(),
        None => api_error(StatusCode::NOT_FOUND, "object not found"),
    }
}

async fn get_signed_object(
    State(state): State<StorageState>,
    Path((bucket, object)): Path<(String, String)>,
) -> Response {
    if state.persistence.is_some() {
        return match persistent_media(&state, bucket, object).await {
            Ok(Some(object)) => media_response(object),
            Ok(None) => api_error(StatusCode::NOT_FOUND, "object not found"),
            Err(error) => persistence_failure(error),
        };
    }
    let stored = state
        .data
        .read()
        .await
        .objects
        .get(&(bucket, object))
        .cloned();
    stored
        .map(media_response)
        .unwrap_or_else(|| api_error(StatusCode::NOT_FOUND, "object not found"))
}

fn media_response(object: StoredObject) -> Response {
    let mut response = object.bytes.unwrap_or_default().into_response();
    let headers = response.headers_mut();
    headers.insert(
        HeaderName::from_static("x-goog-hash"),
        HeaderValue::from_str(&format!("crc32c={}", object.crc32c))
            .expect("base64 checksum is a valid header value"),
    );
    headers.insert(
        HeaderName::from_static("x-goog-stored-content-encoding"),
        HeaderValue::from_static("identity"),
    );

    if let Ok(value) = HeaderValue::from_str(&object.content_type) {
        headers.insert(header::CONTENT_TYPE, value);
    }
    if let Some(value) = object.cache_control.and_then(header_value) {
        headers.insert(header::CACHE_CONTROL, value);
    }
    if let Some(value) = object.content_disposition.and_then(header_value) {
        headers.insert(header::CONTENT_DISPOSITION, value);
    }
    if let Some(value) = object.content_encoding.and_then(header_value) {
        headers.insert(header::CONTENT_ENCODING, value);
    }
    headers.insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{}\"", object.generation))
            .expect("numeric ETag is a valid header"),
    );
    response
}

async fn delete_object(
    State(state): State<StorageState>,
    Path((bucket, object)): Path<(String, String)>,
) -> Response {
    if let Some(persistence) = &state.persistence {
        return delete_persistent_object(
            persistence.clone(),
            state.blob_lifecycle.clone(),
            bucket,
            object,
        )
        .await;
    }
    let removed = state.data.write().await.objects.remove(&(bucket, object));
    if let Some(object) = removed {
        crate::functions::storage_changed("delete", object_json(&object));
        StatusCode::NO_CONTENT.into_response()
    } else {
        api_error(StatusCode::NOT_FOUND, "object not found")
    }
}

async fn list_objects(
    State(state): State<StorageState>,
    Path(bucket): Path<String>,
    Query(query): Query<ListQuery>,
) -> Response {
    let prefix = query.prefix.unwrap_or_default();
    let delimiter = query.delimiter.filter(|value| !value.is_empty());
    if let Some(persistence) = &state.persistence {
        let bucket_for_read = bucket.clone();
        let prefix_for_read = prefix.clone();
        let matching = persistence
            .read(move |connection| {
                let mut statement = connection.prepare(
                    "SELECT name, blob_name, metadata_json, generation, created, updated \
                     FROM storage_objects \
                     WHERE bucket=?1 AND substr(name,1,length(?2))=?2 ORDER BY name",
                )?;
                let mut rows =
                    statement.query(rusqlite::params![bucket_for_read, prefix_for_read])?;
                let mut objects = Vec::new();
                while let Some(row) = rows.next()? {
                    objects.push(stored_object(
                        bucket_for_read.clone(),
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    )?);
                }
                Ok(objects)
            })
            .await;
        return match matching {
            Ok(matching) => list_response(
                matching,
                prefix,
                delimiter,
                query.page_token,
                query.max_results,
            ),
            Err(error) => persistence_failure(error),
        };
    }
    let data = state.data.read().await;
    let mut matching: Vec<_> = data
        .objects
        .values()
        .filter(|object| object.bucket == bucket && object.name.starts_with(&prefix))
        .cloned()
        .collect();
    matching.sort_by(|left, right| left.name.cmp(&right.name));

    list_response(
        matching,
        prefix,
        delimiter,
        query.page_token,
        query.max_results,
    )
}

fn list_response(
    matching: Vec<StoredObject>,
    prefix: String,
    delimiter: Option<String>,
    page_token: Option<String>,
    max_results: Option<usize>,
) -> Response {
    let mut prefixes = BTreeSet::new();
    let mut items = Vec::new();
    for object in matching {
        if let Some(delimiter) = delimiter.as_deref() {
            let remainder = &object.name[prefix.len()..];
            if let Some(index) = remainder.find(delimiter) {
                let end = prefix.len() + index + delimiter.len();
                prefixes.insert(object.name[..end].to_owned());
                continue;
            }
        }
        items.push(object_json(&object));
    }

    let offset = match page_token {
        Some(token) => match BASE64
            .decode(token)
            .ok()
            .and_then(|v| String::from_utf8(v).ok())
            .and_then(|v| v.parse::<usize>().ok())
        {
            Some(offset) => offset,
            None => return api_error(StatusCode::BAD_REQUEST, "invalid page token"),
        },
        None => 0,
    };
    let limit = max_results.unwrap_or(1000).clamp(1, 1000);
    let mut entries: Vec<(String, Option<Value>)> = items
        .into_iter()
        .map(|item| (item["name"].as_str().unwrap().to_owned(), Some(item)))
        .collect();
    entries.extend(prefixes.into_iter().map(|prefix| (prefix, None)));
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let has_more = offset.saturating_add(limit) < entries.len();
    let mut items = Vec::new();
    let mut prefixes = Vec::new();
    for (name, item) in entries.into_iter().skip(offset).take(limit) {
        match item {
            Some(item) => items.push(item),
            None => prefixes.push(name),
        }
    }
    let mut result = json!({"kind":"storage#objects", "items":items, "prefixes":prefixes});
    if has_more {
        result["nextPageToken"] = json!(BASE64.encode((offset + limit).to_string()));
    }
    Json(result).into_response()
}

fn object_json(object: &StoredObject) -> Value {
    let encoded_bucket = percent_encode(&object.bucket);
    let encoded_name = percent_encode(&object.name);
    let mut value = json!({
        "kind": "storage#object",
        "id": format!("{}/{}/{}", object.bucket, object.name, object.generation),
        "selfLink": format!("/storage/v1/b/{encoded_bucket}/o/{encoded_name}"),
        "mediaLink": format!("/storage/v1/b/{encoded_bucket}/o/{encoded_name}?alt=media"),
        "name": object.name,
        "bucket": object.bucket,
        "generation": object.generation.to_string(),
        "metageneration": "1",
        "contentType": object.content_type,
        "size": object.size.to_string(),
        "crc32c": object.crc32c,
        "timeCreated": object.created,
        "updated": object.updated,
        "etag": format!("\"{}\"", object.generation),
        "metadata": object.metadata,
        "downloadTokens": object.download_token.to_string(),
    });
    let map = value.as_object_mut().expect("object metadata is a map");
    if let Some(field) = &object.cache_control {
        map.insert("cacheControl".to_owned(), json!(field));
    }
    if let Some(field) = &object.content_disposition {
        map.insert("contentDisposition".to_owned(), json!(field));
    }
    if let Some(field) = &object.content_encoding {
        map.insert("contentEncoding".to_owned(), json!(field));
    }
    value
}

fn metadata_from_headers(headers: &HeaderMap) -> UploadMetadata {
    let mut result = UploadMetadata {
        cache_control: header_string(headers, header::CACHE_CONTROL.as_str()),
        content_disposition: header_string(headers, header::CONTENT_DISPOSITION.as_str()),
        content_encoding: header_string(headers, header::CONTENT_ENCODING.as_str()),
        ..UploadMetadata::default()
    };
    for (name, value) in headers {
        if let Some(key) = name.as_str().strip_prefix("x-goog-meta-") {
            if let Ok(value) = value.to_str() {
                result.metadata.insert(key.to_owned(), value.to_owned());
            }
        }
    }
    result
}

fn parse_multipart(
    content_type: &str,
    body: &[u8],
) -> Result<(UploadMetadata, Bytes, Option<String>), &'static str> {
    let boundary = content_type
        .split(';')
        .map(str::trim)
        .find_map(|part| part.strip_prefix("boundary="))
        .map(|value| value.trim_matches('"'))
        .filter(|value| !value.is_empty())
        .ok_or("multipart boundary is missing")?;
    let marker = format!("--{boundary}").into_bytes();
    let mut cursor = find_bytes(body, &marker).ok_or("multipart boundary not found")?;
    let mut parts: Vec<(String, Bytes)> = Vec::new();

    loop {
        cursor += marker.len();
        if body.get(cursor..cursor + 2) == Some(b"--") {
            break;
        }
        if body.get(cursor..cursor + 2) == Some(b"\r\n") {
            cursor += 2;
        }
        let header_end_rel = find_bytes(&body[cursor..], b"\r\n\r\n")
            .ok_or("multipart part headers are incomplete")?;
        let header_end = cursor + header_end_rel;
        let headers = std::str::from_utf8(&body[cursor..header_end])
            .map_err(|_| "multipart part headers are not UTF-8")?
            .to_owned();
        let content_start = header_end + 4;
        let mut next_marker = Vec::with_capacity(marker.len() + 2);
        next_marker.extend_from_slice(b"\r\n");
        next_marker.extend_from_slice(&marker);
        let content_end_rel = find_bytes(&body[content_start..], &next_marker)
            .ok_or("multipart closing boundary is missing")?;
        let content_end = content_start + content_end_rel;
        parts.push((
            headers,
            Bytes::copy_from_slice(&body[content_start..content_end]),
        ));
        cursor = content_end + 2;
    }

    if parts.len() < 2 {
        return Err("multipart upload requires metadata and media parts");
    }
    let metadata = serde_json::from_slice::<UploadMetadata>(&parts[0].1)
        .map_err(|_| "multipart metadata is invalid JSON")?;
    let media_type = part_content_type(&parts[1].0);
    Ok((metadata, parts.remove(1).1, media_type))
}

fn part_content_type(headers: &str) -> Option<String> {
    headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("content-type")
            .then(|| value.trim().to_owned())
    })
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn header_value(value: String) -> Option<HeaderValue> {
    HeaderValue::from_str(&value).ok()
}

fn api_error(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(json!({
            "error": {
                "code": status.as_u16(),
                "message": message,
                "errors": [{ "message": message }],
            }
        })),
    )
        .into_response()
}

fn percent_encode(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            output.push(byte as char);
        } else {
            output.push_str(&format!("%{byte:02X}"));
        }
    }
    output
}

fn timestamp_now() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    timestamp_from_unix(seconds)
}

// Gregorian conversion adapted from Howard Hinnant's public-domain civil date
// algorithm. Keeping this here avoids adding a date/time dependency.
fn timestamp_from_unix(seconds: i64) -> String {
    let days = seconds.div_euclid(86_400);
    let day_seconds = seconds.rem_euclid(86_400);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    let hour = day_seconds / 3_600;
    let minute = day_seconds % 3_600 / 60;
    let second = day_seconds % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.000Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temporary(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("firebase-emu-storage-{name}-{}", Uuid::new_v4()))
    }

    async fn persistent_state(root: &FsPath) -> (Persistence, StorageState) {
        let persistence = Persistence::open(root.to_owned(), false).await.unwrap();
        recover_blobs(&persistence).await.unwrap();
        let state = StorageState {
            persistence: Some(persistence.clone()),
            ..StorageState::default()
        };
        (persistence, state)
    }

    #[test]
    fn parses_binary_multipart_upload() {
        let body = b"--edge\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{\"name\":\"folder/a.bin\",\"metadata\":{\"owner\":\"test\"}}\r\n--edge\r\nContent-Type: application/octet-stream\r\n\r\nabc\x00\xffxyz\r\n--edge--\r\n";
        let (metadata, media, content_type) =
            parse_multipart("multipart/related; boundary=edge", body).unwrap();
        assert_eq!(metadata.name.as_deref(), Some("folder/a.bin"));
        assert_eq!(
            metadata.metadata.get("owner").map(String::as_str),
            Some("test")
        );
        assert_eq!(media.as_ref(), b"abc\x00\xffxyz");
        assert_eq!(content_type.as_deref(), Some("application/octet-stream"));
    }

    #[test]
    fn formats_unix_epoch_as_rfc3339() {
        assert_eq!(timestamp_from_unix(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            timestamp_from_unix(1_704_067_200),
            "2024-01-01T00:00:00.000Z"
        );
    }

    #[test]
    fn percent_encodes_object_names_for_json_api_links() {
        assert_eq!(percent_encode("folder/a b.txt"), "folder%2Fa%20b.txt");
    }

    #[test]
    fn emits_gcs_crc32c_checksum_format() {
        assert_eq!(crc32c_base64(b"hello emulator"), "LIKQWQ==");
    }

    #[tokio::test]
    async fn store_overwrite_preserves_creation_time_and_delete_removes_object() {
        let state = StorageState::default();
        let _ = save_object(
            &state,
            "bucket".to_owned(),
            "file.txt".to_owned(),
            Bytes::from_static(b"one"),
            UploadMetadata::default(),
            Some("text/plain".to_owned()),
        )
        .await;
        let first = state
            .data
            .read()
            .await
            .objects
            .get(&("bucket".to_owned(), "file.txt".to_owned()))
            .unwrap()
            .clone();
        let _ = save_object(
            &state,
            "bucket".to_owned(),
            "file.txt".to_owned(),
            Bytes::from_static(b"two"),
            UploadMetadata::default(),
            None,
        )
        .await;
        let second = state
            .data
            .read()
            .await
            .objects
            .get(&("bucket".to_owned(), "file.txt".to_owned()))
            .unwrap()
            .clone();
        assert_eq!(second.bytes.as_deref(), Some(b"two".as_slice()));
        assert!(second.generation > first.generation);
        assert_eq!(second.created, first.created);
        assert_eq!(
            delete_object(
                State(state.clone()),
                Path(("bucket".to_owned(), "file.txt".to_owned()))
            )
            .await
            .status(),
            StatusCode::NO_CONTENT
        );
        assert!(state.data.read().await.objects.is_empty());
    }

    #[tokio::test]
    async fn delimiter_listing_separates_items_and_prefixes() {
        let state = StorageState::default();
        for name in ["logs/a.txt", "logs/2024/jan.txt", "logs/2025/feb.txt"] {
            let _ = save_object(
                &state,
                "bucket".to_owned(),
                name.to_owned(),
                Bytes::from_static(b"data"),
                UploadMetadata::default(),
                None,
            )
            .await;
        }
        let response = list_objects(
            State(state),
            Path("bucket".to_owned()),
            Query(ListQuery {
                prefix: Some("logs/".to_owned()),
                delimiter: Some("/".to_owned()),
                max_results: None,
                page_token: None,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        // The core grouping calculation is also checked directly so this test
        // does not need an extra HTTP-body collection dev dependency.
        let names = ["logs/a.txt", "logs/2024/jan.txt", "logs/2025/feb.txt"];
        let prefixes: BTreeSet<_> = names
            .iter()
            .filter_map(|name| {
                let remainder = &name["logs/".len()..];
                remainder
                    .find('/')
                    .map(|index| name[.."logs/".len() + index + 1].to_owned())
            })
            .collect();
        assert_eq!(
            prefixes,
            BTreeSet::from(["logs/2024/".to_owned(), "logs/2025/".to_owned()])
        );
    }

    #[tokio::test]
    async fn persistent_overwrite_delete_and_reopen_are_consistent() {
        let root = temporary("reopen");
        let (first_store, state) = persistent_state(&root).await;
        assert_eq!(
            save_object(
                &state,
                "bucket".into(),
                "folder/file.txt".into(),
                Bytes::from_static(b"first"),
                UploadMetadata {
                    metadata: HashMap::from([("owner".into(), "test".into())]),
                    ..UploadMetadata::default()
                },
                Some("text/plain".into()),
            )
            .await
            .status(),
            StatusCode::OK
        );
        let first = persistent_object(&first_store, "bucket".into(), "folder/file.txt".into())
            .await
            .unwrap()
            .unwrap();
        let first_blob = first.blob_name.clone().unwrap();
        let created = first.created.clone();
        assert_eq!(
            load_blob(&first_store, &first).await.unwrap(),
            b"first".as_slice()
        );

        assert_eq!(
            save_object(
                &state,
                "bucket".into(),
                "folder/file.txt".into(),
                Bytes::from_static(b"second"),
                UploadMetadata::default(),
                None,
            )
            .await
            .status(),
            StatusCode::OK
        );
        let second = persistent_object(&first_store, "bucket".into(), "folder/file.txt".into())
            .await
            .unwrap()
            .unwrap();
        assert!(second.generation > first.generation);
        assert_eq!(second.created, created);
        assert_eq!(
            load_blob(&first_store, &second).await.unwrap(),
            b"second".as_slice()
        );
        assert!(!first_store.blobs_dir().join(first_blob).exists());

        drop(state);
        drop(first_store);
        let (second_store, reopened) = persistent_state(&root).await;
        let object = persistent_object(&second_store, "bucket".into(), "folder/file.txt".into())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            load_blob(&second_store, &object).await.unwrap(),
            b"second".as_slice()
        );
        assert_eq!(
            delete_object(
                State(reopened.clone()),
                Path(("bucket".into(), "folder/file.txt".into()))
            )
            .await
            .status(),
            StatusCode::NO_CONTENT
        );
        assert!(
            persistent_object(&second_store, "bucket".into(), "folder/file.txt".into())
                .await
                .unwrap()
                .is_none()
        );
        assert!(fs::read_dir(second_store.blobs_dir())
            .unwrap()
            .next()
            .is_none());
        drop(reopened);
        drop(second_store);
        let (third_store, third_state) = persistent_state(&root).await;
        assert!(
            persistent_object(&third_store, "bucket".into(), "folder/file.txt".into())
                .await
                .unwrap()
                .is_none()
        );
        drop(third_state);
        drop(third_store);
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn persistent_media_reads_hold_blob_through_overwrite_and_delete() {
        let root = temporary("media-lifetime");
        let (persistence, mut state) = persistent_state(&root).await;
        assert_eq!(
            save_object(
                &state,
                "bucket".into(),
                "file.bin".into(),
                Bytes::from_static(b"first"),
                UploadMetadata::default(),
                None,
            )
            .await
            .status(),
            StatusCode::OK
        );

        let overwrite_barrier = Arc::new(std::sync::Barrier::new(2));
        state.media_read_barrier = Some(overwrite_barrier.clone());
        let rest_get = tokio::spawn(get_object(
            State(state.clone()),
            Path(("bucket".into(), "file.bin".into())),
            Query(ObjectQuery {
                alt: Some("media".into()),
            }),
        ));
        tokio::task::spawn_blocking({
            let barrier = overwrite_barrier.clone();
            move || barrier.wait()
        })
        .await
        .unwrap();
        let overwrite = {
            let state = state.clone();
            tokio::spawn(async move {
                save_object(
                    &state,
                    "bucket".into(),
                    "file.bin".into(),
                    Bytes::from_static(b"second"),
                    UploadMetadata::default(),
                    None,
                )
                .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!overwrite.is_finished());
        tokio::task::spawn_blocking(move || overwrite_barrier.wait())
            .await
            .unwrap();
        let response = rest_get.await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
            b"first".as_slice()
        );
        assert_eq!(overwrite.await.unwrap().status(), StatusCode::OK);

        let delete_barrier = Arc::new(std::sync::Barrier::new(2));
        state.media_read_barrier = Some(delete_barrier.clone());
        let signed_get = tokio::spawn(get_signed_object(
            State(state.clone()),
            Path(("bucket".into(), "file.bin".into())),
        ));
        tokio::task::spawn_blocking({
            let barrier = delete_barrier.clone();
            move || barrier.wait()
        })
        .await
        .unwrap();
        let delete = tokio::spawn(delete_object(
            State(state.clone()),
            Path(("bucket".into(), "file.bin".into())),
        ));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!delete.is_finished());
        tokio::task::spawn_blocking(move || delete_barrier.wait())
            .await
            .unwrap();
        let response = signed_get.await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
            b"second".as_slice()
        );
        assert_eq!(delete.await.unwrap().status(), StatusCode::NO_CONTENT);

        drop(state);
        drop(persistence);
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn startup_discards_temporary_and_orphan_blob_files() {
        let root = temporary("orphans");
        let persistence = Persistence::open(root.clone(), false).await.unwrap();
        let orphan = persistence.blobs_dir().join(Uuid::new_v4().to_string());
        let temporary = persistence.temporary_dir().join("incomplete-upload.tmp");
        fs::write(&orphan, b"orphan").unwrap();
        fs::write(&temporary, b"partial").unwrap();
        recover_blobs(&persistence).await.unwrap();
        assert!(!orphan.exists());
        assert!(!temporary.exists());
        drop(persistence);
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn startup_rejects_missing_or_unsafe_referenced_blob() {
        for blob in [Uuid::new_v4().to_string(), "../outside".to_owned()] {
            let root = temporary("unsafe-reference");
            let persistence = Persistence::open(root.clone(), false).await.unwrap();
            let metadata = serde_json::to_string(&StoredMetadata {
                content_type: "application/octet-stream".into(),
                metadata: HashMap::new(),
                cache_control: None,
                content_disposition: None,
                content_encoding: None,
                download_token: Uuid::new_v4().to_string(),
                size: 1,
                crc32c: crc32c_base64(b"x"),
            })
            .unwrap();
            let blob_for_write = blob.clone();
            persistence
                .write(move |connection| {
                    connection.execute(
                        "INSERT INTO storage_objects(bucket,name,blob_name,metadata_json,generation,created,updated) \
                         VALUES ('bucket','object',?1,?2,1,'now','now')",
                        rusqlite::params![blob_for_write, metadata],
                    )?;
                    Ok(())
                })
                .await
                .unwrap();
            let error = recover_blobs(&persistence).await.unwrap_err().to_string();
            if blob.starts_with("..") {
                assert!(error.contains("invalid Storage blob id"), "{error}");
            } else {
                assert!(error.contains("missing blob"), "{error}");
            }
            assert!(!root.join("outside").exists());
            drop(persistence);
            let _ = fs::remove_dir_all(root);
        }
    }

    #[tokio::test]
    async fn persistent_mutations_commit_storage_outbox_envelopes() {
        let root = temporary("outbox");
        let persistence = Persistence::open(root.clone(), true).await.unwrap();
        let state = StorageState {
            persistence: Some(persistence.clone()),
            ..StorageState::default()
        };
        assert_eq!(
            save_object(
                &state,
                "bucket".into(),
                "event.txt".into(),
                Bytes::from_static(b"event"),
                UploadMetadata::default(),
                None,
            )
            .await
            .status(),
            StatusCode::OK
        );
        assert_eq!(
            delete_object(
                State(state.clone()),
                Path(("bucket".into(), "event.txt".into()))
            )
            .await
            .status(),
            StatusCode::NO_CONTENT
        );
        let rows: Vec<(String, Value)> = persistence
            .read(|connection| {
                let mut statement =
                    connection.prepare("SELECT source,payload FROM event_outbox ORDER BY id")?;
                let mut rows = statement.query([])?;
                let mut result = Vec::new();
                while let Some(row) = rows.next()? {
                    let source: String = row.get(0)?;
                    let payload: Vec<u8> = row.get(1)?;
                    let payload = serde_json::from_slice(&payload).map_err(|error| {
                        PersistenceError::Corrupt(format!("invalid test outbox JSON: {error}"))
                    })?;
                    result.push((source, payload));
                }
                Ok(result)
            })
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "storage");
        assert_eq!(rows[0].1["kind"], "finalize");
        assert_eq!(rows[0].1["object"]["name"], "event.txt");
        assert_eq!(rows[1].0, "storage");
        assert_eq!(rows[1].1["kind"], "delete");
        assert_eq!(rows[1].1["object"]["name"], "event.txt");
        drop(state);
        drop(persistence);
        let _ = fs::remove_dir_all(root);
    }
}

#[cfg(test)]
mod protocol_tests {
    use super::*;
    #[tokio::test]
    async fn browser_multipart_is_visible_to_gcs_and_listing_pages() {
        let state = StorageState::default();
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-goog-upload-protocol",
            HeaderValue::from_static("multipart"),
        );
        headers.insert(
            "content-type",
            HeaderValue::from_static("multipart/related; boundary=edge"),
        );
        let body = Bytes::from_static(b"--edge\r\nContent-Type: application/json\r\n\r\n{\"name\":\"folder/a.bin\"}\r\n--edge\r\nContent-Type: application/octet-stream\r\n\r\n\x00\xff\r\n--edge--\r\n");
        let response = upload_object(
            State(state.clone()),
            Path("bucket".into()),
            Query(UploadQuery::default()),
            headers,
            body,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let response = get_object(
            State(state.clone()),
            Path(("bucket".into(), "folder/a.bin".into())),
            Query(ObjectQuery {
                alt: Some("media".into()),
            }),
        )
        .await;
        assert_eq!(
            response.headers()["x-goog-hash"],
            format!("crc32c={}", crc32c_base64(&[0, 255]))
        );
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 100)
                .await
                .unwrap()
                .as_ref(),
            &[0, 255]
        );
        let _ = save_object(
            &state,
            "bucket".into(),
            "folder/b.bin".into(),
            Bytes::new(),
            UploadMetadata::default(),
            None,
        )
        .await;
        let first = list_objects(
            State(state.clone()),
            Path("bucket".into()),
            Query(ListQuery {
                max_results: Some(1),
                ..Default::default()
            }),
        )
        .await;
        let first: Value = serde_json::from_slice(
            &axum::body::to_bytes(first.into_body(), 10000)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(first["items"][0]["name"], "folder/a.bin");
        let second = list_objects(
            State(state),
            Path("bucket".into()),
            Query(ListQuery {
                max_results: Some(1),
                page_token: Some(first["nextPageToken"].as_str().unwrap().into()),
                ..Default::default()
            }),
        )
        .await;
        let second: Value = serde_json::from_slice(
            &axum::body::to_bytes(second.into_body(), 10000)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(second["items"][0]["name"], "folder/b.bin");
        assert!(second.get("nextPageToken").is_none());
    }
    #[tokio::test]
    async fn resumable_checks_offsets_and_keeps_all_chunks() {
        let state = StorageState::default();
        let id = Uuid::new_v4();
        state.data.write().await.uploads.insert(
            id,
            PendingUpload {
                bucket: "bucket".into(),
                name: "file".into(),
                metadata: UploadMetadata::default(),
                bytes: Vec::new(),
                created_at: unix_now(),
            },
        );
        for (offset, command, body, status) in [
            (0, "upload", "one", StatusCode::OK),
            (0, "upload", "bad", StatusCode::BAD_REQUEST),
            (3, "upload, finalize", "two", StatusCode::OK),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-goog-upload-offset",
                HeaderValue::from_str(&offset.to_string()).unwrap(),
            );
            headers.insert(
                "x-goog-upload-command",
                HeaderValue::from_str(command).unwrap(),
            );
            let response = resumable_put(
                State(state.clone()),
                Path("bucket".into()),
                Query(UploadQuery {
                    upload_id: Some(id.to_string()),
                    ..Default::default()
                }),
                headers,
                Bytes::from(body),
            )
            .await;
            assert_eq!(response.status(), status);
        }
        assert_eq!(
            state.data.read().await.objects[&("bucket".into(), "file".into())]
                .bytes
                .as_deref(),
            Some(b"onetwo".as_slice())
        );
    }

    #[test]
    fn abandoned_resumable_uploads_expire_opportunistically() {
        let mut data = StorageData::default();
        let expired = Uuid::new_v4();
        let active = Uuid::new_v4();
        let upload = |created_at| PendingUpload {
            bucket: "bucket".into(),
            name: "file".into(),
            metadata: UploadMetadata::default(),
            bytes: Vec::new(),
            created_at,
        };
        data.uploads.insert(expired, upload(1));
        data.uploads
            .insert(active, upload(RESUMABLE_UPLOAD_TTL_SECS + 1));
        prune_expired_uploads(&mut data, RESUMABLE_UPLOAD_TTL_SECS + 2);
        assert!(!data.uploads.contains_key(&expired));
        assert!(data.uploads.contains_key(&active));
    }
}
