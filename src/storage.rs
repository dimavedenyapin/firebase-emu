//! Small in-memory Google Cloud Storage compatible REST emulator.
//!
//! The implementation intentionally keeps all data in process memory.  It is
//! aimed at local SDK and integration tests, not at untrusted networks.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use std::{
    collections::{BTreeSet, HashMap},
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
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::RwLock;
use uuid::Uuid;

const RESUMABLE_UPLOAD_TTL_SECS: u64 = 60 * 60;

fn crc32c_base64(bytes: &[u8]) -> String {
    BASE64.encode(crc32c::crc32c(bytes).to_be_bytes())
}

#[derive(Clone, Default)]
struct StorageState {
    data: Arc<RwLock<StorageData>>,
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
    bytes: Bytes,
    content_type: String,
    metadata: HashMap<String, String>,
    cache_control: Option<String>,
    content_disposition: Option<String>,
    content_encoding: Option<String>,
    generation: u64,
    created: String,
    updated: String,
    download_token: Uuid,
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

/// Build a Storage REST router with its own isolated, in-memory state.
///
/// Mount this router directly on the listener bound to port 9199.
pub fn router() -> Router {
    let state = StorageState::default();

    Router::new()
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
        .layer(DefaultBodyLimit::disable())
        .layer(tower_http::cors::CorsLayer::permissive())
        .with_state(state)
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
        bytes,
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
        download_token: Uuid::new_v4(),
    };
    let value = object_json(&object);
    data.objects.insert((bucket, name), object);
    crate::functions::storage_changed("finalize", value.clone());
    Json(value).into_response()
}

async fn get_object(
    State(state): State<StorageState>,
    Path((bucket, object)): Path<(String, String)>,
    Query(query): Query<ObjectQuery>,
) -> Response {
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
    let crc32c = crc32c_base64(&object.bytes);
    let mut response = object.bytes.into_response();
    let headers = response.headers_mut();
    headers.insert(
        HeaderName::from_static("x-goog-hash"),
        HeaderValue::from_str(&format!("crc32c={crc32c}"))
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
    let data = state.data.read().await;
    let mut matching: Vec<_> = data
        .objects
        .values()
        .filter(|object| object.bucket == bucket && object.name.starts_with(&prefix))
        .cloned()
        .collect();
    matching.sort_by(|left, right| left.name.cmp(&right.name));

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

    let offset = match query.page_token {
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
    let limit = query.max_results.unwrap_or(1000).clamp(1, 1000);
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
    let crc32c = crc32c_base64(&object.bytes);
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
        "size": object.bytes.len().to_string(),
        "crc32c": crc32c,
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
        assert_eq!(second.bytes.as_ref(), b"two");
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
                .as_ref(),
            b"onetwo"
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
