//! Firebase browser WebChannel and REST endpoints over the shared Firestore store.
use crate::firestore::{
    google::firestore::v1::{self as pb, firestore_server::Firestore},
    FirestoreService,
};
use axum07::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value as Json};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, Notify};
use tonic::{Request, Status};

#[derive(Clone)]
struct WebState {
    core: FirestoreService,
    sessions: Arc<Mutex<HashMap<String, Arc<Channel>>>>,
}
struct Channel {
    inner: Mutex<Session>,
    ready: Notify,
}
struct Session {
    seq: u64,
    messages: BTreeMap<u64, Json>,
    targets: BTreeMap<i32, Target>,
    received: std::collections::BTreeSet<u64>,
    touched: Instant,
    write_ready: bool,
    database: String,
}
struct Target {
    spec: Json,
    documents: BTreeMap<String, pb::Document>,
}

pub(crate) fn router(core: FirestoreService) -> Router {
    Router::new()
        .route("/google.firestore.v1.Firestore/:rpc/channel", any(channel))
        .route("/v1/*resource", any(rest))
        .with_state(WebState {
            core,
            sessions: Default::default(),
        })
}

fn frame(value: Json) -> String {
    let body = value.to_string();
    format!("{}\n{}", body.encode_utf16().count(), body)
}
fn response(
    status: StatusCode,
    body: String,
    request: &HeaderMap,
    content_type: &'static str,
) -> Response {
    let mut response = (status, body).into_response();
    let h = response.headers_mut();
    h.insert("content-type", HeaderValue::from_static(content_type));
    h.insert(
        "cache-control",
        HeaderValue::from_static("no-cache, no-store"),
    );
    h.insert(
        "access-control-allow-origin",
        request
            .get("origin")
            .cloned()
            .unwrap_or(HeaderValue::from_static("*")),
    );
    h.insert(
        "access-control-allow-credentials",
        HeaderValue::from_static("true"),
    );
    h.insert(
        "access-control-allow-methods",
        HeaderValue::from_static("GET, POST, OPTIONS"),
    );
    h.insert(
        "access-control-allow-headers",
        request
            .get("access-control-request-headers")
            .cloned()
            .unwrap_or(HeaderValue::from_static(
                "content-type, authorization, x-goog-api-client",
            )),
    );
    h.insert(
        "access-control-expose-headers",
        HeaderValue::from_static("X-HTTP-Session-Id"),
    );
    h.insert("vary", HeaderValue::from_static("Origin"));
    response
}
fn now() -> prost_types::Timestamp {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    prost_types::Timestamp {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    }
}
fn time_json(t: Option<&prost_types::Timestamp>) -> Json {
    t.map(|t| json!(t.to_string())).unwrap_or(Json::Null)
}
fn string(v: &Json, key: &str) -> String {
    v[key].as_str().unwrap_or_default().to_owned()
}
fn array<'a>(v: &'a Json, key: &str) -> &'a [Json] {
    v[key].as_array().map(Vec::as_slice).unwrap_or(&[])
}
fn invalid(message: &str) -> Status {
    Status::invalid_argument(message)
}
fn error_json(error: &Status) -> Json {
    let status = match error.code() {
        tonic::Code::NotFound => "NOT_FOUND",
        tonic::Code::AlreadyExists => "ALREADY_EXISTS",
        tonic::Code::FailedPrecondition => "FAILED_PRECONDITION",
        tonic::Code::Unimplemented => "UNIMPLEMENTED",
        tonic::Code::PermissionDenied => "PERMISSION_DENIED",
        tonic::Code::InvalidArgument => "INVALID_ARGUMENT",
        _ => "INTERNAL",
    };
    json!({"error":{"code":error.code() as i32,"status":status,"message":error.message()}})
}
fn decode_form_component(s: &str) -> Result<String, Status> {
    let mut out = Vec::new();
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => {
                out.push(
                    u8::from_str_radix(
                        std::str::from_utf8(&b[i + 1..i + 3])
                            .map_err(|_| invalid("invalid form escape"))?,
                        16,
                    )
                    .map_err(|_| invalid("invalid form escape"))?,
                );
                i += 2;
            }
            b'%' => return Err(invalid("invalid form escape")),
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8(out).map_err(|_| invalid("invalid UTF-8 form"))
}
fn form_messages(body: &[u8]) -> Result<Vec<(u64, Json)>, Status> {
    let body = std::str::from_utf8(body).map_err(|_| invalid("invalid form"))?;
    let mut fields = BTreeMap::new();
    for pair in body.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            fields.insert(decode_form_component(k)?, decode_form_component(v)?);
        }
    }
    let offset = fields
        .get("ofs")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let mut messages = Vec::new();
    for (key, value) in fields {
        if let Some(index) = key
            .strip_prefix("req")
            .and_then(|k| k.strip_suffix("___data__"))
        {
            let index = index
                .parse::<u64>()
                .map_err(|_| invalid("invalid request index"))?;
            messages.push((
                offset + index,
                serde_json::from_str(&value).map_err(|_| invalid("invalid request JSON"))?,
            ));
        }
    }
    messages.sort_by_key(|(id, _)| *id);
    Ok(messages)
}
impl Session {
    fn push(&mut self, value: Json) {
        self.seq += 1;
        self.messages.insert(self.seq, json!([self.seq, [value]]));
    }
    fn queued(&mut self, aid: u64) -> Option<String> {
        self.messages.retain(|seq, _| *seq > aid);
        if self.messages.is_empty() {
            None
        } else {
            Some(frame(Json::Array(
                self.messages.values().cloned().collect(),
            )))
        }
    }
}
async fn channel(
    State(state): State<WebState>,
    Path(rpc): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let result = channel_inner(&state, &rpc, &params, &method, &body).await;
    match result {
        Ok(body) => response(StatusCode::OK, body, &headers, "text/plain; charset=utf-8"),
        Err(e) => response(
            StatusCode::BAD_REQUEST,
            error_json(&e).to_string(),
            &headers,
            "application/json",
        ),
    }
}
async fn channel_inner(
    state: &WebState,
    rpc: &str,
    params: &HashMap<String, String>,
    method: &Method,
    body: &[u8],
) -> Result<String, Status> {
    if method == Method::OPTIONS {
        return Ok(String::new());
    }
    if rpc != "Listen" && rpc != "Write" {
        return Err(Status::unimplemented("unknown stream"));
    }
    if params.get("TYPE").is_some_and(|t| t == "terminate") {
        if let Some(sid) = params.get("SID") {
            state.sessions.lock().await.remove(sid);
        }
        return Ok(String::new());
    }
    let initial = !params.contains_key("SID");
    if initial && method != Method::POST {
        return Err(invalid("POST is required for channel handshake"));
    }
    let sid = params
        .get("SID")
        .cloned()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let channel = {
        let mut sessions = state.sessions.lock().await;
        if initial {
            // Drop idle channels. No per-channel background task or permanent subscription is kept.
            sessions.retain(|_, c| {
                c.inner
                    .try_lock()
                    .map(|s| s.touched.elapsed() < Duration::from_secs(300))
                    .unwrap_or(true)
            });
            sessions.insert(
                sid.clone(),
                Arc::new(Channel {
                    inner: Mutex::new(Session {
                        seq: 0,
                        messages: BTreeMap::new(),
                        targets: BTreeMap::new(),
                        received: Default::default(),
                        touched: Instant::now(),
                        write_ready: false,
                        database: params.get("database").cloned().unwrap_or_default(),
                    }),
                    ready: Notify::new(),
                }),
            );
        }
        sessions
            .get(&sid)
            .cloned()
            .ok_or_else(|| invalid("Unknown SID"))?
    };
    if method == Method::POST {
        let messages = form_messages(body)?;
        let mut session = channel.inner.lock().await;
        session.touched = Instant::now();
        for (id, message) in messages {
            if !session.received.insert(id) {
                continue;
            }
            if let Err(e) = process(state, rpc, &sid, &mut session, message).await {
                session.push(error_json(&e));
            }
        }
        let seq = session.seq;
        drop(session);
        channel.ready.notify_one();
        return Ok(if initial {
            frame(json!([[0, ["c", sid, "", 8, 12, 30000]]]))
        } else {
            frame(json!([1, seq, 0]))
        });
    }
    if method != Method::GET || params.get("RID").map(String::as_str) != Some("rpc") {
        return Err(invalid("invalid channel request"));
    }
    let aid = params.get("AID").and_then(|v| v.parse().ok()).unwrap_or(0);
    let mut changes = state.core.subscribe();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let notified = channel.ready.notified();
        {
            let mut session = channel.inner.lock().await;
            session.touched = Instant::now();
            if rpc == "Listen" {
                refresh(state, &mut session, None).await?;
            }
            if let Some(body) = session.queued(aid) {
                return Ok(body);
            }
        }
        tokio::select! {
            _ = notified => {},
            _ = changes.recv() => {},
            _ = tokio::time::sleep_until(deadline) => {
                let mut session = channel.inner.lock().await;
                session.seq += 1;
                let seq = session.seq;
                session.messages.insert(seq, json!([seq,["noop"]]));
                return Ok(session.queued(aid).unwrap());
            }
        }
    }
}
async fn process(
    state: &WebState,
    rpc: &str,
    sid: &str,
    session: &mut Session,
    message: Json,
) -> Result<(), Status> {
    if rpc == "Write" {
        if !session.write_ready {
            session.write_ready = true;
            if let Some(database) = message["database"].as_str() {
                session.database = database.to_owned();
            }
            session.push(json!({"streamId":sid,"streamToken":STANDARD.encode("0")}));
        } else {
            let mut result = commit(&state.core, &session.database, &message).await?;
            result["streamId"] = json!(sid);
            result["streamToken"] = json!(STANDARD.encode((session.seq + 1).to_string()));
            session.push(result);
        }
    } else if let Some(target) = message.get("addTarget") {
        let id = target["targetId"]
            .as_i64()
            .ok_or_else(|| invalid("targetId required"))? as i32;
        session.targets.insert(
            id,
            Target {
                spec: target.clone(),
                documents: BTreeMap::new(),
            },
        );
        session.push(json!({"targetChange":{"targetChangeType":"ADD","targetIds":[id]}}));
        refresh(state, session, Some(id)).await?;
    } else if let Some(id) = message["removeTarget"].as_i64() {
        session.targets.remove(&(id as i32));
        session.push(json!({"targetChange":{"targetChangeType":"REMOVE","targetIds":[id]}}));
    }
    Ok(())
}
async fn target_docs(core: &FirestoreService, spec: &Json) -> Result<Vec<pb::Document>, Status> {
    if let Some(query) = spec.get("query") {
        core.query_docs(
            &string(query, "parent"),
            &query_proto(&query["structuredQuery"])?,
        )
        .await
    } else {
        let names = array(&spec["documents"], "documents")
            .iter()
            .filter_map(|name| name.as_str().map(str::to_owned))
            .collect::<Vec<_>>();
        core.documents(&names).await
    }
}
async fn refresh(
    state: &WebState,
    session: &mut Session,
    initial: Option<i32>,
) -> Result<(), Status> {
    let ids: Vec<_> = initial
        .map(|id| vec![id])
        .unwrap_or_else(|| session.targets.keys().copied().collect());
    let read_time = time_json(Some(&now()));
    let mut changed = false;
    for id in ids {
        let target = session.targets.get_mut(&id).unwrap();
        let documents = target_docs(&state.core, &target.spec).await?;
        let next: BTreeMap<_, _> = documents
            .iter()
            .map(|d| (d.name.clone(), d.clone()))
            .collect();
        let mut messages = Vec::new();
        for document in &documents {
            if target
                .documents
                .get(&document.name)
                .map(document_json)
                .as_ref()
                != Some(&document_json(document))
            {
                messages.push(json!({"documentChange":{"document":document_json(document),"targetIds":[id],"removedTargetIds":[]}}));
            }
        }
        for name in target
            .documents
            .keys()
            .filter(|name| !next.contains_key(*name))
        {
            messages.push(json!({"documentRemove":{"document":name,"removedTargetIds":[id],"readTime":read_time}}));
        }
        target.documents = next;
        changed |= !messages.is_empty();
        for message in messages {
            session.push(message);
        }
        if initial.is_some() {
            session.push(json!({"targetChange":{"targetChangeType":"CURRENT","targetIds":[id],"readTime":read_time}}));
        }
    }
    if changed || initial.is_some() {
        session.push(json!({"targetChange":{"targetChangeType":"NO_CHANGE","targetIds":[],"readTime":read_time}}));
    }
    Ok(())
}

async fn rest(
    State(state): State<WebState>,
    Path(resource): Path<String>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if method == Method::OPTIONS {
        return response(StatusCode::OK, String::new(), &headers, "application/json");
    }
    let result = async {
        if method != Method::POST {
            return Err(Status::unimplemented("REST method is not supported"));
        }
        let v: Json = serde_json::from_slice(&body).map_err(|_| invalid("invalid JSON"))?;
        if let Some(parent) = resource.strip_suffix(":commit") {
            commit(
                &state.core,
                parent.strip_suffix("/documents").unwrap_or(parent),
                &v,
            )
            .await
        } else if let Some(parent) = resource.strip_suffix(":batchGet") {
            use tokio_stream::StreamExt;
            let request = pb::BatchGetDocumentsRequest {
                database: parent
                    .strip_suffix("/documents")
                    .unwrap_or(parent)
                    .to_owned(),
                documents: array(&v, "documents")
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect(),
                ..Default::default()
            };
            let mut stream = state
                .core
                .batch_get_documents(Request::new(request))
                .await?
                .into_inner();
            let mut results = Vec::new();
            while let Some(item) = stream.next().await {
                let item = item?;
                let time = time_json(item.read_time.as_ref());
                results.push(match item.result {
                    Some(pb::batch_get_documents_response::Result::Found(doc)) => {
                        json!({"found":document_json(&doc),"readTime":time})
                    }
                    Some(pb::batch_get_documents_response::Result::Missing(name)) => {
                        json!({"missing":name,"readTime":time})
                    }
                    None => json!({"readTime":time}),
                });
            }
            Ok(json!(results))
        } else if let Some(parent) = resource.strip_suffix(":runQuery") {
            let docs = state
                .core
                .query_docs(parent, &query_proto(&v["structuredQuery"])?)
                .await?;
            let time = time_json(Some(&now()));
            let mut results: Vec<_> = docs
                .iter()
                .map(|d| json!({"document":document_json(d),"readTime":time}))
                .collect();
            if results.is_empty() {
                results.push(json!({"readTime":time}));
            }
            Ok(json!(results))
        } else {
            Err(Status::unimplemented("unknown REST operation"))
        }
    }
    .await;
    match result {
        Ok(v) => response(StatusCode::OK, v.to_string(), &headers, "application/json"),
        Err(e) => {
            let status = match e.code() {
                tonic::Code::NotFound => StatusCode::NOT_FOUND,
                tonic::Code::AlreadyExists => StatusCode::CONFLICT,
                tonic::Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
                _ => StatusCode::BAD_REQUEST,
            };
            response(
                status,
                error_json(&e).to_string(),
                &headers,
                "application/json",
            )
        }
    }
}
async fn commit(core: &FirestoreService, database: &str, v: &Json) -> Result<Json, Status> {
    let writes = array(v, "writes")
        .iter()
        .map(write_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let result = core
        .commit(Request::new(pb::CommitRequest {
            database: database.to_owned(),
            writes,
            transaction: Vec::new(),
        }))
        .await?
        .into_inner();
    Ok(
        json!({"writeResults":result.write_results.iter().map(|r| json!({"updateTime":time_json(r.update_time.as_ref()),"transformResults":r.transform_results.iter().map(value_json).collect::<Vec<_>>()})).collect::<Vec<_>>(),"commitTime":time_json(result.commit_time.as_ref())}),
    )
}
fn timestamp(v: &Json) -> Result<prost_types::Timestamp, Status> {
    if let Some(s) = v.as_str() {
        s.parse().map_err(|_| invalid("invalid timestamp"))
    } else {
        Ok(prost_types::Timestamp {
            seconds: v["seconds"]
                .as_i64()
                .or_else(|| v["seconds"].as_str().and_then(|s| s.parse().ok()))
                .ok_or_else(|| invalid("invalid timestamp"))?,
            nanos: v["nanos"].as_i64().unwrap_or(0) as i32,
        })
    }
}
fn fields_proto(v: &Json) -> Result<HashMap<String, pb::Value>, Status> {
    v.as_object()
        .map(|m| {
            m.iter()
                .map(|(k, v)| Ok((k.clone(), value_proto(v)?)))
                .collect()
        })
        .unwrap_or_else(|| Ok(HashMap::new()))
}
fn document_proto(v: &Json) -> Result<pb::Document, Status> {
    Ok(pb::Document {
        name: string(v, "name"),
        fields: fields_proto(&v["fields"])?,
        ..Default::default()
    })
}
pub(crate) fn document_json(d: &pb::Document) -> Json {
    json!({"name":d.name,"fields":d.fields.iter().map(|(k,v)|(k.clone(),value_json(v))).collect::<serde_json::Map<_,_>>(),"createTime":time_json(d.create_time.as_ref()),"updateTime":time_json(d.update_time.as_ref())})
}
fn value_proto(v: &Json) -> Result<pb::Value, Status> {
    use pb::value::ValueType as V;
    let value = if v.get("nullValue").is_some() {
        V::NullValue(0)
    } else if let Some(b) = v["booleanValue"].as_bool() {
        V::BooleanValue(b)
    } else if let Some(i) = v.get("integerValue") {
        V::IntegerValue(
            i.as_i64()
                .or_else(|| i.as_str().and_then(|s| s.parse().ok()))
                .ok_or_else(|| invalid("invalid integer"))?,
        )
    } else if let Some(d) = v.get("doubleValue") {
        V::DoubleValue(
            d.as_f64()
                .or_else(|| {
                    d.as_str().and_then(|s| match s {
                        "NaN" => Some(f64::NAN),
                        "Infinity" => Some(f64::INFINITY),
                        "-Infinity" => Some(f64::NEG_INFINITY),
                        _ => None,
                    })
                })
                .ok_or_else(|| invalid("invalid double"))?,
        )
    } else if let Some(t) = v.get("timestampValue") {
        V::TimestampValue(timestamp(t)?)
    } else if let Some(s) = v["stringValue"].as_str() {
        V::StringValue(s.to_owned())
    } else if let Some(s) = v["referenceValue"].as_str() {
        V::ReferenceValue(s.to_owned())
    } else if let Some(s) = v["bytesValue"].as_str() {
        V::BytesValue(STANDARD.decode(s).map_err(|_| invalid("invalid base64"))?)
    } else if let Some(g) = v.get("geoPointValue") {
        V::GeoPointValue(crate::firestore::google::r#type::LatLng {
            latitude: g["latitude"].as_f64().unwrap_or(0.0),
            longitude: g["longitude"].as_f64().unwrap_or(0.0),
        })
    } else if let Some(a) = v.get("arrayValue") {
        V::ArrayValue(pb::ArrayValue {
            values: array(a, "values")
                .iter()
                .map(value_proto)
                .collect::<Result<_, _>>()?,
        })
    } else if let Some(m) = v.get("mapValue") {
        V::MapValue(pb::MapValue {
            fields: fields_proto(&m["fields"])?,
        })
    } else {
        return Err(invalid("unknown Firestore value"));
    };
    Ok(pb::Value {
        value_type: Some(value),
    })
}
fn value_json(v: &pb::Value) -> Json {
    use pb::value::ValueType as V;
    match v.value_type.as_ref() {
        Some(V::NullValue(_)) | None => json!({"nullValue":null}),
        Some(V::BooleanValue(b)) => json!({"booleanValue":b}),
        Some(V::IntegerValue(i)) => json!({"integerValue":i.to_string()}),
        Some(V::DoubleValue(d)) => {
            json!({"doubleValue":if d.is_nan() { json!("NaN") } else if *d == f64::INFINITY { json!("Infinity") } else if *d == f64::NEG_INFINITY {json!("-Infinity")} else {json!(d)}})
        }
        Some(V::TimestampValue(t)) => json!({"timestampValue":time_json(Some(t))}),
        Some(V::StringValue(s)) => json!({"stringValue":s}),
        Some(V::ReferenceValue(s)) => json!({"referenceValue":s}),
        Some(V::BytesValue(b)) => json!({"bytesValue":STANDARD.encode(b)}),
        Some(V::GeoPointValue(g)) => {
            json!({"geoPointValue":{"latitude":g.latitude,"longitude":g.longitude}})
        }
        Some(V::ArrayValue(a)) => {
            json!({"arrayValue":{"values":a.values.iter().map(value_json).collect::<Vec<_>>()}})
        }
        Some(V::MapValue(m)) => {
            json!({"mapValue":{"fields":m.fields.iter().map(|(k,v)|(k.clone(),value_json(v))).collect::<serde_json::Map<_,_>>()}})
        }
    }
}
fn transform_proto(v: &Json) -> Result<pb::document_transform::FieldTransform, Status> {
    use pb::document_transform::field_transform::TransformType as T;
    let t = if v.get("setToServerValue").is_some() {
        T::SetToServerValue(1)
    } else if let Some(x) = v.get("increment") {
        T::Increment(value_proto(x)?)
    } else if let Some(x) = v.get("maximum") {
        T::Maximum(value_proto(x)?)
    } else if let Some(x) = v.get("minimum") {
        T::Minimum(value_proto(x)?)
    } else if let Some(x) = v.get("appendMissingElements") {
        T::AppendMissingElements(pb::ArrayValue {
            values: array(x, "values")
                .iter()
                .map(value_proto)
                .collect::<Result<_, _>>()?,
        })
    } else if let Some(x) = v.get("removeAllFromArray") {
        T::RemoveAllFromArray(pb::ArrayValue {
            values: array(x, "values")
                .iter()
                .map(value_proto)
                .collect::<Result<_, _>>()?,
        })
    } else {
        return Err(invalid("unknown transform"));
    };
    Ok(pb::document_transform::FieldTransform {
        field_path: string(v, "fieldPath"),
        transform_type: Some(t),
    })
}
fn write_proto(v: &Json) -> Result<pb::Write, Status> {
    let operation = if let Some(doc) = v.get("update") {
        pb::write::Operation::Update(document_proto(doc)?)
    } else if let Some(name) = v["delete"].as_str() {
        pb::write::Operation::Delete(name.to_owned())
    } else if let Some(t) = v.get("transform") {
        pb::write::Operation::Transform(pb::DocumentTransform {
            document: string(t, "document"),
            field_transforms: array(t, "fieldTransforms")
                .iter()
                .map(transform_proto)
                .collect::<Result<_, _>>()?,
        })
    } else {
        return Err(invalid("write operation required"));
    };
    let current_document = v
        .get("currentDocument")
        .map(|c| {
            let condition_type = if let Some(b) = c["exists"].as_bool() {
                pb::precondition::ConditionType::Exists(b)
            } else {
                pb::precondition::ConditionType::UpdateTime(timestamp(&c["updateTime"])?)
            };
            Ok::<_, Status>(pb::Precondition {
                condition_type: Some(condition_type),
            })
        })
        .transpose()?;
    Ok(pb::Write {
        operation: Some(operation),
        current_document,
        update_mask: v.get("updateMask").map(|m| pb::DocumentMask {
            field_paths: array(m, "fieldPaths")
                .iter()
                .filter_map(|s| s.as_str().map(str::to_owned))
                .collect(),
        }),
        update_transforms: array(v, "updateTransforms")
            .iter()
            .map(transform_proto)
            .collect::<Result<_, _>>()?,
    })
}
fn field(v: &Json) -> pb::structured_query::FieldReference {
    pb::structured_query::FieldReference {
        field_path: string(v, "fieldPath"),
    }
}
fn filter_proto(v: &Json) -> Result<pb::structured_query::Filter, Status> {
    use pb::structured_query as q;
    let filter_type = if let Some(f) = v.get("fieldFilter") {
        q::filter::FilterType::FieldFilter(q::FieldFilter {
            field: Some(field(&f["field"])),
            op: q::field_filter::Operator::from_str_name(&string(f, "op"))
                .ok_or_else(|| invalid("invalid field operator"))? as i32,
            value: Some(value_proto(&f["value"])?),
        })
    } else if let Some(f) = v.get("compositeFilter") {
        q::filter::FilterType::CompositeFilter(q::CompositeFilter {
            op: q::composite_filter::Operator::from_str_name(&string(f, "op"))
                .ok_or_else(|| invalid("invalid composite operator"))? as i32,
            filters: array(f, "filters")
                .iter()
                .map(filter_proto)
                .collect::<Result<_, _>>()?,
        })
    } else if let Some(f) = v.get("unaryFilter") {
        q::filter::FilterType::UnaryFilter(q::UnaryFilter {
            op: q::unary_filter::Operator::from_str_name(&string(f, "op"))
                .ok_or_else(|| invalid("invalid unary operator"))? as i32,
            operand_type: Some(q::unary_filter::OperandType::Field(field(&f["field"]))),
        })
    } else {
        return Err(invalid("unknown filter"));
    };
    Ok(q::Filter {
        filter_type: Some(filter_type),
    })
}
fn query_proto(v: &Json) -> Result<pb::StructuredQuery, Status> {
    use pb::structured_query as q;
    let cursor = |c: &Json| -> Result<q::Cursor, Status> {
        Ok(q::Cursor {
            values: array(c, "values")
                .iter()
                .map(value_proto)
                .collect::<Result<_, _>>()?,
            before: c["before"].as_bool().unwrap_or(false),
        })
    };
    Ok(pb::StructuredQuery {
        select: v.get("select").map(|s| q::Projection {
            fields: array(s, "fields").iter().map(field).collect(),
        }),
        from: array(v, "from")
            .iter()
            .map(|s| q::CollectionSelector {
                collection_id: string(s, "collectionId"),
                all_descendants: s["allDescendants"].as_bool().unwrap_or(false),
            })
            .collect(),
        r#where: v.get("where").map(filter_proto).transpose()?,
        order_by: array(v, "orderBy")
            .iter()
            .map(|o| {
                Ok(q::Order {
                    field: Some(field(&o["field"])),
                    direction: q::Direction::from_str_name(&string(o, "direction"))
                        .ok_or_else(|| invalid("invalid order direction"))?
                        as i32,
                })
            })
            .collect::<Result<_, Status>>()?,
        limit: v
            .get("limit")
            .and_then(|n| n.as_i64().or_else(|| n["value"].as_i64()))
            .map(|n| n as i32),
        offset: v["offset"].as_i64().unwrap_or(0) as i32,
        start_at: v.get("startAt").map(cursor).transpose()?,
        end_at: v.get("endAt").map(cursor).transpose()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn frame_counts_utf16_and_form_orders_requests() {
        let value = json!([[1,[{"text":"a😀é"}]]]);
        let encoded = frame(value.clone());
        let (length, body) = encoded.split_once('\n').unwrap();
        assert_eq!(
            length.parse::<usize>().unwrap(),
            body.encode_utf16().count()
        );
        assert_eq!(serde_json::from_str::<Json>(body).unwrap(), value);
        let messages = form_messages(
            b"count=2&ofs=3&req1___data__=%7B%22n%22%3A2%7D&req0___data__=%7B%22n%22%3A1%7D",
        )
        .unwrap();
        assert_eq!(messages, vec![(3, json!({"n":1})), (4, json!({"n":2}))]);
    }
    #[test]
    fn json_values_round_trip() {
        for v in [
            json!({"integerValue":"9223372036854775807"}),
            json!({"timestampValue":"2026-09-06T01:02:03.123456789Z"}),
            json!({"bytesValue":"AAH/"}),
            json!({"mapValue":{"fields":{"arr":{"arrayValue":{"values":[{"stringValue":"😀"},{"nullValue":null}]}}}}}),
            json!({"doubleValue":"NaN"}),
        ] {
            assert_eq!(value_json(&value_proto(&v).unwrap()), v);
        }
    }
    #[tokio::test]
    async fn shared_commit_listen_diff_and_ack_replay() {
        let core = FirestoreService::default();
        let state = WebState {
            core: core.clone(),
            sessions: Default::default(),
        };
        let database = "projects/test/databases/(default)";
        let name = format!("{database}/documents/items/a");
        let mut session = Session {
            seq: 0,
            messages: BTreeMap::new(),
            targets: BTreeMap::new(),
            received: Default::default(),
            touched: Instant::now(),
            write_ready: false,
            database: database.into(),
        };
        process(
            &state,
            "Listen",
            "test",
            &mut session,
            json!({"addTarget":{"targetId":2,"documents":{"documents":[name]}}}),
        )
        .await
        .unwrap();
        assert_eq!(session.seq, 3);
        let first = session.queued(0).unwrap();
        assert_eq!(session.queued(0).unwrap(), first);
        assert!(session.queued(3).is_none());
        commit(&core,database,&json!({"writes":[{"update":{"name":name,"fields":{"label":{"stringValue":"😀"},"nan":{"doubleValue":"NaN"}}}}]})).await.unwrap();
        refresh(&state, &mut session, None).await.unwrap();
        assert_eq!(session.seq, 5);
        assert!(session.messages[&4][1][0].get("documentChange").is_some());
        assert_eq!(core.store.lock().await.documents.len(), 1);
        refresh(&state, &mut session, None).await.unwrap();
        assert_eq!(session.seq, 5, "NaN must not cause repeat changes");
        commit(&core, database, &json!({"writes":[{"delete":name}]}))
            .await
            .unwrap();
        refresh(&state, &mut session, None).await.unwrap();
        assert!(session.messages[&6][1][0].get("documentRemove").is_some());
        assert!(session.queued(7).is_none());
    }
}
