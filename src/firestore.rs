use prost::Message;
use rusqlite::{params, types::Type, OptionalExtension};
use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
fn database_document_prefix(database: &str) -> Result<String, Status> {
    let parts: Vec<_> = database.split('/').collect();
    if parts.len() != 4
        || parts[0] != "projects"
        || parts[1].is_empty()
        || parts[2] != "databases"
        || parts[3].is_empty()
    {
        return Err(Status::invalid_argument(format!(
            "invalid database name: {database}"
        )));
    }
    Ok(format!("{database}/documents/"))
}

fn database_from_resource(resource: &str) -> Result<String, Status> {
    let parts: Vec<_> = resource.split('/').collect();
    if parts.len() < 4 {
        return Err(Status::invalid_argument(format!(
            "invalid Firestore resource: {resource}"
        )));
    }
    let database = parts[..4].join("/");
    database_document_prefix(&database)?;
    Ok(database)
}

use axum07::{extract::Path, http::StatusCode, routing::delete};
use tokio::sync::{broadcast, mpsc, Mutex};
use tonic::{Request, Response, Status};

pub mod google {
    pub mod firestore {
        pub mod v1 {
            tonic::include_proto!("google.firestore.v1");
        }
        pub mod emulator {
            pub mod v1 {
                tonic::include_proto!("google.firestore.emulator.v1");
            }
        }
    }
    pub mod rpc {
        tonic::include_proto!("google.rpc");
    }
    pub mod r#type {
        include!(concat!(env!("OUT_DIR"), "/google.r#type.rs"));
    }
}

use google::firestore::v1::{
    document_transform, listen_request, listen_response, structured_query, target,
    value::ValueType, ArrayValue, DocumentChange, DocumentMask, DocumentRemove, ListenRequest,
    ListenResponse, MapValue, ServerValue, StructuredQuery, Target, TargetChange, Value,
};
use google::firestore::{
    emulator::v1::{
        firestore_emulator_server::{FirestoreEmulator, FirestoreEmulatorServer},
        ClearDataRequest,
    },
    v1::{
        batch_get_documents_request, batch_get_documents_response,
        firestore_server::{Firestore, FirestoreServer},
        precondition, run_query_request, write, BatchGetDocumentsRequest,
        BatchGetDocumentsResponse, BatchWriteRequest, BatchWriteResponse, BeginTransactionRequest,
        BeginTransactionResponse, CommitRequest, CommitResponse, CreateDocumentRequest,
        DeleteDocumentRequest, Document, GetDocumentRequest, Precondition, RollbackRequest,
        RunQueryRequest, RunQueryResponse, UpdateDocumentRequest, Write, WriteResult,
    },
};
use google::rpc::Status as RpcStatus;
use std::{cmp::Ordering, collections::HashMap};

#[derive(Clone, Default)]
pub(crate) struct Store {
    pub(crate) documents: BTreeMap<String, Document>,
    transactions: BTreeMap<Vec<u8>, String>,
    next_id: u64,
}

#[derive(Clone)]
pub(crate) struct FirestoreService {
    pub(crate) store: Arc<Mutex<Store>>,
    changes: broadcast::Sender<()>,
    persistence: Option<crate::persistence::Persistence>,
    #[cfg(test)]
    batch_get_barrier: Option<Arc<std::sync::Barrier>>,
}
impl Default for FirestoreService {
    fn default() -> Self {
        Self {
            store: Default::default(),
            changes: broadcast::channel(128).0,
            persistence: None,
            #[cfg(test)]
            batch_get_barrier: None,
        }
    }
}

fn persistence_status(error: crate::persistence::Error) -> Status {
    Status::unavailable(format!("durable Firestore operation failed: {error}"))
}

fn decode_document(column: usize, bytes: Vec<u8>) -> Result<Document, crate::persistence::Error> {
    Document::decode(bytes.as_slice()).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column, Type::Blob, Box::new(error)).into()
    })
}

fn write_name(write: &Write) -> Result<&str, Status> {
    match write.operation.as_ref() {
        Some(write::Operation::Update(document)) => Ok(&document.name),
        Some(write::Operation::Delete(name)) => Ok(name),
        Some(write::Operation::Transform(transform)) => Ok(&transform.document),
        None => Err(Status::invalid_argument("write operation is required")),
    }
}

fn validate_writes_database(database: &str, writes: &[Write]) -> Result<(), Status> {
    database_document_prefix(database)?;
    for write in writes {
        let name = write_name(write)?;
        validate_document_name(name)?;
        if database_from_resource(name)? != database {
            return Err(Status::invalid_argument(format!(
                "write document does not belong to requested database: {name}"
            )));
        }
    }
    Ok(())
}

fn timestamp_now() -> prost_types::Timestamp {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    prost_types::Timestamp {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    }
}

fn validate_document_name(name: &str) -> Result<(), Status> {
    let parts: Vec<_> = name.split('/').collect();
    let valid_prefix = parts.len() >= 7
        && parts[0] == "projects"
        && !parts[1].is_empty()
        && parts[2] == "databases"
        && !parts[3].is_empty()
        && parts[4] == "documents";
    let document_path_len = parts.len().saturating_sub(5);
    if !valid_prefix || document_path_len < 2 || document_path_len % 2 != 0 {
        return Err(Status::invalid_argument(format!(
            "invalid document name: {name}"
        )));
    }
    Ok(())
}

fn check_precondition(
    existing: Option<&Document>,
    condition: Option<&Precondition>,
) -> Result<(), Status> {
    let Some(condition) = condition.and_then(|value| value.condition_type.as_ref()) else {
        return Ok(());
    };
    match condition {
        precondition::ConditionType::Exists(expected) if *expected == existing.is_some() => Ok(()),
        precondition::ConditionType::Exists(_) => Err(Status::failed_precondition(
            "document existence precondition failed",
        )),
        precondition::ConditionType::UpdateTime(expected)
            if existing.and_then(|doc| doc.update_time.as_ref()) == Some(expected) =>
        {
            Ok(())
        }
        precondition::ConditionType::UpdateTime(_) => Err(Status::failed_precondition(
            "document update_time precondition failed",
        )),
    }
}

fn apply_write(
    documents: &mut BTreeMap<String, Document>,
    request: Write,
    write_time: &prost_types::Timestamp,
) -> Result<WriteResult, Status> {
    let Write {
        operation,
        current_document,
        update_transforms,
        update_mask,
    } = request;
    match operation {
        Some(write::Operation::Update(mut document)) => {
            validate_document_name(&document.name)?;
            let existing = documents.get(&document.name);
            check_precondition(existing, current_document.as_ref())?;
            document = merge_document(existing, document.clone(), update_mask.as_ref())?;
            let transform_results =
                apply_update_transforms(&mut document, &update_transforms, write_time)?;
            let create_time = existing
                .and_then(|doc| doc.create_time.clone())
                .unwrap_or_else(|| write_time.clone());
            document.create_time = Some(create_time);
            document.update_time = Some(write_time.clone());
            documents.insert(document.name.clone(), document);
            Ok(WriteResult {
                update_time: Some(write_time.clone()),
                transform_results,
            })
        }
        Some(write::Operation::Delete(name)) => {
            validate_document_name(&name)?;
            check_precondition(documents.get(&name), current_document.as_ref())?;
            documents.remove(&name);
            Ok(WriteResult {
                update_time: Some(write_time.clone()),
                transform_results: Vec::new(),
            })
        }
        Some(write::Operation::Transform(_)) => Err(Status::unimplemented(
            "document transforms are not implemented",
        )),
        None => Err(Status::invalid_argument("write operation is required")),
    }
}

fn numeric_increment(current: Option<&Value>, operand: &Value) -> Result<Value, Status> {
    use ValueType::{DoubleValue, IntegerValue};
    let operand = operand
        .value_type
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("increment operand is required"))?;
    let result = match (current.and_then(|value| value.value_type.as_ref()), operand) {
        (Some(IntegerValue(a)), IntegerValue(b)) => IntegerValue(a.saturating_add(*b)),
        (Some(IntegerValue(a)), DoubleValue(b)) => DoubleValue(*a as f64 + *b),
        (Some(DoubleValue(a)), IntegerValue(b)) => DoubleValue(*a + *b as f64),
        (Some(DoubleValue(a)), DoubleValue(b)) => DoubleValue(*a + *b),
        (_, IntegerValue(value)) => IntegerValue(*value),
        (_, DoubleValue(value)) => DoubleValue(*value),
        _ => {
            return Err(Status::invalid_argument(
                "increment operand must be numeric",
            ))
        }
    };
    Ok(Value {
        value_type: Some(result),
    })
}

fn float_equal(a: f64, b: f64) -> bool {
    a == b || (a.is_nan() && b.is_nan())
}

fn transform_value_equal(a: &Value, b: &Value) -> bool {
    use ValueType::*;
    match (a.value_type.as_ref(), b.value_type.as_ref()) {
        (Some(NullValue(_)), Some(NullValue(_))) => true,
        (Some(BooleanValue(a)), Some(BooleanValue(b))) => a == b,
        (Some(IntegerValue(a)), Some(IntegerValue(b))) => a == b,
        (Some(IntegerValue(a)), Some(DoubleValue(b))) => {
            compare_integer_double(*a, *b) == Ordering::Equal
        }
        (Some(DoubleValue(a)), Some(IntegerValue(b))) => {
            compare_integer_double(*b, *a) == Ordering::Equal
        }
        (Some(DoubleValue(a)), Some(DoubleValue(b))) => float_equal(*a, *b),
        (Some(TimestampValue(a)), Some(TimestampValue(b))) => a == b,
        (Some(StringValue(a)), Some(StringValue(b))) => a == b,
        (Some(BytesValue(a)), Some(BytesValue(b))) => a == b,
        (Some(ReferenceValue(a)), Some(ReferenceValue(b))) => a == b,
        (Some(GeoPointValue(a)), Some(GeoPointValue(b))) => {
            float_equal(a.latitude, b.latitude) && float_equal(a.longitude, b.longitude)
        }
        (Some(ArrayValue(a)), Some(ArrayValue(b))) => {
            a.values.len() == b.values.len()
                && a.values
                    .iter()
                    .zip(&b.values)
                    .all(|(a, b)| transform_value_equal(a, b))
        }
        (Some(MapValue(a)), Some(MapValue(b))) => {
            a.fields.len() == b.fields.len()
                && a.fields.iter().all(|(key, value)| {
                    b.fields
                        .get(key)
                        .is_some_and(|other| transform_value_equal(value, other))
                })
        }
        (None, None) => true,
        _ => false,
    }
}

fn apply_update_transforms(
    document: &mut Document,
    transforms: &[document_transform::FieldTransform],
    request_time: &prost_types::Timestamp,
) -> Result<Vec<Value>, Status> {
    use document_transform::field_transform::TransformType;
    let mut results = Vec::with_capacity(transforms.len());
    for transform in transforms {
        match transform.transform_type.as_ref() {
            Some(TransformType::Increment(_)) | Some(TransformType::AppendMissingElements(_)) => {}
            Some(TransformType::SetToServerValue(value))
                if *value == ServerValue::RequestTime as i32 => {}
            _ => {
                return Err(Status::unimplemented(
                    "requested field transform is not implemented",
                ))
            }
        }
        let path = field_path(&transform.field_path)?;
        let current = nested_get(&document.fields, &path).cloned();
        let result = match transform.transform_type.as_ref() {
            Some(TransformType::Increment(operand)) => {
                numeric_increment(current.as_ref(), operand)?
            }
            Some(TransformType::AppendMissingElements(elements)) => {
                let mut values = match current.and_then(|value| value.value_type) {
                    Some(ValueType::ArrayValue(array)) => array.values,
                    _ => Vec::new(),
                };
                for candidate in &elements.values {
                    if !values
                        .iter()
                        .any(|existing| transform_value_equal(existing, candidate))
                    {
                        values.push(candidate.clone());
                    }
                }
                Value {
                    value_type: Some(ValueType::ArrayValue(ArrayValue { values })),
                }
            }
            Some(TransformType::SetToServerValue(value))
                if *value == ServerValue::RequestTime as i32 =>
            {
                Value {
                    value_type: Some(ValueType::TimestampValue(request_time.clone())),
                }
            }
            Some(TransformType::SetToServerValue(_))
            | Some(TransformType::Maximum(_))
            | Some(TransformType::Minimum(_))
            | Some(TransformType::RemoveAllFromArray(_))
            | None => {
                return Err(Status::unimplemented(
                    "requested field transform is not implemented",
                ))
            }
        };
        nested_set(&mut document.fields, &path, Some(result.clone()));
        results.push(result);
    }
    Ok(results)
}

fn status_proto(status: &Status) -> RpcStatus {
    RpcStatus {
        code: status.code() as i32,
        message: status.message().to_owned(),
        details: Vec::new(),
    }
}

fn validate_transaction(store: &Store, database: &str, transaction: &[u8]) -> Result<(), Status> {
    match store.transactions.get(transaction) {
        Some(transaction_database) if transaction_database == database => Ok(()),
        Some(_) => Err(Status::invalid_argument(
            "transaction does not belong to the requested database",
        )),
        None => Err(Status::invalid_argument("transaction is not active")),
    }
}

fn create_transaction(store: &mut Store, database: String) -> Vec<u8> {
    let transaction = uuid::Uuid::new_v4().as_bytes().to_vec();
    store.transactions.insert(transaction.clone(), database);
    transaction
}

impl FirestoreService {
    pub(crate) fn new(persistence: Option<crate::persistence::Persistence>) -> Self {
        Self {
            persistence,
            ..Self::default()
        }
    }

    async fn document(&self, name: &str) -> Result<Option<Document>, Status> {
        if let Some(persistence) = &self.persistence {
            let name = name.to_owned();
            persistence
                .read(move |connection| {
                    let bytes = connection
                        .query_row(
                            "SELECT document FROM firestore_documents WHERE name=?1",
                            params![name],
                            |row| row.get::<_, Vec<u8>>(0),
                        )
                        .optional()?;
                    bytes.map(|bytes| decode_document(0, bytes)).transpose()
                })
                .await
                .map_err(persistence_status)
        } else {
            Ok(self.store.lock().await.documents.get(name).cloned())
        }
    }

    pub(crate) async fn documents(&self, names: &[String]) -> Result<Vec<Document>, Status> {
        if let Some(persistence) = &self.persistence {
            let names = names.to_vec();
            #[cfg(test)]
            let barrier = self.batch_get_barrier.clone();
            persistence
                .read(move |connection| {
                    let transaction = connection.transaction()?;
                    let mut documents = Vec::new();
                    let mut statement = transaction
                        .prepare("SELECT document FROM firestore_documents WHERE name=?1")?;
                    for (_index, name) in names.into_iter().enumerate() {
                        let bytes = statement
                            .query_row(params![name], |row| row.get::<_, Vec<u8>>(0))
                            .optional()?;
                        if let Some(bytes) = bytes {
                            documents.push(decode_document(0, bytes)?);
                        }
                        #[cfg(test)]
                        if _index == 0 {
                            if let Some(barrier) = &barrier {
                                barrier.wait();
                                barrier.wait();
                            }
                        }
                    }
                    drop(statement);
                    transaction.commit()?;
                    Ok(documents)
                })
                .await
                .map_err(persistence_status)
        } else {
            let store = self.store.lock().await;
            Ok(names
                .iter()
                .filter_map(|name| store.documents.get(name).cloned())
                .collect())
        }
    }

    async fn database_documents(&self, database: &str) -> Result<Vec<Document>, Status> {
        if let Some(persistence) = &self.persistence {
            let database = database.to_owned();
            persistence
                .read(move |connection| {
                    let mut statement = connection.prepare(
                        "SELECT document FROM firestore_documents \
                         WHERE database_name=?1 ORDER BY name",
                    )?;
                    let mut rows = statement.query(params![database])?;
                    let mut documents = Vec::new();
                    while let Some(row) = rows.next()? {
                        documents.push(decode_document(0, row.get(0)?)?);
                    }
                    Ok(documents)
                })
                .await
                .map_err(persistence_status)
        } else {
            let prefix = database_document_prefix(database)?;
            Ok(self
                .store
                .lock()
                .await
                .documents
                .range(prefix.clone()..)
                .take_while(|(name, _)| name.starts_with(&prefix))
                .map(|(_, document)| document.clone())
                .collect())
        }
    }

    async fn persistent_writes(
        &self,
        database: String,
        writes: Vec<Write>,
        partial: bool,
        write_time: prost_types::Timestamp,
    ) -> Result<(Vec<WriteResult>, Vec<RpcStatus>, BTreeMap<String, Document>), Status> {
        validate_writes_database(&database, &writes)?;
        let persistence = self.persistence.clone().expect("persistent backend");
        let operation_persistence = persistence.clone();
        let outcome = persistence
            .write(move |connection| {
                let transaction = connection.transaction()?;
                let mut names = writes
                    .iter()
                    .map(write_name)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("writes were validated")
                    .into_iter()
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                names.sort();
                names.dedup();
                let mut documents = BTreeMap::new();
                {
                    let mut statement = transaction
                        .prepare("SELECT document FROM firestore_documents WHERE name=?1")?;
                    for name in &names {
                        let bytes = statement
                            .query_row(params![name], |row| row.get::<_, Vec<u8>>(0))
                            .optional()?;
                        if let Some(bytes) = bytes {
                            documents.insert(name.clone(), decode_document(0, bytes)?);
                        }
                    }
                }
                let before = documents.clone();
                let mut results = Vec::with_capacity(writes.len());
                let mut statuses = Vec::with_capacity(writes.len());
                for write in writes {
                    match apply_write(&mut documents, write, &write_time) {
                        Ok(result) => {
                            results.push(result);
                            statuses.push(RpcStatus::default());
                        }
                        Err(status) if partial => {
                            results.push(WriteResult::default());
                            statuses.push(status_proto(&status));
                        }
                        Err(status) => return Ok(Err(status)),
                    }
                }
                for name in names {
                    if let Some(document) = documents.get(&name) {
                        transaction.execute(
                            "INSERT INTO firestore_documents(name,database_name,document) \
                             VALUES (?1,?2,?3) ON CONFLICT(name) DO UPDATE SET \
                             database_name=excluded.database_name,document=excluded.document",
                            params![name, database, document.encode_to_vec()],
                        )?;
                    } else {
                        transaction.execute(
                            "DELETE FROM firestore_documents WHERE name=?1",
                            params![name],
                        )?;
                    }
                }
                let records = crate::functions::document_outbox_records(&before, &documents);
                operation_persistence.insert_outbox(&transaction, &records)?;
                transaction.commit()?;
                Ok(Ok((results, statuses, documents)))
            })
            .await
            .map_err(persistence_status)??;
        crate::functions::outbox_committed();
        let _ = self.changes.send(());
        Ok(outcome)
    }
}

#[tonic::async_trait]
impl Firestore for FirestoreService {
    type ListenStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<ListenResponse, Status>> + Send>>;
    async fn listen(
        &self,
        request: Request<tonic::Streaming<ListenRequest>>,
    ) -> Result<Response<Self::ListenStream>, Status> {
        let mut incoming = request.into_inner();
        let service = self.clone();
        let mut changes = self.subscribe();
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            let mut targets: BTreeMap<i32, (Target, BTreeMap<String, Document>)> = BTreeMap::new();
            loop {
                tokio::select! {
                    _=tx.closed()=>break,
                    message=incoming.message()=> {
                        match message {
                            Ok(Some(message))=>match message.target_change {
                                Some(listen_request::TargetChange::AddTarget(mut target))=> {
                                    if target.target_id==0 {target.target_id=(1..).find(|id|!targets.contains_key(id)).unwrap();}
                                    let id=target.target_id;
                                    if tx.send(Ok(target_response(1,vec![id],false))).await.is_err(){break;}
                                    let mut previous=BTreeMap::new();
                                    if let Err(error)=emit_target(&service,&tx,&target,&mut previous,true).await {let _=tx.send(Err(error)).await;break;}
                                    targets.insert(id,(target,previous));
                                },
                                Some(listen_request::TargetChange::RemoveTarget(id))=>{targets.remove(&id);if tx.send(Ok(target_response(2,vec![id],false))).await.is_err(){break;}},
                                None=>{}
                            },
                            _=>break,
                        }
                    },
                    event=changes.recv()=> {
                        if matches!(event,Err(broadcast::error::RecvError::Closed)){break;}
                        let mut failed=false;
                        for (target,previous) in targets.values_mut() {if let Err(error)=emit_target(&service,&tx,target,previous,false).await {let _=tx.send(Err(error)).await;failed=true;break;}}
                        if failed {break;}
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn get_document(
        &self,
        request: Request<GetDocumentRequest>,
    ) -> Result<Response<Document>, Status> {
        let name = request.into_inner().name;
        validate_document_name(&name)?;
        self.document(&name)
            .await?
            .map(Response::new)
            .ok_or_else(|| Status::not_found(format!("document not found: {name}")))
    }

    async fn create_document(
        &self,
        request: Request<CreateDocumentRequest>,
    ) -> Result<Response<Document>, Status> {
        let request = request.into_inner();
        let mut document = request
            .document
            .ok_or_else(|| Status::invalid_argument("document is required"))?;
        if request.parent.is_empty() || request.collection_id.is_empty() {
            return Err(Status::invalid_argument(
                "parent and collection_id are required",
            ));
        }

        if let Some(persistence) = &self.persistence {
            let persistence = persistence.clone();
            let operation_persistence = persistence.clone();
            let parent = request.parent;
            let collection_id = request.collection_id;
            let requested_id = request.document_id;
            let outcome = persistence
                .write(move |connection| {
                    let transaction = connection.transaction()?;
                    let document_id = if requested_id.is_empty() {
                        transaction.query_row(
                            "UPDATE counters SET value=value+1 WHERE name='firestore_auto_id' RETURNING value",
                            [],
                            |row| row.get::<_, u64>(0),
                        ).map(|id| format!("auto-{id:020}"))?
                    } else {
                        requested_id
                    };
                    let name = format!(
                        "{}/{}/{}",
                        parent.trim_end_matches('/'), collection_id, document_id
                    );
                    if let Err(status) = validate_document_name(&name) {
                        return Ok(Err(status));
                    }
                    let database = match database_from_resource(&name) {
                        Ok(database) => database,
                        Err(status) => return Ok(Err(status)),
                    };
                    let exists = transaction.query_row(
                        "SELECT 1 FROM firestore_documents WHERE name=?1",
                        params![name],
                        |_| Ok(()),
                    ).optional()?.is_some();
                    if exists {
                        return Ok(Err(Status::already_exists(format!(
                            "document already exists: {name}"
                        ))));
                    }
                    let now = timestamp_now();
                    document.name = name.clone();
                    document.create_time = Some(now);
                    document.update_time = Some(now);
                    transaction.execute(
                        "INSERT INTO firestore_documents(name,database_name,document) VALUES (?1,?2,?3)",
                        params![name, database, document.encode_to_vec()],
                    )?;
                    let before = BTreeMap::new();
                    let after = BTreeMap::from([(name, document.clone())]);
                    let records = crate::functions::document_outbox_records(&before, &after);
                    operation_persistence.insert_outbox(&transaction, &records)?;
                    transaction.commit()?;
                    Ok(Ok(document))
                })
                .await
                .map_err(persistence_status)??;
            crate::functions::outbox_committed();
            let _ = self.changes.send(());
            return Ok(Response::new(outcome));
        }

        let mut store = self.store.lock().await;
        let document_id = if request.document_id.is_empty() {
            store.next_id += 1;
            format!("auto-{:020}", store.next_id)
        } else {
            request.document_id
        };
        let name = format!(
            "{}/{}/{}",
            request.parent.trim_end_matches('/'),
            request.collection_id,
            document_id
        );
        validate_document_name(&name)?;
        if store.documents.contains_key(&name) {
            return Err(Status::already_exists(format!(
                "document already exists: {name}"
            )));
        }
        let now = timestamp_now();
        document.name = name.clone();
        document.create_time = Some(now.clone());
        document.update_time = Some(now);
        store.documents.insert(name, document.clone());
        crate::functions::document_changed(None, Some(&document));
        let _ = self.changes.send(());
        Ok(Response::new(document))
    }

    async fn update_document(
        &self,
        request: Request<UpdateDocumentRequest>,
    ) -> Result<Response<Document>, Status> {
        let request = request.into_inner();
        let document = request
            .document
            .ok_or_else(|| Status::invalid_argument("document is required"))?;
        let name = document.name.clone();
        if self.persistence.is_some() {
            let (results, _, documents) = self
                .persistent_writes(
                    database_from_resource(&name)?,
                    vec![Write {
                        operation: Some(write::Operation::Update(document)),
                        update_mask: request.update_mask,
                        current_document: request.current_document,
                        update_transforms: vec![],
                    }],
                    false,
                    timestamp_now(),
                )
                .await?;
            let _ = results;
            return documents
                .get(&name)
                .cloned()
                .map(Response::new)
                .ok_or_else(|| Status::internal("updated document disappeared"));
        }
        let mut store = self.store.lock().await;
        let before = store.documents.get(&name).cloned();
        apply_write(
            &mut store.documents,
            Write {
                operation: Some(write::Operation::Update(document)),
                update_mask: request.update_mask,
                current_document: request.current_document,
                update_transforms: vec![],
            },
            &timestamp_now(),
        )?;
        let document = store.documents[&name].clone();
        crate::functions::document_changed(before.as_ref(), Some(&document));
        let _ = self.changes.send(());
        Ok(Response::new(document))
    }

    async fn delete_document(
        &self,
        request: Request<DeleteDocumentRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        validate_document_name(&request.name)?;
        if self.persistence.is_some() {
            self.persistent_writes(
                database_from_resource(&request.name)?,
                vec![Write {
                    operation: Some(write::Operation::Delete(request.name)),
                    update_mask: None,
                    current_document: request.current_document,
                    update_transforms: vec![],
                }],
                false,
                timestamp_now(),
            )
            .await?;
            return Ok(Response::new(()));
        }
        let mut store = self.store.lock().await;
        check_precondition(
            store.documents.get(&request.name),
            request.current_document.as_ref(),
        )?;
        let before = store.documents.remove(&request.name);
        crate::functions::document_changed(before.as_ref(), None);
        let _ = self.changes.send(());
        Ok(Response::new(()))
    }

    type BatchGetDocumentsStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<BatchGetDocumentsResponse, Status>> + Send>>;

    async fn batch_get_documents(
        &self,
        request: Request<BatchGetDocumentsRequest>,
    ) -> Result<Response<Self::BatchGetDocumentsStream>, Status> {
        let request = request.into_inner();
        for name in &request.documents {
            validate_document_name(name)?;
        }
        let read_time = timestamp_now();
        let (new_transaction, in_memory_documents) = {
            let mut store = self.store.lock().await;
            let new_transaction = match request.consistency_selector.as_ref() {
                Some(batch_get_documents_request::ConsistencySelector::Transaction(
                    transaction,
                )) => {
                    validate_transaction(&store, &request.database, transaction)?;
                    None
                }
                Some(batch_get_documents_request::ConsistencySelector::NewTransaction(_)) => {
                    database_document_prefix(&request.database)?;
                    Some(create_transaction(&mut store, request.database.clone()))
                }
                _ => None,
            };
            let documents = self.persistence.is_none().then(|| store.documents.clone());
            (new_transaction, documents)
        };
        let persistent_documents = if self.persistence.is_some() {
            Some(
                self.documents(&request.documents)
                    .await?
                    .into_iter()
                    .map(|document| (document.name.clone(), document))
                    .collect::<BTreeMap<_, _>>(),
            )
        } else {
            None
        };
        let mut responses: Vec<_> = request
            .documents
            .into_iter()
            .map(|name| {
                let result = persistent_documents
                    .as_ref()
                    .or(in_memory_documents.as_ref())
                    .expect("one Firestore backend supplies documents")
                    .get(&name)
                    .cloned()
                    .map(batch_get_documents_response::Result::Found)
                    .unwrap_or_else(|| batch_get_documents_response::Result::Missing(name));
                Ok(BatchGetDocumentsResponse {
                    result: Some(result),
                    transaction: Vec::new(),
                    read_time: Some(read_time.clone()),
                })
            })
            .collect();
        if responses.is_empty() && new_transaction.is_some() {
            responses.push(Ok(BatchGetDocumentsResponse {
                result: None,
                transaction: Vec::new(),
                read_time: Some(read_time),
            }));
        }
        if let (Some(transaction), Some(Ok(first))) = (new_transaction, responses.first_mut()) {
            first.transaction = transaction;
        }
        Ok(Response::new(Box::pin(tokio_stream::iter(responses))))
    }

    async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>, Status> {
        let request = request.into_inner();
        database_document_prefix(&request.database)?;
        let mut store = self.store.lock().await;
        let transaction = create_transaction(&mut store, request.database);
        Ok(Response::new(BeginTransactionResponse { transaction }))
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        let request = request.into_inner();
        let commit_time = timestamp_now();
        let mut store = self.store.lock().await;
        if !request.transaction.is_empty() {
            validate_transaction(&store, &request.database, &request.transaction)?;
        }
        if self.persistence.is_some() {
            let (results, _, _) = self
                .persistent_writes(request.database, request.writes, false, commit_time)
                .await?;
            if !request.transaction.is_empty() {
                store.transactions.remove(&request.transaction);
            }
            return Ok(Response::new(CommitResponse {
                write_results: results,
                commit_time: Some(commit_time),
            }));
        }
        let mut pending = store.documents.clone();
        let mut results = Vec::with_capacity(request.writes.len());
        for write in request.writes {
            results.push(apply_write(&mut pending, write, &commit_time)?);
        }
        crate::functions::documents_changed(&store.documents, &pending);
        store.documents = pending;
        if !request.transaction.is_empty() {
            store.transactions.remove(&request.transaction);
        }
        let _ = self.changes.send(());
        Ok(Response::new(CommitResponse {
            write_results: results,
            commit_time: Some(commit_time),
        }))
    }

    async fn rollback(&self, request: Request<RollbackRequest>) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let mut store = self.store.lock().await;
        validate_transaction(&store, &request.database, &request.transaction)?;
        store.transactions.remove(&request.transaction);
        Ok(Response::new(()))
    }

    async fn batch_write(
        &self,
        request: Request<BatchWriteRequest>,
    ) -> Result<Response<BatchWriteResponse>, Status> {
        let request = request.into_inner();
        let write_time = timestamp_now();
        if self.persistence.is_some() {
            let (write_results, status, _) = self
                .persistent_writes(request.database, request.writes, true, write_time)
                .await?;
            return Ok(Response::new(BatchWriteResponse {
                write_results,
                status,
            }));
        }
        let mut store = self.store.lock().await;
        let before = store.documents.clone();
        let mut write_results = Vec::with_capacity(request.writes.len());
        let mut statuses = Vec::with_capacity(request.writes.len());
        for write in request.writes {
            match apply_write(&mut store.documents, write, &write_time) {
                Ok(result) => {
                    write_results.push(result);
                    statuses.push(RpcStatus::default());
                }
                Err(status) => {
                    write_results.push(WriteResult::default());
                    statuses.push(status_proto(&status));
                }
            }
        }
        crate::functions::documents_changed(&before, &store.documents);
        let _ = self.changes.send(());
        Ok(Response::new(BatchWriteResponse {
            write_results,
            status: statuses,
        }))
    }

    type RunQueryStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<RunQueryResponse, Status>> + Send>>;

    async fn run_query(
        &self,
        request: Request<RunQueryRequest>,
    ) -> Result<Response<Self::RunQueryStream>, Status> {
        let request = request.into_inner();
        let new_transaction = match request.consistency_selector.as_ref() {
            Some(run_query_request::ConsistencySelector::Transaction(transaction)) => {
                let database = database_from_resource(&request.parent)?;
                let store = self.store.lock().await;
                validate_transaction(&store, &database, transaction)?;
                None
            }
            Some(run_query_request::ConsistencySelector::NewTransaction(_)) => {
                let database = database_from_resource(&request.parent)?;
                let mut store = self.store.lock().await;
                Some(create_transaction(&mut store, database))
            }
            _ => None,
        };
        let query = match request.query_type {
            Some(run_query_request::QueryType::StructuredQuery(query)) => query,
            None => return Err(Status::invalid_argument("structured_query is required")),
        };
        let read_time = timestamp_now();
        let documents = self.query_docs(&request.parent, &query).await?;
        if documents.is_empty() {
            return Ok(Response::new(Box::pin(tokio_stream::iter(vec![Ok(
                RunQueryResponse {
                    document: None,
                    read_time: Some(read_time),
                    skipped_results: 0,
                    done: true,
                    transaction: new_transaction.unwrap_or_default(),
                },
            )]))));
        }
        let responses = documents
            .into_iter()
            .enumerate()
            .map(move |(index, document)| {
                Ok(RunQueryResponse {
                    document: Some(document),
                    transaction: if index == 0 {
                        new_transaction.clone().unwrap_or_default()
                    } else {
                        Vec::new()
                    },
                    read_time: Some(read_time.clone()),
                    skipped_results: 0,
                    done: false,
                })
            });
        Ok(Response::new(Box::pin(tokio_stream::iter(responses))))
    }
}

fn matches_collection(name: &str, parent: &str, collection_id: &str, descendants: bool) -> bool {
    let parent = parent.trim_end_matches('/');
    let Some(relative) = name
        .strip_prefix(parent)
        .and_then(|suffix| suffix.strip_prefix('/'))
    else {
        return false;
    };
    let parts: Vec<_> = relative.split('/').collect();
    if descendants {
        parts.len() >= 2 && parts.len() % 2 == 0 && parts[parts.len() - 2] == collection_id
    } else {
        parts.len() == 2 && parts[0] == collection_id
    }
}

#[tonic::async_trait]
impl FirestoreEmulator for FirestoreService {
    async fn clear_data(&self, request: Request<ClearDataRequest>) -> Result<Response<()>, Status> {
        self.clear_database(&request.into_inner().database).await?;
        Ok(Response::new(()))
    }
}

impl FirestoreService {
    async fn clear_database(&self, database: &str) -> Result<(), Status> {
        let prefix = database_document_prefix(database)?;
        let mut store = self.store.lock().await;
        if let Some(persistence) = &self.persistence {
            let database = database.to_owned();
            let durable_database = database.clone();
            persistence
                .write(move |connection| {
                    let transaction = connection.transaction()?;
                    transaction.execute(
                        "DELETE FROM firestore_documents WHERE database_name=?1",
                        params![durable_database],
                    )?;
                    transaction.commit()?;
                    Ok(())
                })
                .await
                .map_err(persistence_status)?;
            store
                .transactions
                .retain(|_, transaction_database| transaction_database != &database);
            let _ = self.changes.send(());
            return Ok(());
        }
        store.documents.retain(|name, _| !name.starts_with(&prefix));
        store
            .transactions
            .retain(|_, transaction_database| transaction_database != database);
        let _ = self.changes.send(());
        Ok(())
    }
}

pub async fn serve(
    addr: std::net::SocketAddr,
    persistence: Option<crate::persistence::Persistence>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let service = FirestoreService::new(persistence);
    let http_clear_service = service.clone();
    let grpc_routes = tonic::service::Routes::new(FirestoreServer::new(service.clone()))
        .add_service(FirestoreEmulatorServer::new(service.clone()))
        .into_axum_router();
    let routes = crate::firestore_web::router(service)
        .fallback_service(grpc_routes)
        .route(
            "/emulator/v1/projects/:project/databases/:database/documents",
            delete(move |Path((project, database)): Path<(String, String)>| {
                let service = http_clear_service.clone();
                async move {
                    // The route captures valid nonempty resource segments.
                    let resource = format!("projects/{project}/databases/{database}");
                    match service.clear_database(&resource).await {
                        Ok(()) => StatusCode::OK,
                        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
                    }
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum07::serve(listener, routes).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use google::firestore::v1::{structured_query, StructuredQuery, Value};
    use tokio_stream::StreamExt;

    const ROOT: &str = "projects/test/databases/(default)/documents";

    fn document(name: &str, value: i64) -> Document {
        Document {
            name: name.to_owned(),
            fields: std::collections::HashMap::from([(
                "value".to_owned(),
                Value {
                    value_type: Some(google::firestore::v1::value::ValueType::IntegerValue(value)),
                },
            )]),
            create_time: None,
            update_time: None,
        }
    }

    #[tokio::test]
    async fn create_get_update_delete_round_trip() {
        let service = FirestoreService::default();
        let created = service
            .create_document(Request::new(CreateDocumentRequest {
                parent: ROOT.to_owned(),
                collection_id: "cities".to_owned(),
                document_id: "bangkok".to_owned(),
                document: Some(document("", 1)),
                mask: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(created.name, format!("{ROOT}/cities/bangkok"));
        assert!(created.create_time.is_some());

        let fetched = service
            .get_document(Request::new(GetDocumentRequest {
                name: created.name.clone(),
                mask: None,
                consistency_selector: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(fetched.fields, created.fields);

        let updated = service
            .update_document(Request::new(UpdateDocumentRequest {
                document: Some(document(&created.name, 2)),
                update_mask: None,
                mask: None,
                current_document: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(updated.create_time, created.create_time);

        service
            .delete_document(Request::new(DeleteDocumentRequest {
                name: created.name.clone(),
                current_document: None,
            }))
            .await
            .unwrap();
        assert_eq!(
            service
                .get_document(Request::new(GetDocumentRequest {
                    name: created.name,
                    mask: None,
                    consistency_selector: None,
                }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
    }

    #[tokio::test]
    async fn commit_is_atomic_and_query_honors_limit() {
        let service = FirestoreService::default();
        let good_name = format!("{ROOT}/cities/a");
        let bad_name = "not/a/resource".to_owned();
        let failed = service
            .commit(Request::new(CommitRequest {
                database: "projects/test/databases/(default)".to_owned(),
                writes: vec![
                    Write {
                        operation: Some(write::Operation::Update(document(&good_name, 1))),
                        update_mask: None,
                        current_document: None,
                        update_transforms: Vec::new(),
                    },
                    Write {
                        operation: Some(write::Operation::Delete(bad_name)),
                        update_mask: None,
                        current_document: None,
                        update_transforms: Vec::new(),
                    },
                ],
                transaction: Vec::new(),
            }))
            .await;
        assert!(failed.is_err());
        assert!(service.store.lock().await.documents.is_empty());

        for id in ["a", "b", "c"] {
            service
                .create_document(Request::new(CreateDocumentRequest {
                    parent: ROOT.to_owned(),
                    collection_id: "cities".to_owned(),
                    document_id: id.to_owned(),
                    document: Some(document("", 1)),
                    mask: None,
                }))
                .await
                .unwrap();
        }
        let query = StructuredQuery {
            select: None,
            from: vec![structured_query::CollectionSelector {
                collection_id: "cities".to_owned(),
                all_descendants: false,
            }],
            r#where: None,
            order_by: Vec::new(),
            limit: Some(2),
            offset: 0,
            start_at: None,
            end_at: None,
        };
        let mut stream = service
            .run_query(Request::new(RunQueryRequest {
                parent: ROOT.to_owned(),
                query_type: Some(run_query_request::QueryType::StructuredQuery(query)),
                consistency_selector: None,
            }))
            .await
            .unwrap()
            .into_inner();
        let mut count = 0;
        while stream.next().await.is_some() {
            count += 1;
        }
        assert_eq!(count, 2);

        service
            .clear_data(Request::new(ClearDataRequest {
                database: "projects/test/databases/(default)".to_owned(),
            }))
            .await
            .unwrap();
        assert!(service.store.lock().await.documents.is_empty());
    }

    #[tokio::test]
    async fn transaction_token_is_nonempty_and_commit_consumes_it() {
        let service = FirestoreService::default();
        let database = "projects/test/databases/(default)";
        let token = service
            .begin_transaction(Request::new(BeginTransactionRequest {
                database: database.to_owned(),
                options: None,
            }))
            .await
            .unwrap()
            .into_inner()
            .transaction;
        assert!(!token.is_empty());

        let name = format!("{ROOT}/cities/transactional");
        service
            .commit(Request::new(CommitRequest {
                database: database.to_owned(),
                writes: vec![Write {
                    operation: Some(write::Operation::Update(document(&name, 10))),
                    update_mask: None,
                    current_document: None,
                    update_transforms: Vec::new(),
                }],
                transaction: token.clone(),
            }))
            .await
            .unwrap();
        assert_eq!(
            service.store.lock().await.documents[&name].fields["value"].value_type,
            Some(ValueType::IntegerValue(10))
        );

        let reused = service
            .commit(Request::new(CommitRequest {
                database: database.to_owned(),
                writes: Vec::new(),
                transaction: token,
            }))
            .await
            .unwrap_err();
        assert_eq!(reused.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn batch_get_can_start_a_transaction_for_lazy_sdk_transactions() {
        let service = FirestoreService::default();
        let database = "projects/test/databases/(default)";
        let name = format!("{ROOT}/cities/lazy-transaction");
        service
            .create_document(Request::new(CreateDocumentRequest {
                parent: ROOT.to_owned(),
                collection_id: "cities".to_owned(),
                document_id: "lazy-transaction".to_owned(),
                document: Some(document("", 1)),
                mask: None,
            }))
            .await
            .unwrap();

        let mut stream = service
            .batch_get_documents(Request::new(BatchGetDocumentsRequest {
                database: database.to_owned(),
                documents: vec![name.clone()],
                mask: None,
                consistency_selector: Some(
                    batch_get_documents_request::ConsistencySelector::NewTransaction(
                        Default::default(),
                    ),
                ),
            }))
            .await
            .unwrap()
            .into_inner();
        let response = stream.next().await.unwrap().unwrap();
        assert!(!response.transaction.is_empty());
        assert!(matches!(
            response.result,
            Some(batch_get_documents_response::Result::Found(_))
        ));

        service
            .commit(Request::new(CommitRequest {
                database: database.to_owned(),
                writes: vec![Write {
                    operation: Some(write::Operation::Update(document(&name, 10))),
                    update_mask: None,
                    current_document: None,
                    update_transforms: Vec::new(),
                }],
                transaction: response.transaction,
            }))
            .await
            .unwrap();
        assert_eq!(
            service.store.lock().await.documents[&name].fields["value"].value_type,
            Some(ValueType::IntegerValue(10))
        );
    }

    #[tokio::test]
    async fn rollback_validates_database_and_consumes_transaction_token() {
        let service = FirestoreService::default();
        let database = "projects/test/databases/(default)";
        let token = service
            .begin_transaction(Request::new(BeginTransactionRequest {
                database: database.to_owned(),
                options: None,
            }))
            .await
            .unwrap()
            .into_inner()
            .transaction;

        let wrong_database = service
            .rollback(Request::new(RollbackRequest {
                database: "projects/test/databases/other".to_owned(),
                transaction: token.clone(),
            }))
            .await
            .unwrap_err();
        assert_eq!(wrong_database.code(), tonic::Code::InvalidArgument);

        service
            .rollback(Request::new(RollbackRequest {
                database: database.to_owned(),
                transaction: token.clone(),
            }))
            .await
            .unwrap();
        let reused = service
            .rollback(Request::new(RollbackRequest {
                database: database.to_owned(),
                transaction: token,
            }))
            .await
            .unwrap_err();
        assert_eq!(reused.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn clear_data_is_scoped_to_one_database() {
        let service = FirestoreService::default();
        let other_root = "projects/test/databases/other/documents";
        for (root, id) in [(ROOT, "default-doc"), (other_root, "other-doc")] {
            service
                .create_document(Request::new(CreateDocumentRequest {
                    parent: root.to_owned(),
                    collection_id: "cities".to_owned(),
                    document_id: id.to_owned(),
                    document: Some(document("", 1)),
                    mask: None,
                }))
                .await
                .unwrap();
        }

        service
            .clear_data(Request::new(ClearDataRequest {
                database: "projects/test/databases/(default)".to_owned(),
            }))
            .await
            .unwrap();
        let store = service.store.lock().await;
        assert!(!store
            .documents
            .contains_key(&format!("{ROOT}/cities/default-doc")));
        assert!(store
            .documents
            .contains_key(&format!("{other_root}/cities/other-doc")));
    }

    #[test]
    fn descendant_query_matches_only_immediate_parent_collection() {
        assert!(matches_collection(
            &format!("{ROOT}/countries/thailand/cities/bangkok"),
            ROOT,
            "cities",
            true,
        ));
        assert!(!matches_collection(
            &format!("{ROOT}/cities/bangkok/districts/pathum-wan"),
            ROOT,
            "cities",
            true,
        ));
    }

    #[tokio::test]
    async fn batch_get_reports_found_and_missing_and_transforms_are_rejected() {
        let service = FirestoreService::default();
        let found_name = format!("{ROOT}/cities/found");
        service
            .create_document(Request::new(CreateDocumentRequest {
                parent: ROOT.to_owned(),
                collection_id: "cities".to_owned(),
                document_id: "found".to_owned(),
                document: Some(document("", 1)),
                mask: None,
            }))
            .await
            .unwrap();
        let missing_name = format!("{ROOT}/cities/missing");
        let mut stream = service
            .batch_get_documents(Request::new(BatchGetDocumentsRequest {
                database: "projects/test/databases/(default)".to_owned(),
                documents: vec![found_name.clone(), missing_name.clone()],
                mask: None,
                consistency_selector: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            stream.next().await.unwrap().unwrap().result,
            Some(batch_get_documents_response::Result::Found(_))
        ));
        assert!(matches!(
            stream.next().await.unwrap().unwrap().result,
            Some(batch_get_documents_response::Result::Missing(name)) if name == missing_name
        ));

        let rejected = service
            .commit(Request::new(CommitRequest {
                database: "projects/test/databases/(default)".to_owned(),
                writes: vec![Write {
                    operation: Some(write::Operation::Update(document(&found_name, 2))),
                    update_mask: None,
                    current_document: None,
                    update_transforms: vec![Default::default()],
                }],
                transaction: Vec::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn persistent_documents_and_auto_ids_survive_reopen_without_hydration() {
        let root = std::env::temp_dir().join(format!(
            "firebase-emu-firestore-restart-{}",
            uuid::Uuid::new_v4()
        ));
        let persistence = crate::persistence::Persistence::open(root.clone(), false)
            .await
            .unwrap();
        let service = FirestoreService::new(Some(persistence.clone()));
        let mut value = document("", i64::MAX);
        value.fields.insert(
            "bytes".into(),
            Value {
                value_type: Some(google::firestore::v1::value::ValueType::BytesValue(vec![
                    0, 255, 7,
                ])),
            },
        );
        let first = service
            .create_document(Request::new(CreateDocumentRequest {
                parent: ROOT.to_owned(),
                collection_id: "typed".to_owned(),
                document_id: String::new(),
                document: Some(value.clone()),
                mask: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(service.store.lock().await.documents.is_empty());
        drop(service);
        drop(persistence);

        let persistence = crate::persistence::Persistence::open(root.clone(), false)
            .await
            .unwrap();
        let service = FirestoreService::new(Some(persistence.clone()));
        let fetched = service
            .get_document(Request::new(GetDocumentRequest {
                name: first.name.clone(),
                mask: None,
                consistency_selector: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(fetched.fields, first.fields);
        let second = service
            .create_document(Request::new(CreateDocumentRequest {
                parent: ROOT.to_owned(),
                collection_id: "typed".to_owned(),
                document_id: String::new(),
                document: Some(value),
                mask: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_ne!(first.name, second.name);
        let queried = service
            .query_docs(
                ROOT,
                &StructuredQuery {
                    from: vec![structured_query::CollectionSelector {
                        collection_id: "typed".to_owned(),
                        all_descendants: false,
                    }],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(queried.len(), 2);
        drop(service);
        drop(persistence);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn persistent_commit_failure_is_atomic_and_clear_is_database_scoped() {
        let root = std::env::temp_dir().join(format!(
            "firebase-emu-firestore-atomic-{}",
            uuid::Uuid::new_v4()
        ));
        let persistence = crate::persistence::Persistence::open(root.clone(), true)
            .await
            .unwrap();
        let service = FirestoreService::new(Some(persistence.clone()));
        let mut changes = service.subscribe();
        let default_name = format!("{ROOT}/items/default");
        let other_name = "projects/test/databases/other/documents/items/other".to_owned();
        let result = service
            .commit(Request::new(CommitRequest {
                database: "projects/test/databases/(default)".to_owned(),
                writes: vec![
                    Write {
                        operation: Some(write::Operation::Update(document(&default_name, 1))),
                        ..Default::default()
                    },
                    Write {
                        operation: Some(write::Operation::Update(document(&other_name, 2))),
                        ..Default::default()
                    },
                ],
                transaction: Vec::new(),
            }))
            .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
        assert!(service.document(&default_name).await.unwrap().is_none());
        assert!(changes.try_recv().is_err());
        let outbox_count = persistence
            .read(|connection| {
                Ok(
                    connection.query_row("SELECT count(*) FROM event_outbox", [], |row| {
                        row.get::<_, i64>(0)
                    })?,
                )
            })
            .await
            .unwrap();
        assert_eq!(outbox_count, 0);

        for (database, name) in [
            ("projects/test/databases/(default)", default_name.clone()),
            ("projects/test/databases/other", other_name.clone()),
        ] {
            service
                .commit(Request::new(CommitRequest {
                    database: database.to_owned(),
                    writes: vec![Write {
                        operation: Some(write::Operation::Update(document(&name, 1))),
                        ..Default::default()
                    }],
                    transaction: Vec::new(),
                }))
                .await
                .unwrap();
        }
        service
            .clear_database("projects/test/databases/(default)")
            .await
            .unwrap();
        assert!(service.document(&default_name).await.unwrap().is_none());
        assert!(service.document(&other_name).await.unwrap().is_some());
        drop(service);
        drop(persistence);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn persistent_batch_get_uses_one_snapshot_during_atomic_commit() {
        let root = std::env::temp_dir().join(format!(
            "firebase-emu-firestore-batch-snapshot-{}",
            uuid::Uuid::new_v4()
        ));
        let persistence = crate::persistence::Persistence::open(root.clone(), false)
            .await
            .unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let mut service = FirestoreService::new(Some(persistence.clone()));
        service.batch_get_barrier = Some(barrier.clone());
        let first = format!("{ROOT}/items/a");
        let second = format!("{ROOT}/items/b");
        service
            .commit(Request::new(CommitRequest {
                database: "projects/test/databases/(default)".into(),
                writes: vec![
                    Write {
                        operation: Some(write::Operation::Update(document(&first, 1))),
                        ..Default::default()
                    },
                    Write {
                        operation: Some(write::Operation::Update(document(&second, 1))),
                        ..Default::default()
                    },
                ],
                transaction: Vec::new(),
            }))
            .await
            .unwrap();

        let reader = {
            let service = service.clone();
            let first = first.clone();
            let second = second.clone();
            tokio::spawn(async move {
                service
                    .batch_get_documents(Request::new(BatchGetDocumentsRequest {
                        database: "projects/test/databases/(default)".into(),
                        documents: vec![first, second],
                        mask: None,
                        consistency_selector: None,
                    }))
                    .await
                    .unwrap()
                    .into_inner()
                    .collect::<Vec<_>>()
                    .await
            })
        };
        tokio::task::spawn_blocking({
            let barrier = barrier.clone();
            move || barrier.wait()
        })
        .await
        .unwrap();

        service
            .commit(Request::new(CommitRequest {
                database: "projects/test/databases/(default)".into(),
                writes: vec![
                    Write {
                        operation: Some(write::Operation::Update(document(&first, 2))),
                        ..Default::default()
                    },
                    Write {
                        operation: Some(write::Operation::Update(document(&second, 2))),
                        ..Default::default()
                    },
                ],
                transaction: Vec::new(),
            }))
            .await
            .unwrap();
        tokio::task::spawn_blocking(move || barrier.wait())
            .await
            .unwrap();

        let responses = reader.await.unwrap();
        let values: Vec<i64> = responses
            .into_iter()
            .map(|response| match response.unwrap().result.unwrap() {
                batch_get_documents_response::Result::Found(document) => {
                    match document.fields["value"].value_type {
                        Some(google::firestore::v1::value::ValueType::IntegerValue(value)) => value,
                        _ => panic!("expected integer"),
                    }
                }
                _ => panic!("expected document"),
            })
            .collect();
        assert_eq!(values, vec![1, 1]);
        drop(service);
        drop(persistence);
        std::fs::remove_dir_all(root).unwrap();
    }
}

fn field_path(path: &str) -> Result<Vec<String>, Status> {
    let mut parts = Vec::new();
    let mut part = String::new();
    let mut quoted = false;
    let mut escape = false;
    for ch in path.chars() {
        if escape {
            part.push(ch);
            escape = false;
        } else if ch == '\\' && quoted {
            escape = true;
        } else if ch == '`' {
            quoted = !quoted;
        } else if ch == '.' && !quoted {
            if part.is_empty() {
                return Err(Status::invalid_argument("empty field path"));
            }
            parts.push(std::mem::take(&mut part));
        } else {
            part.push(ch);
        }
    }
    if quoted || escape || part.is_empty() {
        return Err(Status::invalid_argument("invalid field path"));
    }
    parts.push(part);
    Ok(parts)
}
fn nested_get<'a>(fields: &'a HashMap<String, Value>, parts: &[String]) -> Option<&'a Value> {
    let value = fields.get(&parts[0])?;
    if parts.len() == 1 {
        return Some(value);
    }
    match value.value_type.as_ref()? {
        ValueType::MapValue(map) => nested_get(&map.fields, &parts[1..]),
        _ => None,
    }
}
fn nested_set(fields: &mut HashMap<String, Value>, parts: &[String], value: Option<Value>) {
    if parts.len() == 1 {
        if let Some(value) = value {
            fields.insert(parts[0].clone(), value);
        } else {
            fields.remove(&parts[0]);
        }
        return;
    }
    if value.is_none() && !fields.contains_key(&parts[0]) {
        return;
    }
    let entry = fields.entry(parts[0].clone()).or_default();
    if !matches!(entry.value_type, Some(ValueType::MapValue(_))) {
        if value.is_none() {
            return;
        }
        entry.value_type = Some(ValueType::MapValue(MapValue::default()));
    }
    if let Some(ValueType::MapValue(map)) = &mut entry.value_type {
        nested_set(&mut map.fields, &parts[1..], value);
    }
}
fn merge_document(
    existing: Option<&Document>,
    mut incoming: Document,
    mask: Option<&DocumentMask>,
) -> Result<Document, Status> {
    if let Some(mask) = mask {
        let paths = mask
            .field_paths
            .iter()
            .map(|p| field_path(p))
            .collect::<Result<Vec<_>, _>>()?;
        for (i, a) in paths.iter().enumerate() {
            for b in paths.iter().skip(i + 1) {
                if a.starts_with(b) || b.starts_with(a) {
                    return Err(Status::invalid_argument("overlapping update mask paths"));
                }
            }
        }
        let mut fields = existing.map(|d| d.fields.clone()).unwrap_or_default();
        for path in paths {
            nested_set(
                &mut fields,
                &path,
                nested_get(&incoming.fields, &path).cloned(),
            );
        }
        incoming.fields = fields;
    }
    Ok(incoming)
}
fn field_value(doc: &Document, path: &str) -> Option<Value> {
    if path == "__name__" {
        return Some(Value {
            value_type: Some(ValueType::ReferenceValue(doc.name.clone())),
        });
    }
    nested_get(&doc.fields, &field_path(path).ok()?).cloned()
}
fn value_rank(v: &Value) -> u8 {
    match v.value_type {
        None | Some(ValueType::NullValue(_)) => 0,
        Some(ValueType::BooleanValue(_)) => 1,
        Some(ValueType::IntegerValue(_) | ValueType::DoubleValue(_)) => 2,
        Some(ValueType::TimestampValue(_)) => 3,
        Some(ValueType::StringValue(_)) => 4,
        Some(ValueType::BytesValue(_)) => 5,
        Some(ValueType::ReferenceValue(_)) => 6,
        Some(ValueType::GeoPointValue(_)) => 7,
        Some(ValueType::ArrayValue(_)) => 8,
        Some(ValueType::MapValue(_)) => 9,
    }
}
fn compare_integer_double(integer: i64, double: f64) -> Ordering {
    if double.is_nan() || double < i64::MIN as f64 {
        return Ordering::Greater;
    }
    if double >= i64::MAX as f64 {
        return Ordering::Less;
    }
    integer
        .cmp(&(double as i64))
        .then_with(|| 0.0_f64.partial_cmp(&double.fract()).unwrap())
}
fn compare(a: &Value, b: &Value) -> Ordering {
    use ValueType::*;
    let rank = value_rank(a).cmp(&value_rank(b));
    if rank != Ordering::Equal {
        return rank;
    }
    let floats = |a: f64, b: f64| {
        if a.is_nan() {
            if b.is_nan() {
                Ordering::Equal
            } else {
                Ordering::Less
            }
        } else if b.is_nan() {
            Ordering::Greater
        } else {
            a.partial_cmp(&b).unwrap()
        }
    };
    match (a.value_type.as_ref(), b.value_type.as_ref()) {
        (Some(IntegerValue(a)), Some(IntegerValue(b))) => a.cmp(b),
        (Some(IntegerValue(a)), Some(DoubleValue(b))) => compare_integer_double(*a, *b),
        (Some(DoubleValue(a)), Some(IntegerValue(b))) => compare_integer_double(*b, *a).reverse(),
        (Some(DoubleValue(a)), Some(DoubleValue(b))) => floats(*a, *b),
        (Some(BooleanValue(a)), Some(BooleanValue(b))) => a.cmp(b),
        (Some(StringValue(a)), Some(StringValue(b)))
        | (Some(ReferenceValue(a)), Some(ReferenceValue(b))) => a.cmp(b),
        (Some(BytesValue(a)), Some(BytesValue(b))) => a.cmp(b),
        (Some(TimestampValue(a)), Some(TimestampValue(b))) => {
            (a.seconds, a.nanos).cmp(&(b.seconds, b.nanos))
        }
        (Some(GeoPointValue(a)), Some(GeoPointValue(b))) => {
            floats(a.latitude, b.latitude).then_with(|| floats(a.longitude, b.longitude))
        }
        (Some(ArrayValue(a)), Some(ArrayValue(b))) => a
            .values
            .iter()
            .zip(&b.values)
            .map(|(a, b)| compare(a, b))
            .find(|o| *o != Ordering::Equal)
            .unwrap_or_else(|| a.values.len().cmp(&b.values.len())),
        (Some(MapValue(a)), Some(MapValue(b))) => {
            let a: BTreeMap<_, _> = a.fields.iter().collect();
            let b: BTreeMap<_, _> = b.fields.iter().collect();
            a.iter()
                .zip(&b)
                .map(|((ka, va), (kb, vb))| ka.cmp(kb).then_with(|| compare(va, vb)))
                .find(|o| *o != Ordering::Equal)
                .unwrap_or_else(|| a.len().cmp(&b.len()))
        }
        _ => Ordering::Equal,
    }
}
fn filter_matches(doc: &Document, filter: &structured_query::Filter) -> Result<bool, Status> {
    use structured_query::filter::FilterType;
    match filter
        .filter_type
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("filter is required"))?
    {
        FilterType::CompositeFilter(f) => {
            let values = f
                .filters
                .iter()
                .map(|f| filter_matches(doc, f))
                .collect::<Result<Vec<_>, _>>()?;
            match f.op {
                1 => Ok(values.iter().all(|v| *v)),
                2 => Ok(values.iter().any(|v| *v)),
                _ => Err(Status::invalid_argument("invalid composite operator")),
            }
        }
        FilterType::FieldFilter(f) => {
            let field = f
                .field
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("filter field is required"))?;
            let b = f
                .value
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("filter value is required"))?;
            let Some(a) = field_value(doc, &field.field_path) else {
                return Ok(false);
            };
            let cmp = compare(&a, b);
            let same = value_rank(&a) == value_rank(b);
            let contains = |array: &Value, needle: &Value| match &array.value_type {
                Some(ValueType::ArrayValue(v)) => v
                    .values
                    .iter()
                    .any(|v| compare(v, needle) == Ordering::Equal),
                _ => false,
            };
            Ok(match f.op {
                1 => same && cmp.is_lt(),
                2 => same && !cmp.is_gt(),
                3 => same && cmp.is_gt(),
                4 => same && !cmp.is_lt(),
                5 => cmp.is_eq(),
                6 => !cmp.is_eq(),
                7 => contains(&a, b),
                8 => contains(b, &a),
                9 => match &a.value_type {
                    Some(ValueType::ArrayValue(v)) => v.values.iter().any(|v| contains(b, v)),
                    _ => false,
                },
                10 => {
                    value_rank(&a) != 0
                        && !contains(
                            b,
                            &Value {
                                value_type: Some(ValueType::NullValue(0)),
                            },
                        )
                        && !contains(b, &a)
                }
                _ => return Err(Status::invalid_argument("invalid field operator")),
            })
        }
        FilterType::UnaryFilter(f) => {
            let Some(structured_query::unary_filter::OperandType::Field(field)) = &f.operand_type
            else {
                return Err(Status::invalid_argument("unary field is required"));
            };
            let Some(a) = field_value(doc, &field.field_path) else {
                return Ok(false);
            };
            let nan = matches!(a.value_type,Some(ValueType::DoubleValue(v)) if v.is_nan());
            let null = value_rank(&a) == 0;
            match f.op {
                2 => Ok(nan),
                3 => Ok(null),
                4 => Ok(!nan),
                5 => Ok(!null),
                _ => Err(Status::invalid_argument("invalid unary operator")),
            }
        }
    }
}
fn inequality_fields(
    filter: &structured_query::Filter,
    fields: &mut std::collections::BTreeSet<String>,
) {
    use structured_query::filter::FilterType;
    match &filter.filter_type {
        Some(FilterType::CompositeFilter(f)) => {
            for f in &f.filters {
                inequality_fields(f, fields);
            }
        }
        Some(FilterType::FieldFilter(f)) if matches!(f.op, 1 | 2 | 3 | 4 | 6 | 10) => {
            if let Some(field) = &f.field {
                fields.insert(field.field_path.clone());
            }
        }
        Some(FilterType::UnaryFilter(f)) if matches!(f.op, 4 | 5) => {
            if let Some(structured_query::unary_filter::OperandType::Field(field)) = &f.operand_type
            {
                fields.insert(field.field_path.clone());
            }
        }
        _ => {}
    }
}
impl FirestoreService {
    pub(crate) fn subscribe(&self) -> broadcast::Receiver<()> {
        self.changes.subscribe()
    }
    pub(crate) async fn query_docs(
        &self,
        parent: &str,
        query: &StructuredQuery,
    ) -> Result<Vec<Document>, Status> {
        if query.from.len() != 1 || query.from[0].collection_id.is_empty() {
            return Err(Status::invalid_argument("one collection is required"));
        }
        if query.offset < 0 || query.limit.is_some_and(|v| v < 0) {
            return Err(Status::invalid_argument("negative query bounds"));
        }
        let selector = &query.from[0];
        let database = database_from_resource(parent)?;
        let persistent_documents = if self.persistence.is_some() {
            Some(self.database_documents(&database).await?)
        } else {
            None
        };
        let store = self.store.lock().await;
        let mut docs = Vec::new();
        let documents: Box<dyn Iterator<Item = &Document> + '_> =
            if let Some(documents) = persistent_documents.as_ref() {
                Box::new(documents.iter())
            } else {
                Box::new(store.documents.values())
            };
        for doc in documents {
            if matches_collection(
                &doc.name,
                parent,
                &selector.collection_id,
                selector.all_descendants,
            ) && match &query.r#where {
                Some(f) => filter_matches(doc, f)?,
                None => true,
            } {
                docs.push(doc.clone());
            }
        }
        drop(store);
        let mut orders = query
            .order_by
            .iter()
            .map(|o| {
                Ok((
                    o.field
                        .as_ref()
                        .ok_or_else(|| Status::invalid_argument("order field required"))?
                        .field_path
                        .clone(),
                    o.direction == 2,
                ))
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let mut inequalities = std::collections::BTreeSet::new();
        if let Some(filter) = &query.r#where {
            inequality_fields(filter, &mut inequalities);
        }
        let descending = orders.last().map(|(_, d)| *d).unwrap_or(false);
        for field in inequalities {
            if field != "__name__" && !orders.iter().any(|(p, _)| p == &field) {
                orders.push((field, descending));
            }
        }
        if !orders.iter().any(|(p, _)| p == "__name__") {
            orders.push((
                "__name__".into(),
                orders.last().map(|(_, d)| *d).unwrap_or(false),
            ));
        }
        docs.retain(|d| orders.iter().all(|(p, _)| field_value(d, p).is_some()));
        docs.sort_by(|a, b| {
            orders
                .iter()
                .map(|(p, desc)| {
                    let c = compare(&field_value(a, p).unwrap(), &field_value(b, p).unwrap());
                    if *desc {
                        c.reverse()
                    } else {
                        c
                    }
                })
                .find(|o| !o.is_eq())
                .unwrap_or(Ordering::Equal)
        });
        for (cursor, start) in [(&query.start_at, true), (&query.end_at, false)] {
            if let Some(cursor) = cursor {
                if cursor.values.len() > orders.len() {
                    return Err(Status::invalid_argument("too many cursor values"));
                }
                docs.retain(|doc| {
                    let cmp = orders
                        .iter()
                        .zip(&cursor.values)
                        .map(|((p, desc), v)| {
                            let c = compare(&field_value(doc, p).unwrap(), v);
                            if *desc {
                                c.reverse()
                            } else {
                                c
                            }
                        })
                        .find(|o| !o.is_eq())
                        .unwrap_or(Ordering::Equal);
                    if start {
                        cmp.is_gt() || (cmp.is_eq() && cursor.before)
                    } else {
                        cmp.is_lt() || (cmp.is_eq() && !cursor.before)
                    }
                });
            }
        }
        let mut docs: Vec<_> = docs
            .into_iter()
            .skip(query.offset as usize)
            .take(query.limit.unwrap_or(i32::MAX) as usize)
            .collect();
        if let Some(select) = &query.select {
            for doc in &mut docs {
                let mut fields = HashMap::new();
                for field in &select.fields {
                    if field.field_path != "__name__" {
                        let path = field_path(&field.field_path)?;
                        nested_set(&mut fields, &path, nested_get(&doc.fields, &path).cloned());
                    }
                }
                doc.fields = fields;
            }
        }
        Ok(docs)
    }
    async fn target_docs(&self, target: &Target) -> Result<Vec<Document>, Status> {
        match target.target_type.as_ref() {
            Some(target::TargetType::Documents(d)) => self.documents(&d.documents).await,
            Some(target::TargetType::Query(q)) => match &q.query_type {
                Some(target::query_target::QueryType::StructuredQuery(query)) => {
                    self.query_docs(&q.parent, query).await
                }
                None => Err(Status::invalid_argument("query required")),
            },
            None => Err(Status::invalid_argument("target required")),
        }
    }
}
fn target_response(kind: i32, ids: Vec<i32>, read_time: bool) -> ListenResponse {
    ListenResponse {
        response_type: Some(listen_response::ResponseType::TargetChange(TargetChange {
            target_change_type: kind,
            target_ids: ids,
            cause: None,
            resume_token: vec![],
            read_time: if read_time {
                Some(timestamp_now())
            } else {
                None
            },
        })),
    }
}
async fn emit_target(
    service: &FirestoreService,
    tx: &mpsc::Sender<Result<ListenResponse, Status>>,
    target: &Target,
    previous: &mut BTreeMap<String, Document>,
    initial: bool,
) -> Result<(), Status> {
    let current: BTreeMap<_, _> = service
        .target_docs(target)
        .await?
        .into_iter()
        .map(|d| (d.name.clone(), d))
        .collect();
    for name in previous.keys().filter(|n| !current.contains_key(*n)) {
        tx.send(Ok(ListenResponse {
            response_type: Some(listen_response::ResponseType::DocumentRemove(
                DocumentRemove {
                    document: name.clone(),
                    removed_target_ids: vec![target.target_id],
                    read_time: Some(timestamp_now()),
                },
            )),
        }))
        .await
        .map_err(|_| Status::cancelled("listener closed"))?;
    }
    for (name, doc) in &current {
        if previous.get(name) != Some(doc) {
            tx.send(Ok(ListenResponse {
                response_type: Some(listen_response::ResponseType::DocumentChange(
                    DocumentChange {
                        document: Some(doc.clone()),
                        target_ids: vec![target.target_id],
                        removed_target_ids: vec![],
                    },
                )),
            }))
            .await
            .map_err(|_| Status::cancelled("listener closed"))?;
        }
    }
    *previous = current;
    if initial {
        tx.send(Ok(target_response(3, vec![target.target_id], false)))
            .await
            .map_err(|_| Status::cancelled("listener closed"))?;
    }
    tx.send(Ok(target_response(0, vec![], true)))
        .await
        .map_err(|_| Status::cancelled("listener closed"))?;
    Ok(())
}

#[cfg(test)]
mod semantic_tests {
    use super::*;
    const ROOT: &str = "projects/test/databases/(default)/documents";
    fn int(v: i64) -> Value {
        Value {
            value_type: Some(ValueType::IntegerValue(v)),
        }
    }
    fn string(v: &str) -> Value {
        Value {
            value_type: Some(ValueType::StringValue(v.into())),
        }
    }
    fn array(values: Vec<Value>) -> Value {
        Value {
            value_type: Some(ValueType::ArrayValue(ArrayValue { values })),
        }
    }
    fn map(fields: &[(&str, Value)]) -> Value {
        Value {
            value_type: Some(ValueType::MapValue(MapValue {
                fields: fields
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.clone()))
                    .collect(),
            })),
        }
    }
    fn doc(id: &str, fields: &[(&str, Value)]) -> Document {
        Document {
            name: format!("{ROOT}/items/{id}"),
            fields: fields
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            ..Default::default()
        }
    }
    fn filter(path: &str, op: i32, value: Value) -> structured_query::Filter {
        structured_query::Filter {
            filter_type: Some(structured_query::filter::FilterType::FieldFilter(
                structured_query::FieldFilter {
                    field: Some(structured_query::FieldReference {
                        field_path: path.into(),
                    }),
                    op,
                    value: Some(value),
                },
            )),
        }
    }

    #[test]
    fn commit_update_transforms_preserve_masked_and_nested_fields() {
        use document_transform::field_transform::TransformType;
        let name = format!("{ROOT}/items/transforms");
        let mut documents = BTreeMap::from([(
            name.clone(),
            doc(
                "transforms",
                &[
                    ("rank", int(1)),
                    ("tags", array(vec![string("seed")])),
                    ("obsolete", string("remove")),
                    ("preserved", string("yes")),
                    ("nested", map(&[("left", int(1)), ("right", int(2))])),
                ],
            ),
        )]);
        let request_time = prost_types::Timestamp {
            seconds: 1_750_000_000,
            nanos: 123_000_000,
        };
        let transform = |field_path: &str, transform_type| document_transform::FieldTransform {
            field_path: field_path.into(),
            transform_type: Some(transform_type),
        };
        let result = apply_write(
            &mut documents,
            Write {
                operation: Some(write::Operation::Update(doc(
                    "transforms",
                    &[("nested", map(&[("left", int(9))]))],
                ))),
                update_mask: Some(DocumentMask {
                    field_paths: vec!["nested.left".into(), "obsolete".into()],
                }),
                current_document: None,
                update_transforms: vec![
                    transform("rank", TransformType::Increment(int(4))),
                    transform(
                        "tags",
                        TransformType::AppendMissingElements(ArrayValue {
                            values: vec![string("updated"), string("updated")],
                        }),
                    ),
                    transform(
                        "updatedAt",
                        TransformType::SetToServerValue(ServerValue::RequestTime as i32),
                    ),
                ],
            },
            &request_time,
        )
        .unwrap();

        let written = documents.get(&name).unwrap();
        assert_eq!(field_value(written, "rank"), Some(int(5)));
        assert_eq!(
            field_value(written, "tags"),
            Some(array(vec![string("seed"), string("updated")]))
        );
        assert!(field_value(written, "obsolete").is_none());
        assert_eq!(field_value(written, "preserved"), Some(string("yes")));
        assert_eq!(field_value(written, "nested.left"), Some(int(9)));
        assert_eq!(field_value(written, "nested.right"), Some(int(2)));
        assert_eq!(
            field_value(written, "updatedAt"),
            Some(Value {
                value_type: Some(ValueType::TimestampValue(request_time.clone())),
            })
        );
        assert_eq!(result.transform_results.len(), 3);
    }

    #[test]
    fn transform_numeric_types_and_array_equality_are_meaningful() {
        let mixed = numeric_increment(
            Some(&int(2)),
            &Value {
                value_type: Some(ValueType::DoubleValue(0.5)),
            },
        )
        .unwrap();
        assert_eq!(mixed.value_type, Some(ValueType::DoubleValue(2.5)));
        assert_eq!(
            numeric_increment(Some(&int(i64::MAX)), &int(1))
                .unwrap()
                .value_type,
            Some(ValueType::IntegerValue(i64::MAX))
        );
        assert!(transform_value_equal(
            &int(1),
            &Value {
                value_type: Some(ValueType::DoubleValue(1.0)),
            }
        ));
        assert!(transform_value_equal(
            &Value {
                value_type: Some(ValueType::DoubleValue(f64::NAN)),
            },
            &Value {
                value_type: Some(ValueType::DoubleValue(f64::NAN)),
            }
        ));
    }
    fn query() -> StructuredQuery {
        StructuredQuery {
            from: vec![structured_query::CollectionSelector {
                collection_id: "items".into(),
                all_descendants: false,
            }],
            ..Default::default()
        }
    }
    #[test]
    fn masks_merge_nested_delete_and_replace_maps() {
        let old = doc(
            "a",
            &[
                ("keep", int(1)),
                ("nested", map(&[("a", int(2)), ("b", int(3))])),
                ("literal.dot", int(4)),
            ],
        );
        let patch = doc("a", &[("nested", map(&[("a", int(9))]))]);
        let result = merge_document(
            Some(&old),
            patch.clone(),
            Some(&DocumentMask {
                field_paths: vec!["nested.a".into(), "`literal.dot`".into()],
            }),
        )
        .unwrap();
        assert_eq!(field_value(&result, "nested.a"), Some(int(9)));
        assert_eq!(field_value(&result, "nested.b"), Some(int(3)));
        assert_eq!(field_value(&result, "keep"), Some(int(1)));
        assert!(field_value(&result, "`literal.dot`").is_none());
        let replaced = merge_document(
            Some(&old),
            patch.clone(),
            Some(&DocumentMask {
                field_paths: vec!["nested".into()],
            }),
        )
        .unwrap();
        assert!(field_value(&replaced, "nested.b").is_none());
        assert!(merge_document(
            Some(&old),
            patch,
            Some(&DocumentMask {
                field_paths: vec!["nested".into(), "nested.a".into()]
            })
        )
        .is_err());
    }
    #[tokio::test]
    async fn composite_query_orders_before_offset_and_limit() {
        let service = FirestoreService::default();
        for (id, rank, active) in [("a", 7, 1), ("b", 2, 1), ("c", 9, 0), ("d", 5, 1)] {
            let d = doc(id, &[("rank", int(rank)), ("active", int(active))]);
            service
                .store
                .lock()
                .await
                .documents
                .insert(d.name.clone(), d);
        }
        let mut q = query();
        q.r#where = Some(structured_query::Filter {
            filter_type: Some(structured_query::filter::FilterType::CompositeFilter(
                structured_query::CompositeFilter {
                    op: 1,
                    filters: vec![filter("rank", 3, int(1)), filter("active", 5, int(1))],
                },
            )),
        });
        q.order_by = vec![structured_query::Order {
            field: Some(structured_query::FieldReference {
                field_path: "rank".into(),
            }),
            direction: 2,
        }];
        q.offset = 1;
        q.limit = Some(1);
        assert_eq!(
            service.query_docs(ROOT, &q).await.unwrap()[0].name,
            format!("{ROOT}/items/d")
        );
        q.offset = 0;
        q.limit = None;
        q.r#where = Some(structured_query::Filter {
            filter_type: Some(structured_query::filter::FilterType::CompositeFilter(
                structured_query::CompositeFilter {
                    op: 2,
                    filters: vec![filter("rank", 5, int(2)), filter("rank", 5, int(9))],
                },
            )),
        });
        assert_eq!(
            service
                .query_docs(ROOT, &q)
                .await
                .unwrap()
                .iter()
                .map(|d| field_value(d, "rank").unwrap())
                .collect::<Vec<_>>(),
            vec![int(9), int(2)]
        );
    }
    #[tokio::test]
    async fn query_excludes_missing_order_fields_and_applies_cursor() {
        let service = FirestoreService::default();
        for d in [
            doc("a", &[("rank", int(1))]),
            doc("b", &[("rank", int(2))]),
            doc("c", &[]),
        ] {
            service
                .store
                .lock()
                .await
                .documents
                .insert(d.name.clone(), d);
        }
        let mut q = query();
        q.order_by = vec![structured_query::Order {
            field: Some(structured_query::FieldReference {
                field_path: "rank".into(),
            }),
            direction: 1,
        }];
        q.start_at = Some(structured_query::Cursor {
            values: vec![int(1)],
            before: false,
        });
        assert_eq!(
            service
                .query_docs(ROOT, &q)
                .await
                .unwrap()
                .iter()
                .map(|d| d.name.clone())
                .collect::<Vec<_>>(),
            vec![format!("{ROOT}/items/b")]
        );
    }
    #[tokio::test]
    async fn listener_emits_query_membership_removal_after_limit_change() {
        let service = FirestoreService::default();
        let a = doc("a", &[("rank", int(1))]);
        service
            .store
            .lock()
            .await
            .documents
            .insert(a.name.clone(), a);
        let mut q = query();
        q.limit = Some(1);
        let target = Target {
            target_id: 2,
            target_type: Some(target::TargetType::Query(target::QueryTarget {
                parent: ROOT.into(),
                query_type: Some(target::query_target::QueryType::StructuredQuery(q)),
            })),
            ..Default::default()
        };
        let (tx, mut rx) = mpsc::channel(32);
        let mut previous = BTreeMap::new();
        emit_target(&service, &tx, &target, &mut previous, true)
            .await
            .unwrap();
        assert!(matches!(
            rx.recv().await.unwrap().unwrap().response_type,
            Some(listen_response::ResponseType::DocumentChange(_))
        ));
        rx.recv().await;
        rx.recv().await;
        service.store.lock().await.documents.clear();
        emit_target(&service, &tx, &target, &mut previous, false)
            .await
            .unwrap();
        assert!(matches!(
            rx.recv().await.unwrap().unwrap().response_type,
            Some(listen_response::ResponseType::DocumentRemove(_))
        ));
    }
    #[tokio::test]
    async fn empty_query_has_a_read_time_response() {
        use tokio_stream::StreamExt;
        let service = FirestoreService::default();
        let mut stream = service
            .run_query(Request::new(RunQueryRequest {
                parent: ROOT.into(),
                query_type: Some(run_query_request::QueryType::StructuredQuery(query())),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        let response = stream.next().await.unwrap().unwrap();
        assert!(response.document.is_none());
        assert!(response.read_time.is_some());
        assert!(stream.next().await.is_none());
    }
    #[tokio::test]
    async fn writes_and_reset_notify_shared_subscribers() {
        let service = FirestoreService::default();
        let mut changes = service.subscribe();
        let d = doc("a", &[("rank", int(1))]);
        service
            .commit(Request::new(CommitRequest {
                database: "projects/test/databases/(default)".into(),
                writes: vec![Write {
                    operation: Some(write::Operation::Update(d)),
                    ..Default::default()
                }],
                ..Default::default()
            }))
            .await
            .unwrap();
        assert!(changes.try_recv().is_ok());
        service
            .clear_database("projects/test/databases/(default)")
            .await
            .unwrap();
        assert!(changes.try_recv().is_ok());
    }
}
