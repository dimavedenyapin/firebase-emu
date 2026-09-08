use fs2::FileExt;
use rusqlite::{Connection, OpenFlags, Transaction};
use std::{
    any::Any,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, Semaphore};

pub(crate) const SCHEMA_VERSION: i64 = 2;
const APPLICATION_ID: i64 = 0x4645_4d55; // "FEMU"
const WRITER_QUEUE_CAPACITY: usize = 128;
const MAX_READERS: usize = 8;

type DynamicResult = Result<Box<dyn Any + Send>, Error>;
type Job = Box<dyn FnOnce(&mut Connection) -> DynamicResult + Send>;

struct WriteRequest {
    job: Job,
    response: oneshot::Sender<DynamicResult>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("persistence I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("SQLite persistence error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("data directory is already owned by another firebase-emu process: {0}")]
    AlreadyOwned(PathBuf),
    #[error(
        "unsupported persistence schema version {found}; this binary supports up to {supported}"
    )]
    NewerSchema { found: i64, supported: i64 },
    #[error("persistence database failed integrity_check: {0}")]
    Corrupt(String),
    #[error("refusing to initialize a non-empty SQLite database without a firebase-emu schema")]
    ForeignDatabase,
    #[error("persistence writer stopped unexpectedly")]
    WriterStopped,
    #[error("persistence worker returned an unexpected result type")]
    ResultType,
    #[error("persistence blocking task failed: {0}")]
    Join(String),
    #[error("durable Functions outbox is full ({0} pending or in-flight events)")]
    OutboxFull(i64),
}

#[derive(Clone, Debug)]
pub(crate) struct OutboxRecord {
    pub source: String,
    pub payload: Vec<u8>,
}

struct Inner {
    root: PathBuf,
    database: PathBuf,
    blobs: PathBuf,
    temporary: PathBuf,
    writer: mpsc::Sender<WriteRequest>,
    readers: Arc<Semaphore>,
    events_enabled: bool,
    _owner_lock: File,
}

#[derive(Clone)]
pub(crate) struct Persistence(Arc<Inner>);

impl std::fmt::Debug for Persistence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Persistence")
            .field("root", &self.0.root)
            .finish_non_exhaustive()
    }
}

impl Persistence {
    pub(crate) async fn open(path: PathBuf, events_enabled: bool) -> Result<Self, Error> {
        tokio::task::spawn_blocking(move || Self::open_blocking(&path, events_enabled))
            .await
            .map_err(|error| Error::Join(error.to_string()))?
    }

    fn open_blocking(path: &Path, events_enabled: bool) -> Result<Self, Error> {
        fs::create_dir_all(path)?;
        let root = fs::canonicalize(path)?;
        let lock_path = root.join(".firebase-emu.lock");
        let owner_lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        owner_lock
            .try_lock_exclusive()
            .map_err(|_| Error::AlreadyOwned(root.clone()))?;

        let blobs = root.join("blobs");
        let temporary = root.join("tmp");
        fs::create_dir_all(&blobs)?;
        fs::create_dir_all(&temporary)?;
        let database = root.join("firebase-emu.sqlite3");
        let mut connection = open_connection(&database)?;
        migrate(&mut connection)?;

        let (writer, mut receiver) = mpsc::channel::<WriteRequest>(WRITER_QUEUE_CAPACITY);
        std::thread::Builder::new()
            .name("firebase-emu-sqlite-writer".into())
            .spawn(move || {
                let mut connection = connection;
                while let Some(request) = receiver.blocking_recv() {
                    let result = (request.job)(&mut connection);
                    let _ = request.response.send(result);
                }
            })?;

        Ok(Self(Arc::new(Inner {
            root,
            database,
            blobs,
            temporary,
            writer,
            readers: Arc::new(Semaphore::new(MAX_READERS)),
            events_enabled,
            _owner_lock: owner_lock,
        })))
    }

    pub(crate) fn database_path(&self) -> &Path {
        &self.0.database
    }

    pub(crate) fn blobs_dir(&self) -> &Path {
        &self.0.blobs
    }

    pub(crate) fn temporary_dir(&self) -> &Path {
        &self.0.temporary
    }

    pub(crate) fn events_enabled(&self) -> bool {
        self.0.events_enabled
    }

    pub(crate) async fn write<R, F>(&self, operation: F) -> Result<R, Error>
    where
        R: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<R, Error> + Send + 'static,
    {
        let (response, result) = oneshot::channel();
        let job = Box::new(move |connection: &mut Connection| {
            operation(connection).map(|value| Box::new(value) as Box<dyn Any + Send>)
        });
        self.0
            .writer
            .send(WriteRequest { job, response })
            .await
            .map_err(|_| Error::WriterStopped)?;
        result
            .await
            .map_err(|_| Error::WriterStopped)??
            .downcast::<R>()
            .map(|value| *value)
            .map_err(|_| Error::ResultType)
    }

    pub(crate) async fn read<R, F>(&self, operation: F) -> Result<R, Error>
    where
        R: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<R, Error> + Send + 'static,
    {
        let inner = self.0.clone();
        let permit = inner
            .readers
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::WriterStopped)?;
        let database = inner.database.clone();
        let result = tokio::task::spawn_blocking(move || {
            // The owned permit and Inner stay in the blocking job even if the
            // async caller is cancelled. That keeps both the eight-reader
            // bound and the data-directory ownership lock valid until the
            // SQLite work has actually stopped.
            let _permit = permit;
            let _inner = inner;
            let mut connection = open_read_connection(&database)?;
            operation(&mut connection)
        })
        .await
        .map_err(|error| Error::Join(error.to_string()))?;
        result
    }

    pub(crate) fn insert_outbox(
        &self,
        transaction: &Transaction<'_>,
        records: &[OutboxRecord],
    ) -> Result<Vec<i64>, Error> {
        if !self.events_enabled() {
            return Ok(Vec::new());
        }
        let outstanding: i64 = transaction.query_row(
            "SELECT count(*) FROM event_outbox WHERE state IN ('pending','in_flight')",
            [],
            |row| row.get(0),
        )?;
        if outstanding.saturating_add(records.len() as i64) > 4096 {
            return Err(Error::OutboxFull(outstanding));
        }
        let mut ids = Vec::with_capacity(records.len());
        for record in records {
            transaction.execute(
                "INSERT INTO event_outbox(source, payload, state, attempts, available_at) \
                 VALUES (?1, ?2, 'pending', 0, 0)",
                rusqlite::params![record.source, record.payload],
            )?;
            ids.push(transaction.last_insert_rowid());
        }
        Ok(ids)
    }
}

fn configure(connection: &Connection) -> Result<(), Error> {
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.execute_batch(
        "PRAGMA foreign_keys=ON;\
         PRAGMA synchronous=FULL;\
         PRAGMA wal_autocheckpoint=1000;\
         PRAGMA cache_size=-8192;",
    )?;
    Ok(())
}

fn open_connection(path: &Path) -> Result<Connection, Error> {
    let connection = Connection::open(path)?;
    configure(&connection)?;
    let mode: String = connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(Error::Corrupt(format!(
            "could not enable WAL mode (SQLite returned {mode})"
        )));
    }
    Ok(connection)
}

fn open_read_connection(path: &Path) -> Result<Connection, Error> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.execute_batch("PRAGMA query_only=ON; PRAGMA cache_size=-8192;")?;
    Ok(connection)
}

fn migrate(connection: &mut Connection) -> Result<(), Error> {
    let integrity: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        return Err(Error::Corrupt(integrity));
    }
    let mut version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version > SCHEMA_VERSION {
        return Err(Error::NewerSchema {
            found: version,
            supported: SCHEMA_VERSION,
        });
    }
    if version == 0 {
        let application_id: i64 =
            connection.query_row("PRAGMA application_id", [], |row| row.get(0))?;
        let table_count: i64 = connection.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )?;
        if table_count != 0 || (application_id != 0 && application_id != APPLICATION_ID) {
            return Err(Error::ForeignDatabase);
        }
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE firestore_documents(\
                 name TEXT PRIMARY KEY NOT NULL,\
                 database_name TEXT NOT NULL,\
                 document BLOB NOT NULL\
             );\
             CREATE INDEX firestore_documents_database ON firestore_documents(database_name, name);\
             CREATE TABLE counters(name TEXT PRIMARY KEY NOT NULL, value INTEGER NOT NULL);\
             INSERT INTO counters(name, value) VALUES \
                 ('firestore_auto_id', 0), ('storage_generation', 0), ('event_completed', 0);\
             CREATE TABLE auth_users(\
                 namespace TEXT NOT NULL, uid TEXT NOT NULL, user_json TEXT NOT NULL,\
                 password TEXT, PRIMARY KEY(namespace, uid)\
             );\
             CREATE INDEX auth_users_email ON auth_users(namespace, json_extract(user_json, '$.email'));\
             CREATE TABLE auth_sessions(\
                 token TEXT PRIMARY KEY NOT NULL, namespace TEXT NOT NULL, uid TEXT NOT NULL,\
                 issued_at INTEGER NOT NULL, expires_at INTEGER NOT NULL, kind INTEGER NOT NULL\
             );\
             CREATE INDEX auth_sessions_user ON auth_sessions(namespace, uid);\
             CREATE TABLE storage_objects(\
                 bucket TEXT NOT NULL, name TEXT NOT NULL, blob_name TEXT NOT NULL UNIQUE,\
                 metadata_json TEXT NOT NULL, generation INTEGER NOT NULL,\
                 created TEXT NOT NULL, updated TEXT NOT NULL, PRIMARY KEY(bucket, name)\
             );\
             CREATE INDEX storage_objects_bucket ON storage_objects(bucket, name);\
             CREATE TABLE event_outbox(\
                 id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL, payload BLOB NOT NULL,\
                 state TEXT NOT NULL CHECK(state IN ('pending','in_flight','delivered','failed')),\
                 attempts INTEGER NOT NULL, available_at INTEGER NOT NULL, lease_until INTEGER,\
                 last_error TEXT, created_at INTEGER NOT NULL DEFAULT(unixepoch()),\
                 delivered_at INTEGER\
             );\
             CREATE INDEX event_outbox_delivery ON event_outbox(state, available_at, id);\
             PRAGMA application_id=1178946901;\
             PRAGMA user_version=1;",
        )?;
        transaction.commit()?;
        version = 1;
    }
    let application_id: i64 =
        connection.query_row("PRAGMA application_id", [], |row| row.get(0))?;
    if application_id != APPLICATION_ID {
        return Err(Error::ForeignDatabase);
    }
    if version == 1 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE pubsub_topics(\
                 name TEXT PRIMARY KEY NOT NULL, topic BLOB NOT NULL\
             );\
             CREATE TABLE pubsub_subscriptions(\
                 name TEXT PRIMARY KEY NOT NULL, topic_name TEXT NOT NULL, subscription BLOB NOT NULL\
             );\
             CREATE INDEX pubsub_subscriptions_topic ON pubsub_subscriptions(topic_name, name);\
             CREATE TABLE pubsub_messages(\
                 message_id TEXT PRIMARY KEY NOT NULL, topic_name TEXT NOT NULL, message BLOB NOT NULL,\
                 size_bytes INTEGER NOT NULL, published_at INTEGER NOT NULL, expire_at INTEGER NOT NULL\
             );\
             CREATE INDEX pubsub_messages_expiry ON pubsub_messages(expire_at);\
             CREATE TABLE pubsub_deliveries(\
                 subscription_name TEXT NOT NULL, message_id TEXT NOT NULL,\
                 state TEXT NOT NULL CHECK(state IN ('available','in_flight')),\
                 ack_id TEXT, deadline INTEGER, attempts INTEGER NOT NULL DEFAULT 0,\
                 PRIMARY KEY(subscription_name, message_id),\
                 FOREIGN KEY(subscription_name) REFERENCES pubsub_subscriptions(name) ON DELETE CASCADE,\
                 FOREIGN KEY(message_id) REFERENCES pubsub_messages(message_id) ON DELETE CASCADE\
             );\
             CREATE UNIQUE INDEX pubsub_delivery_ack ON pubsub_deliveries(ack_id) WHERE ack_id IS NOT NULL;\
             CREATE INDEX pubsub_delivery_claim ON pubsub_deliveries(subscription_name, state, deadline, message_id);\
             PRAGMA user_version=2;",
        )?;
        transaction.commit()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn temporary(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("firebase-emu-{name}-{}", uuid::Uuid::new_v4()))
    }

    #[tokio::test]
    async fn initializes_wal_and_rejects_duplicate_owner() {
        let root = temporary("persistence-owner");
        let first = Persistence::open(root.clone(), true).await.unwrap();
        assert_eq!(
            first
                .read(|connection| {
                    connection
                        .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                        .map_err(Error::from)
                })
                .await
                .unwrap(),
            SCHEMA_VERSION
        );
        let error = Persistence::open(root.clone(), true).await.unwrap_err();
        assert!(matches!(error, Error::AlreadyOwned(_)));
        drop(first);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn migrates_v1_in_place_without_losing_existing_service_data() {
        let root = temporary("persistence-v1-migration");
        let persistence = Persistence::open(root.clone(), false).await.unwrap();
        let database = persistence.database_path().to_owned();
        drop(persistence);
        let connection = Connection::open(&database).unwrap();
        connection
            .execute(
                "INSERT INTO firestore_documents(name,database_name,document) VALUES ('projects/demo/databases/(default)/documents/items/kept','projects/demo/databases/(default)',x'010203')",
                [],
            )
            .unwrap();
        connection
            .execute_batch(
                "DROP TABLE pubsub_deliveries;
                 DROP TABLE pubsub_messages;
                 DROP TABLE pubsub_subscriptions;
                 DROP TABLE pubsub_topics;
                 PRAGMA user_version=1;",
            )
            .unwrap();
        drop(connection);
        let persistence = Persistence::open(root.clone(), false).await.unwrap();
        let (version, preserved, pubsub_tables) = persistence
            .read(|connection| {
                Ok((
                    connection.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))?,
                    connection.query_row(
                        "SELECT document FROM firestore_documents WHERE name LIKE '%/items/kept'",
                        [],
                        |row| row.get::<_, Vec<u8>>(0),
                    )?,
                    connection.query_row(
                        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name LIKE 'pubsub_%'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )?,
                ))
            })
            .await
            .unwrap();
        assert_eq!(version, 2);
        assert_eq!(preserved, vec![1, 2, 3]);
        assert_eq!(pubsub_tables, 4);
        drop(persistence);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 12)]
    async fn cancelled_reads_keep_the_blocking_worker_bound() {
        let root = temporary("cancelled-read-bound");
        let persistence = Persistence::open(root.clone(), false).await.unwrap();
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::new();
        for _ in 0..(MAX_READERS * 3) {
            let persistence = persistence.clone();
            let active = active.clone();
            let maximum = maximum.clone();
            let started = started.clone();
            let release = release.clone();
            tasks.push(tokio::spawn(async move {
                persistence
                    .read(move |_| {
                        let live = active.fetch_add(1, Ordering::SeqCst) + 1;
                        maximum.fetch_max(live, Ordering::SeqCst);
                        started.fetch_add(1, Ordering::SeqCst);
                        while !release.load(Ordering::SeqCst) {
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
            }));
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while started.load(Ordering::SeqCst) != MAX_READERS {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        for task in &tasks {
            task.abort();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(started.load(Ordering::SeqCst), MAX_READERS);
        assert_eq!(maximum.load(Ordering::SeqCst), MAX_READERS);
        release.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(5), async {
            while active.load(Ordering::SeqCst) != 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), persistence.read(|_| Ok(())))
            .await
            .unwrap()
            .unwrap();
        drop(persistence);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn rejects_newer_schema_without_resetting_it() {
        let root = temporary("persistence-newer");
        fs::create_dir_all(&root).unwrap();
        let database = root.join("firebase-emu.sqlite3");
        let connection = Connection::open(&database).unwrap();
        connection
            .pragma_update(None, "application_id", APPLICATION_ID)
            .unwrap();
        connection.pragma_update(None, "user_version", 99).unwrap();
        drop(connection);
        let error = Persistence::open(root.clone(), false).await.unwrap_err();
        assert!(matches!(error, Error::NewerSchema { found: 99, .. }));
        let connection = Connection::open(database).unwrap();
        assert_eq!(
            connection
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            99
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn corrupt_database_fails_without_replacement() {
        let root = temporary("persistence-corrupt");
        fs::create_dir_all(&root).unwrap();
        let database = root.join("firebase-emu.sqlite3");
        let original = b"not a sqlite database";
        fs::write(&database, original).unwrap();
        assert!(Persistence::open(root.clone(), false).await.is_err());
        assert_eq!(fs::read(database).unwrap(), original);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unwritable_data_directory_fails_clearly() {
        use std::os::unix::fs::PermissionsExt;
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let parent = temporary("persistence-permission");
        fs::create_dir_all(&parent).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o500)).unwrap();
        let result = Persistence::open(parent.join("state"), false).await;
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(matches!(result, Err(Error::Io(_))));
        fs::remove_dir_all(parent).unwrap();
    }
}
