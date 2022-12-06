use std::path::Path;

use flume::{self, Receiver, Sender};
use futures::channel::oneshot;
use rusqlite::{Connection, OptionalExtension, Params, Row};

type Task = Box<dyn FnOnce(&mut Connection) + Send + 'static>;

const CAPACITY: usize = 10;

#[derive(Debug, Clone)]
/// A asynchronous wrapper around a SQLite database.
///
/// These operations will be ran in a background thread preventing
/// any IO operations from blocking the async context.
pub struct StorageHandle {
    tx: Sender<Task>,
}

impl StorageHandle {
    /// Connects to the SQLite database.
    ///
    /// This spawns 1 background threads with actions being executed within that thread.
    ///
    /// This approach reduces the affect of writes blocking reads and vice-versa.
    pub async fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let tx = setup_database(path).await?;
        Ok(Self { tx })
    }

    /// Connects to a new in-memory SQLite database.
    pub async fn open_in_memory() -> rusqlite::Result<Self> {
        Self::open(":memory:").await
    }

    /// Execute a SQL statement with some provided parameters.
    pub async fn execute<P>(
        &self,
        sql: impl AsRef<str>,
        params: P,
    ) -> rusqlite::Result<usize>
    where
        P: Params + Clone + Send + 'static,
    {
        let sql = sql.as_ref().to_string();
        self.submit_task(move |conn| {
            let mut prepared = conn.prepare_cached(&sql)?;
            prepared.execute(params)
        })
        .await
    }

    /// Execute a SQL statement several times with some provided parameters.
    ///
    /// The statement is executed within the same transaction.
    pub async fn execute_many<P>(
        &self,
        sql: impl AsRef<str>,
        param_set: Vec<P>,
    ) -> rusqlite::Result<usize>
    where
        P: Params + Clone + Send + 'static,
    {
        let sql = sql.as_ref().to_string();
        self.submit_task(move |conn| {
            let tx = conn.transaction()?;

            let mut total = 0;
            {
                let mut prepared = tx.prepare_cached(&sql)?;

                for params in param_set {
                    total += prepared.execute(params)?;
                }
            }

            tx.commit()?;
            Ok(total)
        })
        .await
    }

    /// Fetch a single row from a given SQL statement with some provided parameters.
    pub async fn fetch_one<P, T>(
        &self,
        sql: impl AsRef<str>,
        params: P,
    ) -> rusqlite::Result<Option<T>>
    where
        P: Params + Send + 'static,
        T: FromRow + Send + 'static,
    {
        let sql = sql.as_ref().to_string();

        self.submit_task(move |conn| {
            let mut prepared = conn.prepare_cached(&sql)?;
            prepared.query_row(params, T::from_row).optional()
        })
        .await
    }

    /// Fetch a all rows from a given SQL statement with some provided parameters.
    pub async fn fetch_all<P, T>(
        &self,
        sql: impl AsRef<str>,
        params: P,
    ) -> rusqlite::Result<Vec<T>>
    where
        P: Params + Send + 'static,
        T: FromRow + Send + 'static,
    {
        let sql = sql.as_ref().to_string();

        self.submit_task(move |conn| {
            let mut prepared = conn.prepare_cached(&sql)?;
            let mut iter = prepared.query(params)?;

            let mut rows = Vec::with_capacity(4);
            while let Some(row) = iter.next()? {
                rows.push(T::from_row(row)?);
            }

            Ok(rows)
        })
        .await
    }

    /// Submits a writer task to execute.
    ///
    /// This executes the callback on the memory view connection which should be
    /// significantly faster to modify or read.
    async fn submit_task<CB, T>(&self, inner: CB) -> rusqlite::Result<T>
    where
        T: Send + 'static,
        CB: FnOnce(&mut Connection) -> rusqlite::Result<T> + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();

        let cb = move |conn: &mut Connection| {
            let res = inner(conn);
            let _ = tx.send(res);
        };

        self.tx
            .send_async(Box::new(cb))
            .await
            .expect("send message");

        rx.await.unwrap()
    }
}

/// A helper trait for converting between a Row reference and the given type.
///
/// This is required due to the nature of rows being tied to the database connection
/// which cannot be shared outside of the thread the actor runs in.
pub trait FromRow: Sized {
    fn from_row(row: &Row) -> rusqlite::Result<Self>;
}

async fn setup_database(path: impl AsRef<Path>) -> rusqlite::Result<Sender<Task>> {
    let path = path.as_ref().to_path_buf();
    let (tx, rx) = flume::bounded(CAPACITY);

    tokio::task::spawn_blocking(move || setup_disk_handle(&path, rx))
        .await
        .expect("spawn background runner")?;

    Ok(tx)
}

fn setup_disk_handle(path: &Path, tasks: Receiver<Task>) -> rusqlite::Result<()> {
    let disk = Connection::open(path)?;

    disk.query_row("pragma journal_mode = WAL;", (), |_r| Ok(()))?;
    disk.execute("pragma synchronous = normal;", ())?;
    disk.execute("pragma temp_store = memory;", ())?;

    std::thread::spawn(move || run_tasks(disk, tasks));

    Ok(())
}

/// Runs all tasks received with a mutable reference to the given connection.
fn run_tasks(mut conn: Connection, tasks: Receiver<Task>) {
    while let Ok(task) = tasks.recv() {
        (task)(&mut conn);
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;

    use super::*;

    #[tokio::test]
    async fn test_memory_storage_handle() {
        let handle = StorageHandle::open_in_memory().await.expect("open DB");

        run_storage_handle_suite(handle).await;
    }

    #[tokio::test]
    async fn test_disk_storage_handle() {
        let path = temp_dir().join(uuid::Uuid::new_v4().to_string());
        let handle = StorageHandle::open(path).await.expect("open DB");

        run_storage_handle_suite(handle).await;
    }

    #[derive(Debug, Eq, PartialEq)]
    struct Person {
        id: i32,
        name: String,
        data: String,
    }

    impl FromRow for Person {
        fn from_row(row: &Row) -> rusqlite::Result<Self> {
            Ok(Self {
                id: row.get(0)?,
                name: row.get(1)?,
                data: row.get(2)?,
            })
        }
    }

    async fn run_storage_handle_suite(handle: StorageHandle) {
        handle
            .execute(
                "CREATE TABLE person (
                    id    INTEGER PRIMARY KEY,
                    name  TEXT NOT NULL,
                    data  BLOB
                )",
                (), // empty list of parameters.
            )
            .await
            .expect("create table");

        let res = handle
            .fetch_one::<_, Person>("SELECT id, name, data FROM person;", ())
            .await
            .expect("execute statement");
        assert!(res.is_none(), "Expected no rows to be returned.");

        handle
            .execute(
                "INSERT INTO person (id, name, data) VALUES (1, 'cf8', 'tada');",
                (),
            )
            .await
            .expect("Insert row");

        let res = handle
            .fetch_one::<_, Person>("SELECT id, name, data FROM person;", ())
            .await
            .expect("execute statement");
        assert_eq!(
            res,
            Some(Person {
                id: 1,
                name: "cf8".to_string(),
                data: "tada".to_string()
            }),
        );

        handle
            .execute(
                "INSERT INTO person (id, name, data) VALUES (2, 'cf6', 'tada2');",
                (),
            )
            .await
            .expect("Insert row");

        let res = handle
            .fetch_all::<_, Person>(
                "SELECT id, name, data FROM person ORDER BY id ASC;",
                (),
            )
            .await
            .expect("execute statement");
        assert_eq!(
            res,
            vec![
                Person {
                    id: 1,
                    name: "cf8".to_string(),
                    data: "tada".to_string()
                },
                Person {
                    id: 2,
                    name: "cf6".to_string(),
                    data: "tada2".to_string()
                },
            ],
        );
    }
}
