use std::sync::Arc;

use anyhow::anyhow;
use futures::channel::oneshot;

#[derive(Clone)]
/// A executor for spawning CPU intensive tasks like compression.
pub struct BlockingExecutor {
    pool: Arc<rayon::ThreadPool>,
}

impl BlockingExecutor {
    pub fn with_n_threads(n: usize) -> anyhow::Result<Self> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .thread_name(|n| format!("hourglass-cpu-task-{}", n))
            .build()?;

        Ok(Self { pool: pool.into() })
    }

    /// Runs a blocking task in the threadpool until completion.
    ///
    /// Note:
    ///  This may (asynchronously) block for an extended period of time
    ///  if the pool is fully utilised.
    pub async fn execute<F, T>(&self, func: F) -> anyhow::Result<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Sync + Send + 'static,
    {
        let (tx, completed) = oneshot::channel();

        self.pool.spawn(move || {
            let result = func();
            let _ = tx.send(result);
        });

        completed
            .await
            .map_err(|_| anyhow!("Failed to get result from thread."))
    }
}
