use std::time::Duration;

use datacake_crdt::{get_unix_timestamp_ms, HLCTimestamp};
use tokio::sync::oneshot;

#[derive(Clone)]
pub struct Clock {
    tx: flume::Sender<oneshot::Sender<HLCTimestamp>>,
}

impl Clock {
    pub fn new(node_id: u32) -> Self {
        let ts = HLCTimestamp::new(get_unix_timestamp_ms(), 0, node_id);
        let (tx, rx) = flume::bounded(100);

        tokio::spawn(run_clock(ts, rx));

        Self { tx }
    }

    pub async fn get_time(&self) -> HLCTimestamp {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(tx)
            .await
            .expect("Clock actor should never die");

        rx.await.expect("Responder should not be dropped")
    }
}

async fn run_clock(
    mut ts: HLCTimestamp,
    reqs: flume::Receiver<oneshot::Sender<HLCTimestamp>>,
) {
    while let Ok(tx) = reqs.recv_async().await {
        let ts = ts.send().expect("Clock counter should not overflow");

        if ts.counter() >= u16::MAX - 10 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let _ = tx.send(ts);
    }
}
