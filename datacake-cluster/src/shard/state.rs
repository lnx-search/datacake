use tokio::sync::oneshot;
use crate::NUMBER_OF_SHARDS;
use crate::shard::StateChangeTs;


pub(crate) async fn state_watcher() -> StateWatcherHandle {
    let (tx, rx) = flume::bounded(10);
    
    let actor = ShardStateWatcher { changes: rx, live_view: vec![0; NUMBER_OF_SHARDS] };
    tokio::spawn(actor.run_actor());
    
    StateWatcherHandle { changes: tx }
}


enum Event {
    ShardChange { shard_id: usize, ts: StateChangeTs },
    GetShards { tx: oneshot::Sender<Vec<StateChangeTs>> },
}

#[derive(Clone)]
pub struct StateWatcherHandle {
    changes: flume::Sender<Event>,
}

impl StateWatcherHandle {
    pub async fn change(&self, shard_id: usize, ts: StateChangeTs) {
        self.changes
            .send_async(Event::ShardChange { shard_id, ts })
            .await
            .expect("Actor should never die.")
    }

    pub async fn get(&self) -> Vec<StateChangeTs> {
        let (tx, rx) = oneshot::channel();

        self.changes
            .send_async(Event::GetShards { tx })
            .await
            .expect("Actor should never die.");

        rx.await.expect("Actor should not drop responder.")
    }
}


struct ShardStateWatcher {
    live_view: Vec<StateChangeTs>,
    changes: flume::Receiver<Event>,
}

impl ShardStateWatcher {
    async fn run_actor(mut self) {
        while let Ok(event) = self.changes.recv_async().await {
            match event {
                Event::GetShards { tx } => {
                    let _ = tx.send(self.live_view.clone());
                },
                Event::ShardChange { shard_id, ts } => {
                    if shard_id >= self.live_view.len() {
                        continue;
                    }
                    
                    self.live_view[shard_id] = ts;
                },
            }
        }
    }
}




