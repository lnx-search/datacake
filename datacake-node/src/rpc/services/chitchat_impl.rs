use std::net::SocketAddr;

use bytecheck::CheckBytes;
use chitchat::serialize::Serializable;
use chitchat::ChitchatMessage;
use datacake_crdt::HLCTimestamp;
use datacake_rpc::{Handler, Request, RpcService, ServiceRegistry, Status};
use rkyv::{Archive, Deserialize, Serialize};

use crate::Clock;

#[repr(C)]
#[derive(Serialize, Deserialize, Archive)]
#[archive_attr(derive(CheckBytes))]
pub struct ChitchatRpcMessage {
    pub data: Vec<u8>,
    pub source: SocketAddr,
    pub timestamp: HLCTimestamp,
}

pub struct ChitchatService {
    clock: Clock,
    messages: flume::Sender<(SocketAddr, ChitchatMessage)>,
}

impl ChitchatService {
    pub fn new(
        clock: Clock,
        messages: flume::Sender<(SocketAddr, ChitchatMessage)>,
    ) -> Self {
        Self { clock, messages }
    }
}

impl RpcService for ChitchatService {
    fn register_handlers(registry: &mut ServiceRegistry<Self>) {
        registry.add_handler::<ChitchatRpcMessage>();
    }
}

#[datacake_rpc::async_trait]
impl Handler<ChitchatRpcMessage> for ChitchatService {
    type Reply = HLCTimestamp;

    async fn on_message(
        &self,
        request: Request<ChitchatRpcMessage>,
    ) -> Result<Self::Reply, Status> {
        let msg = request.to_owned().map_err(Status::internal)?;

        let from = msg.source;
        self.clock.register_ts(msg.timestamp).await;

        let mut buffer = msg.data.as_slice();
        let msg = <ChitchatMessage as Serializable>::deserialize(&mut buffer)
            .map_err(|e| Status::internal(e.to_string()))?;

        let _ = self.messages.try_send((from, msg));

        Ok(self.clock.get_time().await)
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    #[tokio::test]
    async fn test_chitchat_service() {
        let clock = Clock::new(0);
        let (tx, rx) = flume::bounded(10);
        let service = ChitchatService::new(clock.clone(), tx);

        let source = SocketAddr::new(IpAddr::V4(Ipv4Addr::from([127, 0, 0, 1])), 80);
        let message = ChitchatMessage::BadCluster;
        let timestamp = clock.get_time().await;

        let msg_req = Request::using_owned(ChitchatRpcMessage {
            timestamp,
            data: message.serialize_to_vec(),
            source,
        });

        service.on_message(msg_req).await.expect("Send message");

        let (addr, msg) = rx.try_recv().expect("Message should be registered");
        assert_eq!(addr, source);
        assert_eq!(msg, message);
    }
}
