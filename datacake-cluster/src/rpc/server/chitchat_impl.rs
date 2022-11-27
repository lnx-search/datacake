use std::net::SocketAddr;

use async_trait::async_trait;
use chitchat::serialize::Serializable;
use chitchat::ChitchatMessage;
use tonic::{Request, Response, Status};

use crate::rpc::chitchat_transport_api::chitchat_transport_server::ChitchatTransport;
use crate::rpc::chitchat_transport_api::{ChitchatRpcMessage, Empty};

pub struct ChitchatService {
    messages: flume::Sender<(SocketAddr, ChitchatMessage)>,
}

impl ChitchatService {
    pub fn new(messages: flume::Sender<(SocketAddr, ChitchatMessage)>) -> Self {
        Self { messages }
    }
}

#[async_trait]
impl ChitchatTransport for ChitchatService {
    async fn send_msg(
        &self,
        request: Request<ChitchatRpcMessage>,
    ) -> Result<Response<Empty>, Status> {
        let msg = request.into_inner();

        let mut buffer = msg.source.as_slice();
        let from = SocketAddr::deserialize(&mut buffer)
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut buffer = msg.message.as_slice();
        let msg = ChitchatMessage::deserialize(&mut buffer)
            .map_err(|e| Status::internal(e.to_string()))?;

        let _ = self.messages.try_send((from, msg));

        Ok(Response::new(Empty {}))
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    #[tokio::test]
    async fn test_chitchat_service() {
        let (tx, rx) = flume::bounded(10);
        let service = ChitchatService::new(tx);

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::from([127, 0, 0, 1])), 80);
        let message = ChitchatMessage::BadCluster;

        let msg_req = Request::new(ChitchatRpcMessage {
            message: message.serialize_to_vec(),
            source: addr.serialize_to_vec(),
        });

        service.send_msg(msg_req).await.expect("Send message");

        let (source, msg) = rx.try_recv().expect("Message should be registered");
        assert_eq!(source, addr);
        assert_eq!(msg, message);
    }
}
