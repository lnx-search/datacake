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
        let msg = chitchat::ChitchatMessage::deserialize(&mut buffer)
            .map_err(|e| Status::internal(e.to_string()))?;

        let _ = self.messages.try_send((from, msg));

        Ok(Response::new(Empty {}))
    }
}
