use std::hint::black_box;
use std::net::SocketAddr;
use std::time::Instant;
use anyhow::Result;
use echo::echo_client::EchoClient;
use echo::echo_server::{Echo, EchoServer};
use echo::Message;
use rkyv::{Serialize, Deserialize, Archive};
use bytecheck::CheckBytes;
use tonic::transport::Endpoint;
use datacake_rpc::{Handler, Request, RpcService, ServiceRegistry, Status};

pub mod echo {
    tonic::include_proto!("echo_api");
}


#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let task = tokio::spawn(run_tonic_server());

    let bind = "0.0.0.0:9000".parse::<SocketAddr>().unwrap();
    let server = datacake_rpc::Server::listen(bind).await.unwrap();
    server.add_service(MyService);
    println!("Listening to address {}!", bind);

    task.await.unwrap()?;

    Ok(())
}


async fn run_tonic_server() -> Result<()> {
    let addr = "0.0.0.0:9001".parse().unwrap();
    let greeter = TonicServer::default();

    println!("Tonic server listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(EchoServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}

#[derive(Default)]
pub struct TonicServer {}

#[tonic::async_trait]
impl Echo for TonicServer {
    async fn echo(
        &self,
        request: tonic::Request<Message>,
    ) -> Result<tonic::Response<Message>, tonic::Status> {
        Ok(tonic::Response::new(request.into_inner()))
    }
}



#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Debug)]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct DatacakeMessage {
    name: String,
    age: u32,
    buffer: Vec<u8>,
}


pub struct MyService;

impl RpcService for MyService {
    fn register_handlers(registry: &mut ServiceRegistry<Self>) {
        registry.add_handler::<DatacakeMessage>();
    }
}

#[datacake_rpc::async_trait]
impl Handler<DatacakeMessage> for MyService {
    type Reply = DatacakeMessage;

    async fn on_message(&self, msg: Request<DatacakeMessage>) -> Result<Self::Reply, Status> {
        Ok(msg.to_owned().unwrap())
    }
}