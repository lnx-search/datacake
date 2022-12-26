use std::net::SocketAddr;

use bytecheck::CheckBytes;
use datacake_rpc::{
    Channel,
    Handler,
    Request,
    RpcClient,
    RpcService,
    Server,
    ServiceRegistry,
    Status,
};
use rkyv::{Archive, Deserialize, Serialize};

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Debug)]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct Payload {
    value: u64,
}

pub struct Add5Service;

impl RpcService for Add5Service {
    fn register_handlers(registry: &mut ServiceRegistry<Self>) {
        registry.add_handler::<Payload>();
    }
}

#[datacake_rpc::async_trait]
impl Handler<Payload> for Add5Service {
    type Reply = u64;

    async fn on_message(&self, msg: Request<Payload>) -> Result<Self::Reply, Status> {
        dbg!(&msg);
        let counter = msg.to_owned().expect("Get owned value.");
        Ok(counter.value.saturating_add(5))
    }
}

pub struct Sub5Service;

impl RpcService for Sub5Service {
    fn register_handlers(registry: &mut ServiceRegistry<Self>) {
        registry.add_handler::<Payload>();
    }
}

#[datacake_rpc::async_trait]
impl Handler<Payload> for Sub5Service {
    type Reply = u64;

    async fn on_message(&self, msg: Request<Payload>) -> Result<Self::Reply, Status> {
        dbg!(&msg);
        let counter = msg.to_owned().expect("Get owned value.");
        Ok(counter.value.saturating_sub(5))
    }
}

#[tokio::test]
async fn test_multiple_services() {
    let _ = tracing_subscriber::fmt::try_init();

    let addr = "127.0.0.1:7003".parse::<SocketAddr>().unwrap();

    let server = Server::listen(addr).await.unwrap();
    server.add_service(Add5Service);
    server.add_service(Sub5Service);
    println!("Listening to address {}!", addr);

    let client = Channel::connect(addr).unwrap();
    println!("Connected to address {}!", addr);

    let msg = Payload { value: 5 };

    let mut add_client = RpcClient::<Add5Service>::new(client.clone());
    let mut subtract_client = RpcClient::<Sub5Service>::new(client);

    let resp = add_client.send(&msg).await.unwrap();
    assert_eq!(resp, 10);

    let resp = subtract_client.send(&msg).await.unwrap();
    assert_eq!(resp, 0);

    let mut subtract_client = add_client.new_client::<Sub5Service>();
    let resp = subtract_client.send(&msg).await.unwrap();
    assert_eq!(resp, 0);
}
