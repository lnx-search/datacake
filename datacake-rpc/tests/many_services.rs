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
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
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
    let addr = test_helper::get_unused_addr();

    let server = Server::listen(addr).await.unwrap();
    server.add_service(Add5Service);
    server.add_service(Sub5Service);
    println!("Listening to address {}!", addr);

    let client = Channel::connect(addr);
    println!("Connected to address {}!", addr);

    let msg = Payload { value: 5 };

    let add_client = RpcClient::<Add5Service>::new(client.clone());
    let subtract_client = RpcClient::<Sub5Service>::new(client);

    let resp = add_client.send(&msg).await.unwrap();
    assert_eq!(resp, 10);

    let resp = subtract_client.send(&msg).await.unwrap();
    assert_eq!(resp, 0);

    let subtract_client = add_client.new_client::<Sub5Service>();
    let resp = subtract_client.send(&msg).await.unwrap();
    assert_eq!(resp, 0);

    server.shutdown();
}
