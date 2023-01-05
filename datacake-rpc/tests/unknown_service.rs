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
    fn register_handlers(_registry: &mut ServiceRegistry<Self>) {}
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
async fn test_unknown_service() {
    let addr = test_helper::get_unused_addr();

    let server = Server::listen(addr).await.unwrap();
    server.add_service(Add5Service);
    println!("Listening to address {}!", addr);

    let client = Channel::connect(addr);
    println!("Connected to address {}!", addr);

    let msg = Payload { value: 5 };

    let add_client = RpcClient::<Add5Service>::new(client.clone());
    let subtract_client = RpcClient::<Sub5Service>::new(client);

    let resp = add_client.send(&msg).await.unwrap();
    assert_eq!(resp, 10);

    let res = subtract_client
        .send(&msg)
        .await
        .expect_err("Server should reject unknown service");
    assert_eq!(
        res,
        Status::unavailable(format!(
            "Unknown service /{}/{}",
            Sub5Service::service_name(),
            <Sub5Service as Handler<Payload>>::path(),
        )),
        "Server should reject unknown service with message."
    )
}

#[tokio::test]
async fn test_unknown_message() {
    let addr = test_helper::get_unused_addr();

    let server = Server::listen(addr).await.unwrap();
    server.add_service(Add5Service);
    println!("Listening to address {}!", addr);

    let client = Channel::connect(addr);
    println!("Connected to address {}!", addr);

    let msg = Payload { value: 5 };

    let add_client = RpcClient::<Add5Service>::new(client.clone());
    let subtract_client = RpcClient::<Sub5Service>::new(client);

    let resp = add_client.send(&msg).await.unwrap();
    assert_eq!(resp, 10);

    let res = subtract_client
        .send(&msg)
        .await
        .expect_err("Server should reject unknown message");
    assert_eq!(
        res,
        Status::unavailable(format!(
            "Unknown service /{}/{}",
            Sub5Service::service_name(),
            <Sub5Service as Handler<Payload>>::path(),
        )),
        "Server should reject unknown message with message."
    )
}
