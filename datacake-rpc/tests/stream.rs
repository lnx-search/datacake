use datacake_rpc::{
    Body,
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
pub struct MyMessage {
    name: String,
    age: u32,
    buffer: Vec<u8>,
}

pub struct MyService;

impl RpcService for MyService {
    fn register_handlers(registry: &mut ServiceRegistry<Self>) {
        registry.add_handler::<MyMessage>();
    }
}

#[datacake_rpc::async_trait]
impl Handler<MyMessage> for MyService {
    type Reply = Body;

    async fn on_message(&self, msg: Request<MyMessage>) -> Result<Self::Reply, Status> {
        let bytes = msg.into_inner().into_data();
        Ok(Body::from(bytes.to_vec()))
    }
}

#[tokio::test]
async fn test_stream_body() {
    let addr = test_helper::get_unused_addr();

    let server = Server::listen(addr).await.unwrap();
    server.add_service(MyService);
    println!("Listening to address {}!", addr);

    let client = Channel::connect(addr);
    println!("Connected to address {}!", addr);

    let rpc_client = RpcClient::<MyService>::new(client);

    let msg1 = MyMessage {
        name: "Bobby".to_string(),
        age: 12,
        buffer: vec![0u8; 32 << 10],
    };

    let bytes = rkyv::to_bytes::<_, 1024>(&msg1).unwrap();
    let resp = rpc_client.send(&msg1).await.unwrap();
    let body = hyper::body::to_bytes(resp.into_inner()).await.unwrap();
    assert_eq!(bytes.as_ref(), body.as_ref());

    server.shutdown();
}
