use std::hint;
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
    type Reply = String;

    async fn on_message(&self, msg: Request<MyMessage>) -> Result<Self::Reply, Status> {
        Ok(msg.to_owned().unwrap().name)
    }
}

#[tokio::test]
async fn test_basic() {
    let bind = "0.0.0.0:8000".parse::<SocketAddr>().unwrap();
    let connect = "127.0.0.1:8000".parse::<SocketAddr>().unwrap();

    let server = Server::listen(bind).await.unwrap();
    server.add_service(MyService);
    println!("Listening to address {}!", bind);

    let client = Channel::connect(connect).await.unwrap();
    println!("Connected to address {}!", connect);

    let mut rpc_client = RpcClient::<MyService>::new(client);

    let msg1 = MyMessage {
        name: "Bobby".to_string(),
        age: 12,
        buffer: vec![0u8; 32 << 10],
    };

    for _ in 0..3 {
        println!("=================================================");
        let resp =
            hint::black_box(rpc_client.send(hint::black_box(&msg1)).await.unwrap());
        assert_eq!(resp, msg1.name);
    }
}
