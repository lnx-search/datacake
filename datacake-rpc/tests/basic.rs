use std::hint;
use std::net::SocketAddr;
use datacake_rpc::{Client, RpcService, Server, ServiceRegistry, Handler, Request, Status};
use rkyv::{Serialize, Deserialize, Archive};
use bytecheck::CheckBytes;


#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Debug)]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct MyMessage {
    name: String,
    age: u32,
}

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, PartialEq, Eq, Debug)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug, PartialEq, Eq))]
pub struct MyOtherMessage {
    age: u32,
}

pub struct MyService;

impl RpcService for MyService {
    fn register_handlers(registry: &mut ServiceRegistry<Self>) {
        registry.add_handler::<MyMessage>();
        registry.add_handler::<MyOtherMessage>();
    }
}

#[datacake_rpc::async_trait]
impl Handler<MyMessage> for MyService {
    type Reply = String;

    async fn on_message(&self, msg: Request<MyMessage>) -> Result<Self::Reply, Status> {
        Ok(msg.to_owned().unwrap().name)
    }
}

#[datacake_rpc::async_trait]
impl Handler<MyOtherMessage> for MyService {
    type Reply = MyOtherMessage;

    async fn on_message(&self, msg: Request<MyOtherMessage>) -> Result<Self::Reply, Status> {
        Ok(msg.to_owned().unwrap())
    }
}


#[tokio::test]
async fn test_basic() {
    let bind = "0.0.0.0:8000".parse::<SocketAddr>().unwrap();
    let connect = "127.0.0.1:8000".parse::<SocketAddr>().unwrap();

    let server = Server::listen(bind).await.unwrap();
    server.add_service(MyService);
    println!("Listening to address {}!", bind);

    let client = Client::connect(connect).await.unwrap();
    println!("Connected to address {}!", connect);

    let mut rpc_client = client.create_rpc_client::<MyService>().await.unwrap();

    let msg1 = MyMessage {
        name: "Bobby".to_string(),
        age: 12,
    };

    let msg2 = MyOtherMessage {
        age: 1
    };

    let resp = hint::black_box(rpc_client.send(hint::black_box(&msg1)).await.unwrap());
    assert_eq!(resp, msg1.name);
    let resp = hint::black_box(rpc_client.send(hint::black_box(&msg2)).await.unwrap());
    assert_eq!(resp, msg2);
}