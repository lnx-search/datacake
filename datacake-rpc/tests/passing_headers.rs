use std::collections::BTreeMap;

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
use http::HeaderValue;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Serialize, Deserialize, Archive, Debug)]
#[archive_attr(derive(Debug))]
pub struct SingleHeader;

#[derive(Serialize, Deserialize, Archive, Debug)]
#[archive_attr(derive(Debug))]
pub struct ManyHeaders;

pub struct MyService;

impl RpcService for MyService {
    fn register_handlers(registry: &mut ServiceRegistry<Self>) {
        registry.add_handler::<SingleHeader>();
        registry.add_handler::<ManyHeaders>();
    }
}

#[datacake_rpc::async_trait]
impl Handler<SingleHeader> for MyService {
    type Reply = Option<String>;

    async fn on_message(
        &self,
        msg: Request<SingleHeader>,
    ) -> Result<Self::Reply, Status> {
        let header = msg
            .headers()
            .get("hello")
            .map(|h| h.to_str().unwrap().to_string());

        Ok(header)
    }
}

#[datacake_rpc::async_trait]
impl Handler<ManyHeaders> for MyService {
    type Reply = BTreeMap<String, String>;

    async fn on_message(
        &self,
        msg: Request<ManyHeaders>,
    ) -> Result<Self::Reply, Status> {
        let mut headers = BTreeMap::new();

        for (key, value) in msg.headers() {
            let value = value.to_str().unwrap().to_string();

            headers.insert(key.to_string(), value);
        }

        Ok(headers)
    }
}

#[tokio::test]
async fn test_sending_headers() {
    let addr = test_helper::get_unused_addr();

    let server = Server::listen(addr).await.unwrap();
    server.add_service(MyService);
    println!("Listening to address {}!", addr);

    let client = Channel::connect(addr);
    println!("Connected to address {}!", addr);

    let rpc_client = RpcClient::<MyService>::new(client);

    let response = rpc_client
        .create_rpc_context()
        .set_header("hello", HeaderValue::from_static("world"))
        .send(&SingleHeader)
        .await
        .expect("Send RPC message");

    assert_eq!(
        response,
        Some("world".to_string()),
        "Header should be received"
    );

    server.shutdown();
}

#[tokio::test]
async fn test_sending_many_headers() {
    let addr = test_helper::get_unused_addr();

    let server = Server::listen(addr).await.unwrap();
    server.add_service(MyService);
    println!("Listening to address {}!", addr);

    let client = Channel::connect(addr);
    println!("Connected to address {}!", addr);

    let rpc_client = RpcClient::<MyService>::new(client);

    let response = rpc_client
        .create_rpc_context()
        .set_headers([
            ("name", HeaderValue::from_static("bobby")),
            ("trace-id", HeaderValue::from_static("1234")),
            ("tag", HeaderValue::from_static("food")),
        ])
        .send(&ManyHeaders)
        .await
        .expect("Send RPC message");

    let mut headers = BTreeMap::new();
    headers.insert("name".to_string(), "bobby".to_string());
    headers.insert("trace-id".to_string(), "1234".to_string());
    headers.insert("tag".to_string(), "food".to_string());
    headers.insert("content-length".to_string(), "4".to_string());

    assert_eq!(response, headers, "Headers should be received",);

    server.shutdown();
}
