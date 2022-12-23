use std::hint::black_box;
use std::net::SocketAddr;
use std::time::Instant;
use anyhow::Result;
use echo::echo_client::EchoClient;
use echo::echo_server::{Echo, EchoServer};
use echo::Message;
use rkyv::{Serialize, Deserialize, Archive};
use bytecheck::CheckBytes;
use datacake_rpc::{Handler, Request, RpcService, ServiceRegistry, Status};

pub mod echo {
    tonic::include_proto!("echo_api");
}


#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let task = tokio::spawn(run_tonic_server());
    let mut client = EchoClient::connect("http://127.0.0.1:8001").await?;

    let start = Instant::now();
    for _ in 0..10_000 {
        let request = tonic::Request::new(Message {
            name: "Harrison".into(),
            age: 19,
            buffer: vec![0u8; 32 << 10],
        });

        let response = black_box(client.echo(black_box(request)).await)?;
        let inner = response.into_inner();
        assert_eq!(inner.name, "Harrison");
        assert_eq!(inner.age, 19)
    }
    println!("Tonic took: {:?}, {:?}/req", start.elapsed(), start.elapsed() / 10_000);

    //let client = Endpoint::from_static("http://127.0.0.1:8001").connect().await?;
    //let start = Instant::now();
    //let mut tasks = Vec::new();
    //for _ in 0..100 {
    //    let mut client = EchoClient::new(client.clone());
    //    let task = tokio::spawn(async move {
    //        for _ in 0..10_000 {
    //            let request = tonic::Request::new(Message {
    //                name: "Harrison".into(),
    //                age: 19,
    //                buffer: vec![0u8; 32 << 10],
    //            });
    //
    //            let response = black_box(client.echo(black_box(request)).await)?;
    //            let inner = response.into_inner();
    //            assert_eq!(inner.name, "Harrison");
    //            assert_eq!(inner.age, 19)
    //        }
    //        Ok::<_, anyhow::Error>(())
    //    });
    //    tasks.push(task);
    //}
    //
    //for task in tasks {
    //    task.await.unwrap()?;
    //}
    //println!("(100 Concurrency) Tonic took: {:?}, {:?}/req", start.elapsed(), start.elapsed() / (10_000 * 100));
    task.abort();

    let bind = "0.0.0.0:8000".parse::<SocketAddr>().unwrap();
    let connect = "127.0.0.1:8000".parse::<SocketAddr>().unwrap();
    let server = datacake_rpc::Server::listen(bind).await.unwrap();
    server.add_service(MyService);
    println!("Listening to address {}!", bind);

    let client = datacake_rpc::Client::connect(connect).await.unwrap();
    println!("Connected to address {}!", connect);

    let mut rpc_client = client.create_rpc_client::<MyService>().await.unwrap();
    let start = Instant::now();
    for _ in 0..10_000 {
        let request = DatacakeMessage {
            name: "Harrison".into(),
            age: 19,
            buffer: vec![0u8; 32 << 10],
        };
        let response = black_box(rpc_client.send(&black_box(request)).await)?;
        assert_eq!(response.name, "Harrison");
        assert_eq!(response.age, 19)
    }
    println!("Datacake took: {:?}, {:?}/req", start.elapsed(), start.elapsed() / 10_000);

    //let start = Instant::now();
    //let mut tasks = Vec::new();
    //for _ in 0..100 {
    //    let mut rpc_client = rpc_client.clone_client().await.unwrap();
    //    let task = tokio::spawn(async move {
    //        for _ in 0..10_000 {
    //            let request = DatacakeMessage {
    //                name: "Harrison".into(),
    //                age: 19,
    //                buffer: vec![0u8; 32 << 10],
    //            };
    //            let response = black_box(rpc_client.send(&black_box(request)).await)?;
    //            assert_eq!(response.name, "Harrison");
    //            assert_eq!(response.age, 19)
    //        }
    //        Ok::<_, anyhow::Error>(())
    //    });
    //    tasks.push(task);
    //}
    //
    //for task in tasks {
    //    task.await.unwrap()?;
    //}
    //println!("(100 Concurrency) Datacake took: {:?}, {:?}/req", start.elapsed(), start.elapsed() / (10_000 * 100));
    Ok(())
}


async fn run_tonic_server() -> Result<()> {
    let addr = "127.0.0.1:8001".parse().unwrap();
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