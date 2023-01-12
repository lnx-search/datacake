use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use turmoil::{Builder, lookup};
use bytecheck::CheckBytes;
use datacake::rpc::{Channel, ErrorCode, Handler, Request, RpcClient, RpcService, Server, ServiceRegistry, Status};
use rkyv::{Archive, Deserialize, Serialize};

const PORT: u16 = 9999;

#[test]
fn network_partition_connect() -> turmoil::Result {
    let mut sim = Builder::new().build();

    sim.host("server", || async {
        let message_count = Arc::new(AtomicUsize::new(0));

        let server = Server::listen(get_listen_addr()).await?;
        server.add_service(MyService { message_count: message_count.clone() });

        tokio::time::sleep(Duration::from_secs(3)).await;

        let n = message_count.load(Ordering::Relaxed);
        assert_eq!(n, 0, "No messages should be delivered to the server");

        Ok(())
    });

    sim.client("client", async {
        turmoil::partition("client", "server");

        let channel = Channel::connect(addr("server"));
        let client = RpcClient::<MyService>::new(channel);

        let msg = MyMessage {
            name: "Bob".to_string(),
            age: 120
        };

        let result = client.send(&msg).await;
        assert!(result.is_err(), "Client should fail to connect to server.");

        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::ConnectionError, "Returned error should be a connection error.");

        Ok(())
    });

    sim.run()
}


#[test]
fn network_timeout_connect() -> turmoil::Result {
    let mut sim = Builder::new().build();

    sim.host("server", || async {
        let message_count = Arc::new(AtomicUsize::new(0));

        let server = Server::listen(get_listen_addr()).await?;
        server.add_service(MyService { message_count: message_count.clone() });

        tokio::time::sleep(Duration::from_secs(3)).await;

        let n = message_count.load(Ordering::Relaxed);
        assert_eq!(n, 0, "No messages should be delivered to the server.");

        Ok(())
    });

    sim.client("client", async {
        // The behavour is slightly different here as it'll cause the connection
        // to timeout rather than immediately be rejected on connect.
        turmoil::hold("client", "server");

        let channel = Channel::connect(addr("server"));
        let client = RpcClient::<MyService>::new(channel);

        let msg = MyMessage {
            name: "Bob".to_string(),
            age: 120
        };

        let result = client.send(&msg).await;
        assert!(result.is_err(), "Client should fail to connect to server.");

        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::ConnectionError, "Returned error should be a connection error.");

        Ok(())
    });

    sim.run()
}


#[test]
fn network_partition_after_init() -> turmoil::Result {
    let mut sim = Builder::new().build();

    sim.host("server", || async {
        let message_count = Arc::new(AtomicUsize::new(0));

        let server = Server::listen(get_listen_addr()).await?;
        server.add_service(MyService { message_count: message_count.clone() });

        tokio::time::sleep(Duration::from_secs(3)).await;

        let n = message_count.load(Ordering::Relaxed);
        assert_eq!(n, 1, "Only 1 message should be delivered to the server.");

        Ok(())
    });

    sim.client("client", async {
        let channel = Channel::connect(addr("server"));
        let mut client = RpcClient::<MyService>::new(channel);
        client.set_timeout(Duration::from_secs(2));

        let msg = MyMessage {
            name: "Bob".to_string(),
            age: 120
        };

        let result = client.send(&msg).await;
        assert!(result.is_ok(), "Client should successfully contact server.");

        // The behavour is slightly different here as it'll cause the connection
        // to timeout rather than immediately be rejected on connect.
        turmoil::hold("client", "server");

        let result = client.send(&msg).await;
        assert!(result.is_err(), "Client should fail to connect to server.");

        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::Timeout, "Returned error should be a timeout error.");

        Ok(())
    });

    sim.run()
}


fn get_listen_addr() -> SocketAddr {
    (IpAddr::from(Ipv4Addr::UNSPECIFIED), PORT).into()
}

fn addr(name: &str) -> SocketAddr {
    (lookup(name), PORT).into()
}

// The framework accepts any messages which implement `Archive` and `Serialize` along
// with the archived values implementing `CheckBytes` from the `bytecheck` crate.
// This is to ensure safe, validated deserialization of the values.
//
// Checkout rkyv for more information!
#[repr(C)]
#[derive(Serialize, Deserialize, Archive, PartialEq, Debug)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, PartialEq, Debug))]
pub struct MyMessage {
    name: String,
    age: u32,
}

pub struct MyService {
    message_count: Arc<AtomicUsize>,
}

impl RpcService for MyService {
    // The `register_handlers` is used to mark messages as something
    // the given service can handle and process.
    //
    // Messages which are not registered will not be dispatched to the handler.
    fn register_handlers(registry: &mut ServiceRegistry<Self>) {
        registry.add_handler::<MyMessage>();
    }
}

#[datacake::rpc::async_trait]
impl Handler<MyMessage> for MyService {
    type Reply = String;

    // Our `Request` gives us a zero-copy view to our message, this doesn't actually
    // allocate the message type.
    async fn on_message(&self, msg: Request<MyMessage>) -> Result<Self::Reply, Status> {
        println!("On-message");
        self.message_count.fetch_add(1, Ordering::Relaxed);
        Ok(msg.to_owned().unwrap().name)
    }
}