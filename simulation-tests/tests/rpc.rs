use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytecheck::CheckBytes;
use datacake::rpc::{
    Channel,
    ErrorCode,
    Handler,
    Request,
    RpcClient,
    RpcService,
    Server,
    ServiceRegistry,
    Status,
};
use rkyv::{Archive, Deserialize, Serialize};
use turmoil::{lookup, Builder};

const PORT: u16 = 9999;

#[test]
/// Simulate a basic network partition when the client
/// first attempts to connect to the RPC server.
fn network_partition_connect() -> turmoil::Result {
    let mut sim = Builder::new().build();

    sim.host("server", || async {
        let message_count = Arc::new(AtomicUsize::new(0));

        let server = Server::listen(get_listen_addr()).await?;
        server.add_service(MyService {
            message_count: message_count.clone(),
            simulate: Simulate::None,
        });

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
            age: 120,
        };

        let result = client.send(&msg).await;
        assert!(result.is_err(), "Client should fail to connect to server.");

        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::ConnectionError,);

        Ok(())
    });

    sim.run()
}

#[test]
/// Simulates the client attempting to contact the RPC server
/// but the connect timeout expiring due to no network traffic
/// going through.
fn network_timeout_connect() -> turmoil::Result {
    let mut sim = Builder::new().build();

    sim.host("server", || async {
        let message_count = Arc::new(AtomicUsize::new(0));

        let server = Server::listen(get_listen_addr()).await?;
        server.add_service(MyService {
            message_count: message_count.clone(),
            simulate: Simulate::None,
        });

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
            age: 120,
        };

        let result = client.send(&msg).await;
        assert!(result.is_err(), "Client should fail to connect to server.");

        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::ConnectionError,);

        Ok(())
    });

    sim.run()
}

#[test]
/// Simulates a network timeout / delay occurring after the client has an already
/// established connection in it's pool.
///
/// The default client will just pend forever unless a timeout is set.
fn network_timeout_after_init() -> turmoil::Result {
    let mut sim = Builder::new().build();

    sim.host("server", || async {
        let message_count = Arc::new(AtomicUsize::new(0));

        let server = Server::listen(get_listen_addr()).await?;
        server.add_service(MyService {
            message_count: message_count.clone(),
            simulate: Simulate::None,
        });

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
            age: 120,
        };

        let result = client.send(&msg).await;
        assert!(result.is_ok(), "Client should successfully contact server.");

        // The behavour is slightly different here as it'll cause the connection
        // to timeout rather than immediately be rejected on connect.
        turmoil::hold("client", "server");

        let result = client.send(&msg).await;
        assert!(result.is_err(), "Client should fail to connect to server.");

        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::Timeout,);

        Ok(())
    });

    sim.run()
}

#[test]
/// Simulates a network delay / timeout occurring after the client has an already
/// established connection in it's pool and then recovering quickly before
/// the request times out.
fn network_timeout_after_init_with_recovery() -> turmoil::Result {
    let mut sim = Builder::new().build();

    sim.host("server", || async {
        let message_count = Arc::new(AtomicUsize::new(0));

        let server = Server::listen(get_listen_addr()).await?;
        server.add_service(MyService {
            message_count: message_count.clone(),
            simulate: Simulate::None,
        });

        tokio::time::sleep(Duration::from_secs(3)).await;

        let n = message_count.load(Ordering::Relaxed);
        assert_eq!(n, 2, "Only 1 message should be delivered to the server.");

        Ok(())
    });

    sim.client("client", async {
        let channel = Channel::connect(addr("server"));
        let mut client = RpcClient::<MyService>::new(channel);
        client.set_timeout(Duration::from_secs(2));

        let msg = MyMessage {
            name: "Bob".to_string(),
            age: 120,
        };

        let result = client.send(&msg).await;
        assert!(result.is_ok(), "Client should successfully contact server.");

        // The behavour is slightly different here as it'll cause the connection
        // to timeout rather than immediately be rejected on connect.
        turmoil::hold("client", "server");

        let task = tokio::spawn(async move { client.send(&msg).await });
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Release the
        turmoil::release("client", "server");

        let result = task.await;
        assert!(
            result.is_ok(),
            "Client should be able to connect to server."
        );

        Ok(())
    });

    sim.run()
}

#[test]
/// Simulates a network timeout occurring after just before the server sends the response.
fn network_timeout_during_server_response() -> turmoil::Result {
    let mut sim = Builder::new().build();

    sim.host("server", || async {
        let message_count = Arc::new(AtomicUsize::new(0));

        let server = Server::listen(get_listen_addr()).await?;
        server.add_service(MyService {
            message_count: message_count.clone(),
            simulate: Simulate::Hold("client", "server"),
        });

        tokio::time::sleep(Duration::from_secs(3)).await;

        let n = message_count.load(Ordering::Relaxed);
        assert_eq!(n, 2, "Only 1 message should be delivered to the server.");

        Ok(())
    });

    sim.client("client", async {
        let channel = Channel::connect(addr("server"));
        let mut client = RpcClient::<MyService>::new(channel);
        client.set_timeout(Duration::from_secs(2));

        let msg = MyMessage {
            name: "Bob".to_string(),
            age: 120,
        };

        let result = client.send(&msg).await;
        assert!(
            result.is_err(),
            "Client should successfully contact server."
        );

        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::Timeout);

        Ok(())
    });

    sim.run()
}

// #[test]  TODO: Currently partitioning only has the same affect as hold, we want to be able
//                to simulate terminating the connection entirely so maybe something to add to turmoil?
// /// Simulates a network partition occurring after just before the server sends the response.
// fn network_partition_during_server_response() -> turmoil::Result {
//     let mut sim = Builder::new().build();
//
//     sim.host("server", || async {
//         let message_count = Arc::new(AtomicUsize::new(0));
//
//         let server = Server::listen(get_listen_addr()).await?;
//         server.add_service(MyService {
//             message_count: message_count.clone(),
//             simulate: Simulate::Partition("client", "server"),
//         });
//
//         tokio::time::sleep(Duration::from_secs(3)).await;
//
//         let n = message_count.load(Ordering::Relaxed);
//         assert_eq!(n, 2, "Only 1 message should be delivered to the server.");
//
//         Ok(())
//     });
//
//     sim.client("client", async {
//         let channel = Channel::connect(addr("server"));
//         let mut client = RpcClient::<MyService>::new(channel);
//         client.set_timeout(Duration::from_secs(2));
//
//         let msg = MyMessage {
//             name: "Bob".to_string(),
//             age: 120,
//         };
//
//         let result = client.send(&msg).await;
//         assert!(result.is_err(), "Client should successfully contact server.");
//
//         let err = result.unwrap_err();
//         assert_eq!(err.code, ErrorCode::ConnectionError);
//
//         Ok(())
//     });
//
//     sim.run()
// }

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
    simulate: Simulate,
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
        self.message_count.fetch_add(1, Ordering::Relaxed);

        match self.simulate {
            // Simulate::Partition(a, b) => turmoil::partition(a, b),
            Simulate::Hold(a, b) => turmoil::hold(a, b),
            Simulate::None => {},
        }

        Ok(msg.to_owned().unwrap().name)
    }
}

enum Simulate {
    // Partition(&'static str, &'static str),
    Hold(&'static str, &'static str),
    None,
}
