use std::net::{SocketAddr, TcpListener};


/// Gets a new socket address allocated by the OS.
pub fn get_unused_addr() -> SocketAddr {
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_unused_addr() {
        let addr = get_unused_addr();
        TcpListener::bind(addr)
            .expect("Connect to allocated address");
    }

}