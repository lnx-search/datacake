use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;

use http::{Request, Response, StatusCode};
use hyper::body::HttpBody;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::Body;
use rkyv::AlignedVec;
use tokio::task::JoinHandle;

use crate::server::ServerState;
use crate::{Status, SCRATCH_SPACE};

/// Starts the RPC server.
///
/// This takes a binding socket address and server state.
pub(crate) async fn start_rpc_server(
    bind_addr: SocketAddr,
    state: ServerState,
) -> io::Result<JoinHandle<()>> {
    let make_service = make_service_fn(move |socket: &AddrStream| {
        let remote_addr = socket.remote_addr();
        let state = state.clone();

        async move {
            let service = move |req| handle_connection(req, state.clone(), remote_addr);
            Ok::<_, Infallible>(service_fn(service))
        }
    });

    let handle = tokio::spawn(async move {
        let server = hyper::Server::bind(&bind_addr)
            .tcp_nodelay(false)
            .http2_only(true)
            .http2_adaptive_window(true)
            .serve(make_service);

        if let Err(e) = server.await {
            error!(error = ?e, "Server failed to handle requests.");
        }
    });

    Ok(handle)
}

/// A single connection handler.
///
/// This accepts new streams being created and spawns concurrent tasks to handle
/// them.
async fn handle_connection(
    req: Request<Body>,
    state: ServerState,
    remote_addr: SocketAddr,
) -> Result<Response<Body>, Infallible> {
    match handle_message(req, state, remote_addr).await {
        Ok(r) => Ok(r),
        Err(e) => {
            let mut response = Response::new(Body::from(e.to_string()));
            (*response.status_mut()) = StatusCode::INTERNAL_SERVER_ERROR;
            Ok(response)
        },
    }
}

async fn handle_message(
    req: Request<Body>,
    state: ServerState,
    remote_addr: SocketAddr,
) -> anyhow::Result<Response<Body>> {
    let (req, mut body) = req.into_parts();
    let uri = req.uri.path();
    match state.get_handler(uri) {
        None => {
            let status = Status::unavailable(format!("Unknown service {uri}"));
            let buffer =
                rkyv::to_bytes::<_, SCRATCH_SPACE>(&status).unwrap_or_else(|e| {
                    warn!(error = ?e, "Failed to serialize error message.");
                    AlignedVec::new()
                });

            let mut response = Response::new(Body::from(buffer.into_vec()));
            (*response.status_mut()) = StatusCode::BAD_REQUEST;

            Ok(response)
        },
        Some(handler) => {
            let size = body.size_hint().upper().unwrap_or(1024);
            let mut data = AlignedVec::with_capacity(size as usize);
            while let Some(chunk) = body.data().await {
                data.extend_from_slice(&chunk?);
            }

            let reply = handler.try_handle(remote_addr, data).await;

            match reply {
                Ok(buffer) => {
                    let mut response = Response::new(Body::from(buffer.into_vec()));
                    (*response.status_mut()) = StatusCode::OK;
                    Ok(response)
                },
                Err(buffer) => {
                    let mut response = Response::new(Body::from(buffer.into_vec()));
                    (*response.status_mut()) = StatusCode::BAD_REQUEST;
                    Ok(response)
                },
            }
        },
    }
}
