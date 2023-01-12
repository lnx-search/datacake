use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use http::{Request, Response, StatusCode};
use hyper::body::HttpBody;
#[cfg(not(feature = "simulation"))]
use hyper::server::conn::AddrStream;
use hyper::service::service_fn;
use hyper::Body;
use hyper::server::conn::Http;
use rkyv::AlignedVec;
use tokio::sync::oneshot;
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
    #[cfg(not(feature = "simulation"))]
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    #[cfg(feature = "simulation")]
    let listener = turmoil::net::TcpListener::bind(bind_addr).await?;

    let (ready, waiter) = oneshot::channel();
    let handle = tokio::spawn(async move {
        let _ = ready.send(());

        loop {
            let (io, remote_addr) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(e) => {
                    warn!(error = ?e, "Failed to accept client.");
                    continue
                }
            };

            let state = state.clone();
            tokio::task::spawn(async move {let state = state.clone();
                let handler = service_fn(move |req| handle_connection(req, state.clone(), remote_addr));

                let connection = Http::new()
                    .http2_only(true)
                    .http2_adaptive_window(true)
                    .http2_keep_alive_timeout(Duration::from_secs(10))
                    .serve_connection(io, handler);

                if let Err(e) = connection.await {
                    error!(error = ?e, "Error while serving HTTP connection.");
                }
            });
        }
    });

    let _ = waiter.await;

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
