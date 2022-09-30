#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardState {
    #[prost(uint64, repeated, tag="1")]
    pub shards: ::prost::alloc::vec::Vec<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SyncRequest {
    #[prost(uint32, tag="1")]
    pub shard_id: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SyncResponse {
    #[prost(uint32, tag="1")]
    pub uncompressed_size: u32,
    #[prost(bytes="bytes", tag="2")]
    pub doc_set: ::prost::bytes::Bytes,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataFetchRequest {
    #[prost(uint64, repeated, tag="3")]
    pub requested_docs: ::prost::alloc::vec::Vec<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataFetchResponse {
    #[prost(uint64, repeated, tag="1")]
    pub doc_ids: ::prost::alloc::vec::Vec<u64>,
    #[prost(uint32, repeated, tag="2")]
    pub offsets: ::prost::alloc::vec::Vec<u32>,
    #[prost(message, repeated, tag="3")]
    pub timestamps: ::prost::alloc::vec::Vec<Timestamp>,
    #[prost(uint32, tag="4")]
    pub uncompressed_size: u32,
    #[prost(bytes="bytes", tag="5")]
    pub docs: ::prost::bytes::Bytes,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpsertPayload {
    #[prost(uint64, repeated, tag="1")]
    pub doc_ids: ::prost::alloc::vec::Vec<u64>,
    #[prost(uint32, repeated, tag="2")]
    pub offsets: ::prost::alloc::vec::Vec<u32>,
    #[prost(message, repeated, tag="3")]
    pub timestamps: ::prost::alloc::vec::Vec<Timestamp>,
    #[prost(uint32, tag="4")]
    pub uncompressed_size: u32,
    #[prost(bytes="bytes", tag="5")]
    pub doc_data: ::prost::bytes::Bytes,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Timestamp {
    #[prost(uint64, tag="1")]
    pub millis: u64,
    #[prost(uint32, tag="2")]
    pub counter: u32,
    #[prost(uint32, tag="3")]
    pub node_id: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeletePayload {
    #[prost(uint64, repeated, tag="1")]
    pub doc_ids: ::prost::alloc::vec::Vec<u64>,
    #[prost(message, repeated, tag="2")]
    pub timestamps: ::prost::alloc::vec::Vec<Timestamp>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Blank {
}
/// Generated client implementations.
pub mod document_sync_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    ///
    /// RPC for bringing nodes up to speed between their data.
    ///
    #[derive(Debug, Clone)]
    pub struct DocumentSyncClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl DocumentSyncClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> DocumentSyncClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> DocumentSyncClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            DocumentSyncClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        pub async fn get_shard_state(
            &mut self,
            request: impl tonic::IntoRequest<super::Blank>,
        ) -> Result<tonic::Response<super::ShardState>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/cluster_rpc_models.DocumentSync/GetShardState",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn sync(
            &mut self,
            request: impl tonic::IntoRequest<super::SyncRequest>,
        ) -> Result<tonic::Response<super::SyncResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/cluster_rpc_models.DocumentSync/Sync",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn fetch_docs(
            &mut self,
            request: impl tonic::IntoRequest<super::DataFetchRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::DataFetchResponse>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/cluster_rpc_models.DocumentSync/FetchDocs",
            );
            self.inner.server_streaming(request.into_request(), path, codec).await
        }
    }
}
/// Generated client implementations.
pub mod general_rpc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    ///
    /// General RPC for consistency propagation.
    ///
    #[derive(Debug, Clone)]
    pub struct GeneralRpcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl GeneralRpcClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> GeneralRpcClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> GeneralRpcClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            GeneralRpcClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        pub async fn upsert_docs(
            &mut self,
            request: impl tonic::IntoRequest<super::UpsertPayload>,
        ) -> Result<tonic::Response<super::Blank>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/cluster_rpc_models.GeneralRpc/UpsertDocs",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn delete_docs(
            &mut self,
            request: impl tonic::IntoRequest<super::DeletePayload>,
        ) -> Result<tonic::Response<super::Blank>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/cluster_rpc_models.GeneralRpc/DeleteDocs",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod document_sync_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with DocumentSyncServer.
    #[async_trait]
    pub trait DocumentSync: Send + Sync + 'static {
        async fn get_shard_state(
            &self,
            request: tonic::Request<super::Blank>,
        ) -> Result<tonic::Response<super::ShardState>, tonic::Status>;
        async fn sync(
            &self,
            request: tonic::Request<super::SyncRequest>,
        ) -> Result<tonic::Response<super::SyncResponse>, tonic::Status>;
        ///Server streaming response type for the FetchDocs method.
        type FetchDocsStream: futures_core::Stream<
                Item = Result<super::DataFetchResponse, tonic::Status>,
            >
            + Send
            + 'static;
        async fn fetch_docs(
            &self,
            request: tonic::Request<super::DataFetchRequest>,
        ) -> Result<tonic::Response<Self::FetchDocsStream>, tonic::Status>;
    }
    ///
    /// RPC for bringing nodes up to speed between their data.
    ///
    #[derive(Debug)]
    pub struct DocumentSyncServer<T: DocumentSync> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: DocumentSync> DocumentSyncServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for DocumentSyncServer<T>
    where
        T: DocumentSync,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/cluster_rpc_models.DocumentSync/GetShardState" => {
                    #[allow(non_camel_case_types)]
                    struct GetShardStateSvc<T: DocumentSync>(pub Arc<T>);
                    impl<T: DocumentSync> tonic::server::UnaryService<super::Blank>
                    for GetShardStateSvc<T> {
                        type Response = super::ShardState;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::Blank>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).get_shard_state(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetShardStateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/cluster_rpc_models.DocumentSync/Sync" => {
                    #[allow(non_camel_case_types)]
                    struct SyncSvc<T: DocumentSync>(pub Arc<T>);
                    impl<T: DocumentSync> tonic::server::UnaryService<super::SyncRequest>
                    for SyncSvc<T> {
                        type Response = super::SyncResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SyncRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).sync(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SyncSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/cluster_rpc_models.DocumentSync/FetchDocs" => {
                    #[allow(non_camel_case_types)]
                    struct FetchDocsSvc<T: DocumentSync>(pub Arc<T>);
                    impl<
                        T: DocumentSync,
                    > tonic::server::ServerStreamingService<super::DataFetchRequest>
                    for FetchDocsSvc<T> {
                        type Response = super::DataFetchResponse;
                        type ResponseStream = T::FetchDocsStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DataFetchRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).fetch_docs(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = FetchDocsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: DocumentSync> Clone for DocumentSyncServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: DocumentSync> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: DocumentSync> tonic::server::NamedService for DocumentSyncServer<T> {
        const NAME: &'static str = "cluster_rpc_models.DocumentSync";
    }
}
/// Generated server implementations.
pub mod general_rpc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with GeneralRpcServer.
    #[async_trait]
    pub trait GeneralRpc: Send + Sync + 'static {
        async fn upsert_docs(
            &self,
            request: tonic::Request<super::UpsertPayload>,
        ) -> Result<tonic::Response<super::Blank>, tonic::Status>;
        async fn delete_docs(
            &self,
            request: tonic::Request<super::DeletePayload>,
        ) -> Result<tonic::Response<super::Blank>, tonic::Status>;
    }
    ///
    /// General RPC for consistency propagation.
    ///
    #[derive(Debug)]
    pub struct GeneralRpcServer<T: GeneralRpc> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: GeneralRpc> GeneralRpcServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for GeneralRpcServer<T>
    where
        T: GeneralRpc,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/cluster_rpc_models.GeneralRpc/UpsertDocs" => {
                    #[allow(non_camel_case_types)]
                    struct UpsertDocsSvc<T: GeneralRpc>(pub Arc<T>);
                    impl<T: GeneralRpc> tonic::server::UnaryService<super::UpsertPayload>
                    for UpsertDocsSvc<T> {
                        type Response = super::Blank;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpsertPayload>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).upsert_docs(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpsertDocsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/cluster_rpc_models.GeneralRpc/DeleteDocs" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteDocsSvc<T: GeneralRpc>(pub Arc<T>);
                    impl<T: GeneralRpc> tonic::server::UnaryService<super::DeletePayload>
                    for DeleteDocsSvc<T> {
                        type Response = super::Blank;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeletePayload>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).delete_docs(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteDocsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: GeneralRpc> Clone for GeneralRpcServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: GeneralRpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: GeneralRpc> tonic::server::NamedService for GeneralRpcServer<T> {
        const NAME: &'static str = "cluster_rpc_models.GeneralRpc";
    }
}
