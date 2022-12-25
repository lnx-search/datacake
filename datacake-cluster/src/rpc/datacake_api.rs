#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchPayload {
    #[prost(message, optional, tag = "1")]
    pub timestamp: ::core::option::Option<Timestamp>,
    #[prost(message, repeated, tag = "2")]
    pub modified: ::prost::alloc::vec::Vec<MultiPutPayload>,
    #[prost(message, repeated, tag = "3")]
    pub removed: ::prost::alloc::vec::Vec<MultiRemovePayload>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PollPayload {
    #[prost(message, optional, tag = "1")]
    pub timestamp: ::core::option::Option<Timestamp>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PutPayload {
    #[prost(string, tag = "1")]
    pub keyspace: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub ctx: ::core::option::Option<Context>,
    #[prost(message, optional, tag = "3")]
    pub document: ::core::option::Option<Document>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MultiPutPayload {
    #[prost(string, tag = "1")]
    pub keyspace: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub ctx: ::core::option::Option<Context>,
    #[prost(message, repeated, tag = "3")]
    pub documents: ::prost::alloc::vec::Vec<Document>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Context {
    #[prost(string, tag = "1")]
    pub node_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub node_addr: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemovePayload {
    #[prost(string, tag = "1")]
    pub keyspace: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub document: ::core::option::Option<DocumentMetadata>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MultiRemovePayload {
    #[prost(string, tag = "1")]
    pub keyspace: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub documents: ::prost::alloc::vec::Vec<DocumentMetadata>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyspaceInfo {
    #[prost(message, optional, tag = "1")]
    pub timestamp: ::core::option::Option<Timestamp>,
    /// A mapping of a given keyspace and the timestamp of when it was last updated.
    #[prost(map = "string, message", tag = "2")]
    pub keyspace_timestamps: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        Timestamp,
    >,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetState {
    /// The keyspace to fetch the CRDT set from.
    #[prost(string, tag = "1")]
    pub keyspace: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub timestamp: ::core::option::Option<Timestamp>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyspaceOrSwotSet {
    /// The timestamp in which the keyspace was last updated.
    #[prost(message, optional, tag = "1")]
    pub last_updated: ::core::option::Option<Timestamp>,
    /// The serialized data form of the keyspace orswot set.
    #[prost(bytes = "vec", tag = "2")]
    pub set_data: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "3")]
    pub timestamp: ::core::option::Option<Timestamp>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchDocs {
    /// The keyspace to fetch the documents from.
    #[prost(string, tag = "1")]
    pub keyspace: ::prost::alloc::string::String,
    /// The set of document ids to fetch.
    #[prost(uint64, repeated, tag = "2")]
    pub doc_ids: ::prost::alloc::vec::Vec<u64>,
    #[prost(message, optional, tag = "3")]
    pub timestamp: ::core::option::Option<Timestamp>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchedDocs {
    /// The returning set of documents with their applicable data and metadata.
    #[prost(message, repeated, tag = "1")]
    pub documents: ::prost::alloc::vec::Vec<Document>,
    #[prost(message, optional, tag = "3")]
    pub timestamp: ::core::option::Option<Timestamp>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Empty {}
/// / A HLCTimestamp.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Timestamp {
    #[prost(uint64, tag = "1")]
    pub millis: u64,
    #[prost(uint32, tag = "2")]
    pub counter: u32,
    #[prost(uint32, tag = "3")]
    pub node_id: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Document {
    #[prost(message, optional, tag = "1")]
    pub metadata: ::core::option::Option<DocumentMetadata>,
    /// The raw binary data of the document's value.
    #[prost(bytes = "bytes", tag = "2")]
    pub data: ::prost::bytes::Bytes,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DocumentMetadata {
    /// The unique id of the document.
    #[prost(uint64, tag = "1")]
    pub id: u64,
    /// The timestamp of when the document was last updated.
    #[prost(message, optional, tag = "2")]
    pub last_updated: ::core::option::Option<Timestamp>,
}
/// Generated client implementations.
pub mod consistency_api_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct ConsistencyApiClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ConsistencyApiClient<tonic::transport::Channel> {
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
    impl<T> ConsistencyApiClient<T>
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
        ) -> ConsistencyApiClient<InterceptedService<T, F>>
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
            ConsistencyApiClient::new(InterceptedService::new(inner, interceptor))
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
        /// Adds a document to the state.
        pub async fn put(
            &mut self,
            request: impl tonic::IntoRequest<super::PutPayload>,
        ) -> Result<tonic::Response<super::Timestamp>, tonic::Status> {
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
                "/datacake_api.ConsistencyApi/put",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Adds a set of documents to the state.
        pub async fn multi_put(
            &mut self,
            request: impl tonic::IntoRequest<super::MultiPutPayload>,
        ) -> Result<tonic::Response<super::Timestamp>, tonic::Status> {
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
                "/datacake_api.ConsistencyApi/multi_put",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Removes a document from the state.
        pub async fn remove(
            &mut self,
            request: impl tonic::IntoRequest<super::RemovePayload>,
        ) -> Result<tonic::Response<super::Timestamp>, tonic::Status> {
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
                "/datacake_api.ConsistencyApi/remove",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Removes a set of documents from the state.
        pub async fn multi_remove(
            &mut self,
            request: impl tonic::IntoRequest<super::MultiRemovePayload>,
        ) -> Result<tonic::Response<super::Timestamp>, tonic::Status> {
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
                "/datacake_api.ConsistencyApi/multi_remove",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Applies a set of queued changed from a given node.
        pub async fn apply_batch(
            &mut self,
            request: impl tonic::IntoRequest<super::BatchPayload>,
        ) -> Result<tonic::Response<super::Timestamp>, tonic::Status> {
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
                "/datacake_api.ConsistencyApi/apply_batch",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated client implementations.
pub mod replication_api_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct ReplicationApiClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ReplicationApiClient<tonic::transport::Channel> {
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
    impl<T> ReplicationApiClient<T>
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
        ) -> ReplicationApiClient<InterceptedService<T, F>>
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
            ReplicationApiClient::new(InterceptedService::new(inner, interceptor))
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
        /// Fetches the live state of the keyspace states.
        pub async fn poll_keyspace(
            &mut self,
            request: impl tonic::IntoRequest<super::PollPayload>,
        ) -> Result<tonic::Response<super::KeyspaceInfo>, tonic::Status> {
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
                "/datacake_api.ReplicationApi/poll_keyspace",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Fetches a given ORSWOT set for a given keyspace.
        pub async fn get_state(
            &mut self,
            request: impl tonic::IntoRequest<super::GetState>,
        ) -> Result<tonic::Response<super::KeyspaceOrSwotSet>, tonic::Status> {
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
                "/datacake_api.ReplicationApi/get_state",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Fetches a given set of documents in order to bring itself up to speed.
        pub async fn fetch_docs(
            &mut self,
            request: impl tonic::IntoRequest<super::FetchDocs>,
        ) -> Result<tonic::Response<super::FetchedDocs>, tonic::Status> {
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
                "/datacake_api.ReplicationApi/fetch_docs",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod consistency_api_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with ConsistencyApiServer.
    #[async_trait]
    pub trait ConsistencyApi: Send + Sync + 'static {
        /// Adds a document to the state.
        async fn put(
            &self,
            request: tonic::Request<super::PutPayload>,
        ) -> Result<tonic::Response<super::Timestamp>, tonic::Status>;
        /// Adds a set of documents to the state.
        async fn multi_put(
            &self,
            request: tonic::Request<super::MultiPutPayload>,
        ) -> Result<tonic::Response<super::Timestamp>, tonic::Status>;
        /// Removes a document from the state.
        async fn remove(
            &self,
            request: tonic::Request<super::RemovePayload>,
        ) -> Result<tonic::Response<super::Timestamp>, tonic::Status>;
        /// Removes a set of documents from the state.
        async fn multi_remove(
            &self,
            request: tonic::Request<super::MultiRemovePayload>,
        ) -> Result<tonic::Response<super::Timestamp>, tonic::Status>;
        /// Applies a set of queued changed from a given node.
        async fn apply_batch(
            &self,
            request: tonic::Request<super::BatchPayload>,
        ) -> Result<tonic::Response<super::Timestamp>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct ConsistencyApiServer<T: ConsistencyApi> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: ConsistencyApi> ConsistencyApiServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ConsistencyApiServer<T>
    where
        T: ConsistencyApi,
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
                "/datacake_api.ConsistencyApi/put" => {
                    #[allow(non_camel_case_types)]
                    struct putSvc<T: ConsistencyApi>(pub Arc<T>);
                    impl<
                        T: ConsistencyApi,
                    > tonic::server::UnaryService<super::PutPayload> for putSvc<T> {
                        type Response = super::Timestamp;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PutPayload>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).put(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = putSvc(inner);
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
                "/datacake_api.ConsistencyApi/multi_put" => {
                    #[allow(non_camel_case_types)]
                    struct multi_putSvc<T: ConsistencyApi>(pub Arc<T>);
                    impl<
                        T: ConsistencyApi,
                    > tonic::server::UnaryService<super::MultiPutPayload>
                    for multi_putSvc<T> {
                        type Response = super::Timestamp;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::MultiPutPayload>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).multi_put(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = multi_putSvc(inner);
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
                "/datacake_api.ConsistencyApi/remove" => {
                    #[allow(non_camel_case_types)]
                    struct removeSvc<T: ConsistencyApi>(pub Arc<T>);
                    impl<
                        T: ConsistencyApi,
                    > tonic::server::UnaryService<super::RemovePayload>
                    for removeSvc<T> {
                        type Response = super::Timestamp;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RemovePayload>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).remove(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = removeSvc(inner);
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
                "/datacake_api.ConsistencyApi/multi_remove" => {
                    #[allow(non_camel_case_types)]
                    struct multi_removeSvc<T: ConsistencyApi>(pub Arc<T>);
                    impl<
                        T: ConsistencyApi,
                    > tonic::server::UnaryService<super::MultiRemovePayload>
                    for multi_removeSvc<T> {
                        type Response = super::Timestamp;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::MultiRemovePayload>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).multi_remove(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = multi_removeSvc(inner);
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
                "/datacake_api.ConsistencyApi/apply_batch" => {
                    #[allow(non_camel_case_types)]
                    struct apply_batchSvc<T: ConsistencyApi>(pub Arc<T>);
                    impl<
                        T: ConsistencyApi,
                    > tonic::server::UnaryService<super::BatchPayload>
                    for apply_batchSvc<T> {
                        type Response = super::Timestamp;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::BatchPayload>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).apply_batch(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = apply_batchSvc(inner);
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
    impl<T: ConsistencyApi> Clone for ConsistencyApiServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: ConsistencyApi> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ConsistencyApi> tonic::server::NamedService for ConsistencyApiServer<T> {
        const NAME: &'static str = "datacake_api.ConsistencyApi";
    }
}
/// Generated server implementations.
pub mod replication_api_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with ReplicationApiServer.
    #[async_trait]
    pub trait ReplicationApi: Send + Sync + 'static {
        /// Fetches the live state of the keyspace states.
        async fn poll_keyspace(
            &self,
            request: tonic::Request<super::PollPayload>,
        ) -> Result<tonic::Response<super::KeyspaceInfo>, tonic::Status>;
        /// Fetches a given ORSWOT set for a given keyspace.
        async fn get_state(
            &self,
            request: tonic::Request<super::GetState>,
        ) -> Result<tonic::Response<super::KeyspaceOrSwotSet>, tonic::Status>;
        /// Fetches a given set of documents in order to bring itself up to speed.
        async fn fetch_docs(
            &self,
            request: tonic::Request<super::FetchDocs>,
        ) -> Result<tonic::Response<super::FetchedDocs>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct ReplicationApiServer<T: ReplicationApi> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: ReplicationApi> ReplicationApiServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ReplicationApiServer<T>
    where
        T: ReplicationApi,
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
                "/datacake_api.ReplicationApi/poll_keyspace" => {
                    #[allow(non_camel_case_types)]
                    struct poll_keyspaceSvc<T: ReplicationApi>(pub Arc<T>);
                    impl<
                        T: ReplicationApi,
                    > tonic::server::UnaryService<super::PollPayload>
                    for poll_keyspaceSvc<T> {
                        type Response = super::KeyspaceInfo;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PollPayload>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).poll_keyspace(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = poll_keyspaceSvc(inner);
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
                "/datacake_api.ReplicationApi/get_state" => {
                    #[allow(non_camel_case_types)]
                    struct get_stateSvc<T: ReplicationApi>(pub Arc<T>);
                    impl<T: ReplicationApi> tonic::server::UnaryService<super::GetState>
                    for get_stateSvc<T> {
                        type Response = super::KeyspaceOrSwotSet;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetState>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_state(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = get_stateSvc(inner);
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
                "/datacake_api.ReplicationApi/fetch_docs" => {
                    #[allow(non_camel_case_types)]
                    struct fetch_docsSvc<T: ReplicationApi>(pub Arc<T>);
                    impl<T: ReplicationApi> tonic::server::UnaryService<super::FetchDocs>
                    for fetch_docsSvc<T> {
                        type Response = super::FetchedDocs;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FetchDocs>,
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
                        let method = fetch_docsSvc(inner);
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
    impl<T: ReplicationApi> Clone for ReplicationApiServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: ReplicationApi> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ReplicationApi> tonic::server::NamedService for ReplicationApiServer<T> {
        const NAME: &'static str = "datacake_api.ReplicationApi";
    }
}
