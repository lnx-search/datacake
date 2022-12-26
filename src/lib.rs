//! # lnx Datacake
//! Easy to use tooling for building distributed data systems in Rust.
//!
//! This is a convenience package which includes all of the sub-projects within
//! Datacake, realistically you probably only want some of these projects:
//!
//! ### Features
//! - `datacake_cluster` - An eventually consistent, batteries included distributed framework.
//! - `datacake_crdt` - A implementation of a ORSWOT CRDT and HLC (Hybrid Logical Clock).
//! - `datacake_sqlite` - A implementation of the `datacake_cluster::Storage` trait using SQLite.
//! - `datacake_rpc` - A fast, zero-copy RPC framework with a familiar actor-like feel.

#[cfg(feature = "datacake-node")]
pub use datacake_node as node;
#[cfg(feature = "datacake-rpc")]
pub use datacake_rpc as rpc;
#[cfg(feature = "datacake-crdt")]
pub use datacake_crdt as crdt;
#[cfg(feature = "datacake-cluster")]
pub use datacake_cluster as cluster;
#[cfg(feature = "datacake-sqlite")]
pub use datacake_sqlite as sqlite;
