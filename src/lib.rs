//! # lnx Datacake
//! Easy to use tooling for building distributed data systems in Rust.+
//!
//! > "Oh consistency where art thou?" - CF.
//!
//! ### Features ✨
//! - **Simple** setup, a cluster can be setup and ready to use with one trait.
//! - Adjustable consistency levels when mutating state.
//! - Data center aware replication prioritisation.
//! - Pre-built test suite for `Storage` trait implementations to ensure correct functionality.
//!
//! This is a convenience package which includes all of the sub-projects within
//! Datacake, realistically you probably only want some of these projects:
//!
//! ### Features
//! - `datacake_node` - The core membership system as part of datacake.
//! - `datacake_eventual_consistency` - An eventually consistent, batteries included distributed framework.
//! - `datacake_crdt` - A implementation of a ORSWOT CRDT and HLC (Hybrid Logical Clock).
//! - `datacake_sqlite` - A implementation of the `datacake_eventual_consistency::Storage` trait using SQLite.
//! - `datacake_rpc` - A fast, zero-copy RPC framework with a familiar actor-like feel.
//!
//! ### Examples
//! You can find some examples in the `tests` folders of each package and
//! also [in the dedicated examples folder](https://github.com/lnx-search/datacake/tree/main/examples)
//!

#[cfg(feature = "datacake-crdt")]
pub use datacake_crdt as crdt;
#[cfg(feature = "datacake-eventual-consistency")]
pub use datacake_eventual_consistency as eventual_consistency;
#[cfg(feature = "datacake-node")]
pub use datacake_node as node;
#[cfg(feature = "datacake-rpc")]
pub use datacake_rpc as rpc;
#[cfg(feature = "datacake-sqlite")]
pub use datacake_sqlite as sqlite;
