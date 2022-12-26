# Datacake Cluster
A batteries included library for building your own distributed data stores or replicated state.

This library is largely based on the same concepts as Riak and Cassandra. Consensus, membership and failure 
detection are managed by [Quickwit's Chitchat](https://github.com/quickwit-oss/chitchat) while state alignment
and replication is managed by [Datacake CRDT](https://github.com/lnx-search/datacake/tree/main/datacake-crdt).

RPC is provided and managed entirely within Datacake using [Tonic](https://crates.io/crates/tonic) and GRPC.

This library is focused around providing a simple and easy to build framework for your distributed apps without
being overwhelming. In fact, you can be up and running just by implementing 2 async traits.

### Examples
Indepth examples [can be found here](https://github.com/lnx-search/datacake/tree/main/examples).
