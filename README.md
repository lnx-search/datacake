# Datacake
Easy to use tooling for building eventually consistent distributed data systems in Rust.

**WARNING: This is still a WIP, nothing is guaranteed to stay the same or be bug free**

Datacake provides several utility libraries as well as some pre-made data store handlers:

- `datacake-crdt` - A ORSWOT CRDT implementation based on a hybrid logical clock (HLC) 
  provided in the form of the `HLCTimestamp`
- `datacake-cluster` - Built on top of datacake-crdt, this implements all required RPC, 
  gossip and consensus agreements for a distributed datastore.


### TODO list
_In no particular order_

- [x] Add basic working example.
- [x] Add unit tests to RPC layer.
- [x] Add integration/unit tests for cluster manager.
- [ ] Add ready to go RocksDB implementation.
- [x] Test and check HLCTimestamp implementation for correctness.
