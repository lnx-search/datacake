# Datacake
Easy to use tooling for building eventually consistent distributed data systems in Rust.

Datacake provides several utility libraries as well as some pre-made data store handlers:

- `datacake-crdt` - A ORSWOT CRDT implementation based on a hybrid logical clock (HLC) 
  provided in the form of the `HLCTimestamp`
- `datacake-cluster` - Built on top of datacake-crdt, this implements all required RPC, 
  gossip and consensus agreements for a distributed datastore.