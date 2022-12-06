# Datacake SQLite

A pre-built implementation of the datacake-cluster `Storage` trait, this allows you to set up
a persistent cluster immediately without any hassle of implementing a correct store.

### Setup
It's important to note that this crate does bundle SQLite with it but it can be disabled by passing
`default-features = false`.