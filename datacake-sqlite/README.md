# Datacake SQLite

A pre-built implementation of the datacake-cluster `Storage` trait, this allows you to set up
a persistent cluster immediately without any hassle of implementing a correct store.

### Setup
It's important to note that this crate does *not* bundle SQLite with it, instead you should add
```
rusqlite = { version = "0.28.0", features = ["bundled"] }
```
if you want to use the bundled version of sqlite rather than linking from some other source.