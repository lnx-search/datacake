# Replicated KV store.

This is a nice little example of a KV store implemented as two basic HTTP endpoints.

## Running
### Single node cluster
```shell
cargo run --release -- --node-id 1 --data-dir "./my-data"
```

This will spawn a single node with an ID of `node-1` and store the data in the `my-data` directory which will be
created if it doesn't already exist.


### 3 node cluster
```shell
cargo run --release -- --node-id 1 --data-dir "./my-data/node-1-data --rest-listen-addr 127.0.0.1:8000 --cluster-listen-addr 127.0.0.1:8001 --seed 127.0.0.1:8003 --seed 127.0.0.1:8005"
cargo run --release -- --node-id 2 --data-dir "./my-data/node-2-data --rest-listen-addr 127.0.0.1:8002 --cluster-listen-addr 127.0.0.1:8003 --seed 127.0.0.1:8001 --seed 127.0.0.1:8005"
cargo run --release -- --node-id 3 --data-dir "./my-data/node-3-data --rest-listen-addr 127.0.0.1:8004 --cluster-listen-addr 127.0.0.1:8005 --seed 127.0.0.1:8001 --seed 127.0.0.1:8003"
```

This will start a local 3 node cluster, for simplicity we've set the seeds to be each node's peers rather although this does
not need to be every node in the cluster, normally 2 or 3 is sufficient for any size cluster larger than 3 nodes.

## Sending Requests
Now we have a running cluster we can send requests with the request system of our choice, 
you'll want be sure to use the node's `rest-listen-addr` when sending the API calls.

The API has the following endpoints:
- GET `/:keyspace/:key` - Get and existing document from the store for the given keyspace and key.
- POST `/:keyspace/:key` - Set a document value in the store.

You can use a keyspace to organise and scale your storage, access to a single keyspace is always serial, 
but they are cheap to create and use, so you can offset this issue by just partitioning data across several
keyspace sets.