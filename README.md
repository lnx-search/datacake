# lnx Datacake
Easy to use tooling for building eventually consistent distributed data systems in Rust.

> "Oh consistency where art thou?" - CF.

### Features ✨
- **Simple** setup, a cluster can be setup and ready to use with one trait.
- Adjustable consistency levels when mutating state.
- Data center aware replication prioritisation. 
- Pre-built test suite for `Storage` trait implementations to ensure correct functionality.

### The packages
Datacake provides several utility libraries as well as some pre-made data store handlers:

- `datacake-crdt` - A CRDT implementation based on a hybrid logical clock (HLC) 
  provided in the form of the `HLCTimestamp`.
- `datacake-cluster` - Built on top of `datacake-crdt`, a batteries included framework
  for building eventually consistent, replicated systems where you only need to implement a basic
  storage trait.
- `datacake-sqlite` - A pre-built and tested implementation of the datacake `Storage` trait built 
  upon SQLite.

### Why does Datacake exist?

Datacake is the result of my attempts at bringing high-availability to [lnx](https://github.com/lnx-search/lnx) 
unlike languages like Erlang or Go, Rust currently has a fairly young ecosystem around distributed
systems. This makes it very hard to build a replicated system in Rust without implementing a lot of things
from scratch and without a lot of research into the area to begin with.

Currently, the main algorithms available in Rust is [Raft](https://raft.github.io/) which is replication via
consensus, overall it is a very good algorithm, and it's a very simple to understand algorithm however,
I'm not currently satisfied that the current implementations are stable enough or are maintained in order to
choose it. (Also for lnx's particular use case leader-less eventual consistency was more preferable.)

Because of the above, I built Datacake with the aim of building a reliable, well tested, eventual consistent system
akin to how Cassandra or more specifically how ScyllaDB behave with eventual consistent replication, but with a few
core differences:

- Datacake does not require an extern source or read repair to clear tombstones.
- The underlying CRDTs which are what actually power Datacake are kept purely in memory.
- Partitioning and sharding is not (currently) supported.

It's worth noting that Datacake itself does not implement the consensus and membership algorithms from scratch, instead
we use [chitchat](https://github.com/quickwit-oss/chitchat) developed by [Quickwit](https://quickwit.io/) which is an 
implementation of the scuttlebutt algorithm.

### Inspirations and references 
- [CRDTs for Mortals by James Long](https://www.youtube.com/watch?v=iEFcmfmdh2w)
- [Big(ger) Sets: Making CRDT Sets Scale in Riak by Russell Brown](https://www.youtube.com/watch?v=f20882ZSdkU)
- ["CRDTs Illustrated" by Arnout Engelen](https://www.youtube.com/watch?v=9xFfOhasiOE)
- ["Practical data synchronization with CRDTs" by Dmitry Ivanov](https://www.youtube.com/watch?v=veeWamWy8dk)
- [CRDTs and the Quest for Distributed Consistency](https://www.youtube.com/watch?v=B5NULPSiOGw)
- [Logical Physical Clocks and Consistent Snapshots in Globally Distributed Databases](https://cse.buffalo.edu/tech-reports/2014-04.pdf)

### Contributing
Contributions are always welcome, although please open an issue for an idea about extending the main cluster system
if you wish to extend or modify it heavily as something's are not always as simple as they seem.

#### What sort of things could I contribute?
🧪 Tests! 🧪 Tests! 🧪 Tests! Joking aside testing is probably the most important part of the system, extending these
tests in any way you might think of, big or small is a huge help :) 