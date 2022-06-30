# Hourglass 
For when you need to store documents with minimal latency and high read efficiency.

### WARNING
This is primary designed to be a WORM type store and does not offer the same
level of durability of documents as a traditional database like Postgres, SQLite, etc...

Although it will guarantee that either all the documents are added in a commit
or none of them are.

## Zero to hero
Hourglass is a linux only library due to its use of io_uring, that being said,
you can run and develop with this library on Windows via WSL2 (Just make sure you
have the latest WSL installed and a kernel that supports uring.)

### Prerequisites
Make sure your kernel supports io_uring (version 5.8+)

Also ensure you have your `memlock` limit set to at least 512KB, to check this value you
can run `ulimit -l` and should look like this:
```shell
$ ulimit -l
1024
```

To set your memlock limit edit `/etc/security/limits.conf` and set the following:
```
*    hard    memlock        512
*    soft    memlock        512
```

#### Gotchas
In WSL you may need to run `su <your username>` in order to use the raised memlock size
rather than the default 64KB size.

### Setup for Linux and WSL
- Clone this repository
- Run `cargo build` to build the library. 
- Alternatively you can run `cargo test` to run the unit tests.