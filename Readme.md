# MDBX-Remote

`mdbx-remote` is a tool to access your MDBX databases over network safely without any concern of corrupting the databases or getting dirty data. `mdbx-remote` also provides the _same_ but _async_ programming interface as the original `libmdbx-rs` bindings, which _passes_ all tests of origin `libmdbx-rs`.

## Motivation

[MDBX](https://libmdbx.dqdkfa.ru/) is a fast and easy-to-use KV database offering ACID properties. However, one of the limitations of MDBX is that it tries hard to prevent users from accessing the database via network to avoid potential corruption. For example, opening a database in the common collaborative mode will immediately get `MDBX_EREMOTE`. Even if you open the database via `MDBX_EXCLUSIVE`, it is still highly likely to corrupt the database or get inconsistent data if there are remote readers, which MDBX can not always detect. This is super inconvenient in many cases, like just changing one or two entries.

`mdbx-remote` solves this by spinning up an RPC service to expose all raw MDBX primitives.

## Usages

### Build

Simply do

```
cargo build --release
```

Note `libclang-dev` is required to generate the libmdbx bindings.

### MDBX Utility

`mdbx-remote` is shipped with an `mdbx` utlity. For example, spin up an RPC endpoint:

```
# By default, this will listen at 0.0.0.0:1899
RUST_LOG=mdbx=info cargo run --release server
```

And stat any database with a url:

```
# Stat a remote reth database
cargo run --release mdbx://127.0.0.1/remote/path/to/db?ro=1

Database:
	Page size: 4096
	Entries: 19265135020
	Leaf Pages: 365773779
	Branch Pages: 23212389
	Depth: 102
	Overflow Pages: 5270586
Table Main:
	Page size: 4096
	Entries: 29
	Leaf Pages: 1
	Branch Pages: 0
	Depth: 1
	Overflow Pages: 0
Table AccountChangeSets:
	Page size: 4096
	Entries: 3298505382
	Leaf Pages: 42669897
	Branch Pages: 13964000
	Depth: 4
	Overflow Pages: 0
...
```

This exactly mimics the output of `mdbx_stat` utility from `libmdbx`. Note the `mdbx://` URL supports a subset of MDBX features by parameters including:

- `ro`: Read-only
- `rw`: Read-write
- `accede`: Use options from existing environment
- `exclusive`: Open in the exclusive mode
- `no_sub_dir`: No directory for the mdbx database
- `max_readers`: Max readers
- `max_dbs`: Max databases
- `deadline`: The seconds to wait before giving up an RPC request

### Access Reth Database

One of the motivation to build `mdbx-remote` is to access reth database. `mdbx reth` can get a value from one reth table.

```
cargo run --release reth \
    -u mdbx://127.0.0.1/path/to/reth/db \
    -t PlainAccountState \
    0x95222290DD7278Aa3Ddd389Cc1E1d165CC4BAfe5

The account is:
{
  "nonce": 1569575,
  "balance": "0x7db63f1578c045b9",
  "bytecode_hash": null
}
```

### For Developers

`mdbx-remote` reaches almost the full parity with `libmdbx-rs` interfaces, except forcing everything to _async_ because of `tarpc`. Generally, the table below shows the types for different scenarios.

||Environment|Database|Transaction|Cursor|
|-|-|-|-|-|
|Local|`Environment`|`Database`|`Transaction<K>`|`Cursor<K>`|
|Remote|`RemoteEnvironment`|`RemoteDatabase`|`RemoteTransaction<K>`|`RemoteCursor<K>`|
|Local or Remote|`EnvironmentAny`|`DatabaseAny`|`TransactionAny<K>`|`CursorAny<K>`|

## Caveats

- Only expose the RPC endpoint to trusted network because it doesn't have any protection or authentication. Tunnel the traffic via something like SSH if you really would like to expose it to public network.
- The same database can't be opened twice as inherited from MDBX.

## TODO

- Implmenet more MDBX utilities, especially `mdbx_dump`.
- Reach full parity with `libmdbx-rs`.
- Reach more parity with `reth db` operations.
- Benchmark the performance.

## Credits

The bindings is forked from reth v1.1.0.
