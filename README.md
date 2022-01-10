# valkeyrie

[![GoDoc](https://godoc.org/github.com/abronan/valkeyrie?status.png)](https://godoc.org/github.com/abronan/valkeyrie)
[![Build Status](https://github.com/abronan/valkeyrie/actions/workflows/build.yml/badge.svg)](https://github.com/abronan/valkeyrie/actions/workflows/build.yml)
[![Coverage Status](https://coveralls.io/repos/abronan/valkeyrie/badge.svg)](https://coveralls.io/r/abronan/valkeyrie)
[![Go Report Card](https://goreportcard.com/badge/github.com/abronan/valkeyrie)](https://goreportcard.com/report/github.com/abronan/valkeyrie)

`valkeyrie` provides a `Go` native library to store metadata using Distributed Key/Value stores (or common databases).

Its goal is to abstract common store operations (Get, Put, List, etc.) for multiple Key/Value store backends.

For example, you can easily implement a generic *Leader Election* algorithm on top of it (see the [docker/leadership](https://github.com/docker/leadership) repository).

## Examples of usage

You can refer to [Examples](https://github.com/abronan/valkeyrie/blob/master/docs/examples.md) for a basic overview of the library.

## Compatibility

A **storage backend** in `valkeyrie` implements (fully or partially) the [Store](https://github.com/abronan/valkeyrie/blob/master/store/store.go#L69) interface.

| Calls                 |      Consul       |     Etcd      |     Zookeeper      |       Redis       |     BoltDB      |      DynamoDB     |
|-----------------------|:-----------------:|:-------------:|:------------------:|:-----------------:|:---------------:|:-----------------:|
| Put                   |     &#10003;      |   &#10003;    |      &#10003;      |      &#10003;     |    &#10003;     |      &#10003;     |
| Get                   |     &#10003;      |   &#10003;    |      &#10003;      |      &#10003;     |    &#10003;     |      &#10003;     |
| Delete                |     &#10003;      |   &#10003;    |      &#10003;      |      &#10003;     |    &#10003;     |      &#10003;     |
| Exists                |     &#10003;      |   &#10003;    |      &#10003;      |      &#10003;     |    &#10003;     |      &#10003;     |
| Watch                 |     &#10003;      |   &#10003;    |      &#10003;      |      &#10003;     |                 |                   |
| WatchTree             |     &#10003;      |   &#10003;    |      &#10003;      |      &#10003;     |                 |                   |
| NewLock (Lock/Unlock) |     &#10003;      |   &#10003;    |      &#10003;      |      &#10003;     |                 |      &#10003;     |
| List                  |     &#10003;      |   &#10003;    |      &#10003;      |      &#10003;     |    &#10003;     |      &#10003;     |
| DeleteTree            |     &#10003;      |   &#10003;    |      &#10003;      |      &#10003;     |    &#10003;     |      &#10003;     |
| AtomicPut             |     &#10003;      |   &#10003;    |      &#10003;      |      &#10003;     |    &#10003;     |      &#10003;     |
| AtomicDelete          |     &#10003;      |   &#10003;    |      &#10003;      |      &#10003;     |    &#10003;     |      &#10003;     |

## Supported versions

- **Consul** versions >= `0.5.1` because it uses Sessions with `Delete` behavior for the use of `TTLs` (mimics zookeeper's Ephemeral node support), If you don't plan to use `TTLs`: you can use Consul version `0.4.0+`.
- **Etcd** versions >= `2.0` with **APIv2** (*deprecated*) and >= `3.0` **APIv3** (*recommended*).
- **Zookeeper** versions >= `3.4.5`.
- **Redis** versions >= `3.2.6`. [Key space notification](https://redis.io/topics/notifications) needs to be enabled to have access to Watch and Lock methods.
- **Boltdb** and **DynamoDB** shouldn't be subject to any version dependencies.

## Limitations

Distributed Key/Value stores often have different concepts for managing and formatting keys and their associated values. Even though `valkeyrie` tries to abstract those stores aiming for some consistency, in some cases it can't be applied easily.

Please refer to the [docs/compatibility.md](https://github.com/abronan/valkeyrie/blob/master/docs/compatibility.md) file to see what are the special cases for cross-backend compatibility.

Calls like `WatchTree` may return different events (or number of events) depending on the backend (for now, `Etcd` and `Consul` will likely return more events than `Zookeeper` that you should triage properly).

## Security

Only `Consul` and `etcd` have support for secure communication and you should build and provide your own `config.TLS` object to feed the client. Support is planned for `zookeeper` and `redis`.

## Contributing

Want to contribute to valkeyrie? Take a look at the [Contribution Guidelines](https://github.com/abronan/valkeyrie/blob/master/CONTRIBUTING.md).

## Maintainers

- [Alexandre Beslic](https://github.com/abronan)
- [Victor Castell](https://github.com/victorcoder)
- [Nicolas Mengin](https://github.com/nmengin)
- [Maciej Winnicki](https://github.com/mthenw)

## Copyright and license

Apache License Version 2.0
