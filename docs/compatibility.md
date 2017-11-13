# Cross-Backend Compatibility

The benefit of `valkeyrie` is not to duplicate the code for programs that should support multiple distributed Key/Value stores such as `Consul`/`etcd`/`zookeeper`, etc.

This document offers general guidelines for users willing to support those backends with the same codebase using `valkeyrie`.

## Etcd versions

Support for etcd comes with two API versions: **APIv2** and **APIv3**. We recommend you use the new **APIv3** because of incompatibilities of **APIv2** with other stores. Use **APIv2** only if you plan to support older versions of etcd.

### Etcd APIv2 pitfalls

In the case you plan to support etcd with **APIv2**, please be aware of some pitfalls.

#### Etcd directory/key distinction

`etcd` with APIv2 makes the distinction between keys and directories. The result with `valkeyrie` is that when using the etcd driver:

- You cannot store values on directories
- You cannot invoke `WatchTree` (watching on child values), on a regular key

This is fundamental difference from stores like `Consul` and `zookeeper` which are more permissive and allow the same set of operations on keys and directories (called a Node for zookeeper).

### Put

`etcd` cannot put values on directories, so this puts a major restriction compared to `Consul` and `zookeeper`.

If you want to support all those three backends, you should make sure to only put data on **leaves**.

For example:

```go
_ := kv.Put("path/to/key/bis", []byte("foo"), nil)
_ := kv.Put("path/to/key", []byte("bar"), nil)
```

Will work on `Consul` and `zookeeper` but fail for `etcd`. This is because the first `Put` in the case of `etcd` will recursively create the directory hierarchy and `path/to/key` is now considered as a directory. Thus, values should always be stored on leaves if the support for all backends is planned.

#### WatchTree

When initializing the `WatchTree`, the natural way to do so is through the following code:

```go
key := "path/to/key"
if !kv.Exists(key, nil) {
    err := kv.Put(key, []byte("data"), nil)
}
events, err := kv.WatchTree(key, nil, nil)
```

The code above will not work across backends and etcd with **APIv2** will fail on the `WatchTree` call. What happens exactly:

- `Consul` will create a regular `key` because it has no distinction between directories and keys. This is not an issue as we can invoke `WatchTree` on regular keys.
- `zookeeper` is going to create a `node` that can either be a directory or a key during the lifetime of a program but it does not matter as a directory can hold values and be watchable like a regular key.
- `etcd` is going to create a regular `key`. We cannot invoke `WatchTree` on regular keys using etcd.

To use `WatchTree` with every supported backend, we need to enforce a parameter that is only interpreted with `etcd` **APIv2** and which asks the client to create a `directory` instead of a key.

```go
key := "path/to/key"
if !kv.Exists(key, nil) {
    // We enforce IsDir = true to make sure etcd creates a directory
    err := kv.Put(key, []byte("data"), &store.WriteOptions{IsDir:true})
}
events, err := kv.WatchTree(key, nil, nil)
```

The code above will work for all backends but make sure not to try to store any value on that path as the call to `Put` will fail for `etcd` (you can only put at `path/to/key/foo`, `path/to/key/bar` for example).
