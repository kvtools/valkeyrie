# Examples

This document contains useful example of usage for `valkeyrie`. It might not be complete but provides with general informations on how to use the client.

## Create a store and use Put/Get/Delete

```go
package main

import (
    "fmt"
    "time"
    "log"

    "github.com/abronan/valkeyrie"
    "github.com/abronan/valkeyrie/store"
    "github.com/abronan/valkeyrie/store/boltdb"
    "github.com/abronan/valkeyrie/store/consul"
    "github.com/abronan/valkeyrie/store/etcd/v3"
    "github.com/abronan/valkeyrie/store/zookeeper"
    "github.com/abronan/valkeyrie/store/redis"
)

func init() {
    // Register consul store to valkeyrie
    consul.Register()

    // We can register more backends that are supported by
    // valkeyrie if we plan to use these
    etcdv3.Register()
    zookeeper.Register()
    boltdb.Register()
    redis.Register()
}

func main() {
    client := "localhost:8500"

    // Initialize a new store with consul
    kv, err := valkeyrie.NewStore(
        store.CONSUL, // or "consul"
        []string{client},
        &store.Config{
            ConnectionTimeout: 10*time.Second,
        },
    )
    if err != nil {
        log.Fatal("Cannot create store consul")
    }

    key := "foo"
    err = kv.Put(key, []byte("bar"), nil)
    if err != nil {
        fmt.Errorf("Error trying to put value at key: %v", key)
    }

    pair, err := kv.Get(key, nil)
    if err != nil {
        fmt.Errorf("Error trying accessing value at key: %v", key)
    }

    err = kv.Delete(key)
    if err != nil {
        fmt.Errorf("Error trying to delete key %v", key)
    }

    log.Info("value: ", string(pair.Value))
}
```

## List keys

```go
// List will list all the keys under `key` if it contains a set of child keys/values
entries, err := kv.List(key, nil)
for _, pair := range entries {
    fmt.Printf("key=%v - value=%v", pair.Key, string(pair.Value))
}

```

## Watching for events on a single key (Watch)

You can use watches to watch modifications on a key. First you need to check if the key exists. If this is not the case, we need to create it using the `Put` function.

```go
// Checking on the key before watching
if !kv.Exists(key, nil) {
    err := kv.Put(key, []byte("bar"), nil)
    if err != nil {
        fmt.Errorf("Something went wrong when initializing key %v", key)
    }
}

stopCh := make(<-chan struct{})
events, err := kv.Watch(key, stopCh, nil)

for {
    select {
        case pair := <-events:
            // Do something with events
            fmt.Printf("value changed on key %v: new value=%v", key, pair.Value)
    }
}

```

## Watching for events happening on child keys (WatchTree)

You can use watches to watch modifications on a key. First you need to check if the key exists. If this is not the case, we need to create it using the `Put` function. There is a special step here if you are using etcd **APIv2** and if want your code to work across backends. `etcd` with **APIv2** makes the distinction between directories and keys, we need to make sure that the created key is considered as a directory by enforcing `IsDir` at `true`.

```go
// Checking on the key before watching
if !kv.Exists(key, nil) {
    // Do not forget `IsDir:true` if you are using etcd APIv2
    err := kv.Put(key, []byte("bar"), &store.WriteOptions{IsDir:true})
    if err != nil {
        fmt.Errorf("Something went wrong when initializing key %v", key)
    }
}

stopCh := make(<-chan struct{})
events, err := kv.WatchTree(key, stopCh, nil)

select {
    case pairs := <-events:
        // Do something with events
        for _, pair := range pairs {
            fmt.Printf("value changed on key %v: new value=%v", key, pair.Value)
        }
}

```

## Distributed Locking, using Lock/Unlock

```go
key := "lockKey"
value := []byte("bar")

// Initialize a distributed lock. TTL is optional, it is here to make sure that
// the lock is released after the program that is holding the lock ends or crashes
lock, err := kv.NewLock(key, &store.LockOptions{Value: value, TTL: 2 * time.Second})
if err != nil {
    fmt.Errorf("something went wrong when trying to initialize the Lock")
}

// Try to lock the key, the call to Lock() is blocking
_, err := lock.Lock(nil)
if err != nil {
    fmt.Errorf("something went wrong when trying to lock key %v", key)
}

// Get should work because we are holding the key
pair, err := kv.Get(key, nil)
if err != nil {
    fmt.Errorf("key %v has value %v", key, pair.Value)
}

// Unlock the key
err = lock.Unlock()
if err != nil {
    fmt.Errorf("something went wrong when trying to unlock key %v", key)
}
```
