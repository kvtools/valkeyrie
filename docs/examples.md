# Examples

This document contains useful example of usage for `valkeyrie`.
It might not be complete but provides with general information on how to use the client.

## Create a Store and Use `Put`/`Get`/`Delete`

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/kvtools/consul"
	"github.com/kvtools/valkeyrie"
)

func main() {
	addr := "localhost:8500"

	// Initialize a new store.
	config := &consul.Config{
		ConnectionTimeout: 10 * time.Second,
	}

	kv, err := valkeyrie.NewStore(consul.StoreName, []string{addr}, config)
	if err != nil {
		log.Fatal("Cannot create store consul")
	}

	key := "foo"
	ctx := context.Background()

	err = kv.Put(ctx, key, []byte("bar"), nil)
	if err != nil {
		log.Fatalf("Error trying to put value at key: %v", key)
	}

	pair, err := kv.Get(ctx, key, nil)
	if err != nil {
		log.Fatalf("Error trying accessing value at key: %v", key)
	}

	err = kv.Delete(ctx, key)
	if err != nil {
		log.Fatalf("Error trying to delete key %v", key)
	}

	log.Printf("value: %s", string(pair.Value))
}
```

## List Keys

```go
// List will list all the keys under `key` if it contains a set of child keys/values
entries, err := kv.List(ctx, key, nil)
for _, pair := range entries {
    fmt.Printf("key=%v - value=%v", pair.Key, string(pair.Value))
}
```

## Watching for Events on a Single Key (`Watch`)

You can use watches to watch modifications on a key.
First you need to check if the key exists.
If this is not the case, we need to create it using the `Put` function.

```go
package example

import (
	"context"
	"fmt"

	"github.com/kvtools/valkeyrie/store"
)

func foo(ctx context.Context, kv store.Store, key string) error {
	// Checking on the key before watching
	exists, _ := kv.Exists(ctx, key, nil)
	if !exists {
		err := kv.Put(ctx, key, []byte("bar"), nil)
		if err != nil {
			return fmt.Errorf("something went wrong when initializing key %v", key)
		}
	}

	events, err := kv.Watch(ctx, key, nil)
	if err != nil {
		return err
	}

	for {
		select {
		case pair := <-events:
			// Do something with events
			fmt.Printf("value changed on key %v: new value=%v", key, pair.Value)
		}
	}
	// ...

	return nil
}
```

## Watching for Events Happening on Child Keys (`WatchTree`)

You can use watches to watch modifications on a key.
First you need to check if the key exists.
If this is not the case, we need to create it using the `Put` function.

There is a special step here if you are using etcd **APIv2** and if want your code to work across backends.
`etcd` with **APIv2** makes the distinction between directories and keys,
we need to make sure that the created key is considered as a directory by enforcing `IsDir` at `true`.

```go
package example

import (
	"context"
	"fmt"

	"github.com/kvtools/valkeyrie/store"
)

func foo(ctx context.Context, kv store.Store, key string) error {
	// Checking on the key before watching
	exists, _ := kv.Exists(ctx, key, nil)
	if !exists {
		// Do not forget `IsDir:true` if you are using etcd APIv2
		err := kv.Put(ctx, key, []byte("bar"), &store.WriteOptions{IsDir: true})
		if err != nil {
			return fmt.Errorf("something went wrong when initializing key %v", key)
		}
	}

	events, err := kv.WatchTree(ctx, key, nil)
	if err != nil {
		return err
	}

	select {
	case pairs := <-events:
		// Do something with events
		for _, pair := range pairs {
			fmt.Printf("value changed on key %v: new value=%v", key, pair.Value)
		}
	}

	// ...

	return nil
}
```

## Distributed Locking, Using Lock/Unlock

```go
package example

import (
	"context"
	"fmt"
	"time"

	"github.com/kvtools/valkeyrie/store"
)

func foo(ctx context.Context, kv store.Store) error {
	key := "lockKey"
	value := []byte("bar")

	// Initialize a distributed lock. TTL is optional, it is here to make sure that
	// the lock is released after the program that is holding the lock ends or crashes
	lock, err := kv.NewLock(key, &store.LockOptions{Value: value, TTL: 2 * time.Second})
	if err != nil {
		return fmt.Errorf("something went wrong when trying to initialize the Lock")
	}

	// Try to lock the key, the call to Lock() is blocking
	_, err = lock.Lock(nil)
	if err != nil {
		return fmt.Errorf("something went wrong when trying to lock key %v", key)
	}

	// Get should work because we are holding the key
	pair, err := kv.Get(ctx, key, nil)
	if err != nil {
		return fmt.Errorf("key %v has value %v", key, pair.Value)
	}

	// Unlock the key
	err = lock.Unlock(ctx)
	if err != nil {
		return fmt.Errorf("something went wrong when trying to unlock key %v", key)
	}

	// ...

	return nil
}
```
