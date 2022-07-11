// Package boltdb contains the BoltDB store implementation.
package boltdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kvtools/valkeyrie"
	"github.com/kvtools/valkeyrie/store"
	"go.etcd.io/bbolt"
)

var (
	// ErrMultipleEndpointsUnsupported is thrown when multiple endpoints specified for BoltDB.
	// Endpoint has to be a local file path.
	ErrMultipleEndpointsUnsupported = errors.New("boltdb supports one endpoint and should be a file path")
	// ErrBoltBucketOptionMissing is thrown when boltBucket config option is missing.
	ErrBoltBucketOptionMissing = errors.New("boltBucket config option missing")
)

const (
	filePerm os.FileMode = 0o644
)

const (
	metadataLen      = 8
	transientTimeout = time.Duration(10) * time.Second
)

// Register registers boltdb to valkeyrie.
func Register() {
	valkeyrie.AddStore(store.BOLTDB, New)
}

// BoltDB type implements the Store interface.
type BoltDB struct {
	client     *bbolt.DB
	boltBucket []byte
	dbIndex    uint64
	path       string
	timeout    time.Duration
	// By default, valkeyrie opens and closes the bolt DB connection for every get/put operation.
	// This allows multiple apps to use a Bolt DB at the same time.
	// PersistConnection flag provides an option to override ths behavior.
	// ie: open the connection in New and use it till Close is called.
	PersistConnection bool
	mu                sync.Mutex
}

// New opens a new BoltDB connection to the specified path and bucket.
func New(_ context.Context, endpoints []string, options *store.Config) (store.Store, error) {
	if len(endpoints) > 1 {
		return nil, ErrMultipleEndpointsUnsupported
	}

	if options == nil || options.Bucket == "" {
		return nil, ErrBoltBucketOptionMissing
	}

	dir, _ := filepath.Split(endpoints[0])
	err := os.MkdirAll(dir, 0o750)
	if err != nil {
		return nil, err
	}

	var db *bbolt.DB
	if options.PersistConnection {
		boltOptions := &bbolt.Options{Timeout: options.ConnectionTimeout}
		db, err = bbolt.Open(endpoints[0], filePerm, boltOptions)
		if err != nil {
			return nil, err
		}
	}

	timeout := transientTimeout
	if options.ConnectionTimeout != 0 {
		timeout = options.ConnectionTimeout
	}

	b := &BoltDB{
		client:            db,
		path:              endpoints[0],
		boltBucket:        []byte(options.Bucket),
		timeout:           timeout,
		PersistConnection: options.PersistConnection,
	}

	return b, nil
}

func (b *BoltDB) reset() {
	b.path = ""
	b.boltBucket = []byte{}
}

func (b *BoltDB) getDBHandle() (*bbolt.DB, error) {
	if b.PersistConnection {
		return b.client, nil
	}

	boltOptions := &bbolt.Options{Timeout: b.timeout}
	db, err := bbolt.Open(b.path, filePerm, boltOptions)
	if err != nil {
		return nil, err
	}

	b.client = db
	return b.client, nil
}

func (b *BoltDB) releaseDBHandle() {
	if !b.PersistConnection {
		_ = b.client.Close()
	}
}

// Get the value at "key".
// BoltDB doesn't provide an inbuilt last modified index with every kv pair.
// It's implemented by an atomic counter maintained by the valkeyrie
// and appended to the value passed by the client.
func (b *BoltDB) Get(_ context.Context, key string, _ *store.ReadOptions) (*store.KVPair, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	db, err := b.getDBHandle()
	if err != nil {
		return nil, err
	}
	defer b.releaseDBHandle()

	var val []byte

	err = db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.boltBucket)
		if bucket == nil {
			return store.ErrKeyNotFound
		}

		v := bucket.Get([]byte(key))
		val = make([]byte, len(v))
		copy(val, v)

		return nil
	})

	if len(val) == 0 {
		return nil, store.ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}

	dbIndex := binary.LittleEndian.Uint64(val[:metadataLen])
	val = val[metadataLen:]

	return &store.KVPair{Key: key, Value: val, LastIndex: dbIndex}, nil
}

// Put the key, value pair.
// Index number metadata is prepended to the value.
func (b *BoltDB) Put(_ context.Context, key string, value []byte, _ *store.WriteOptions) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	dbval := make([]byte, metadataLen)

	db, err := b.getDBHandle()
	if err != nil {
		return err
	}
	defer b.releaseDBHandle()

	return db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(b.boltBucket)
		if err != nil {
			return err
		}

		dbIndex := atomic.AddUint64(&b.dbIndex, 1)
		binary.LittleEndian.PutUint64(dbval, dbIndex)
		dbval = append(dbval, value...)

		err = bucket.Put([]byte(key), dbval)
		if err != nil {
			return err
		}
		return nil
	})
}

// Delete the value for the given key.
func (b *BoltDB) Delete(_ context.Context, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	db, err := b.getDBHandle()
	if err != nil {
		return err
	}
	defer b.releaseDBHandle()

	return db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.boltBucket)
		if bucket == nil {
			return store.ErrKeyNotFound
		}
		err := bucket.Delete([]byte(key))
		return err
	})
}

// Exists checks if the key exists inside the store.
func (b *BoltDB) Exists(_ context.Context, key string, _ *store.ReadOptions) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	db, err := b.getDBHandle()
	if err != nil {
		return false, err
	}
	defer b.releaseDBHandle()

	var val []byte

	err = db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.boltBucket)
		if bucket == nil {
			return store.ErrKeyNotFound
		}

		val = bucket.Get([]byte(key))

		return nil
	})

	if len(val) == 0 {
		return false, err
	}
	return true, err
}

// List returns the range of keys starting with the passed in prefix.
func (b *BoltDB) List(_ context.Context, keyPrefix string, _ *store.ReadOptions) ([]*store.KVPair, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	kv := []*store.KVPair{}

	db, err := b.getDBHandle()
	if err != nil {
		return nil, err
	}
	defer b.releaseDBHandle()

	hasResult := false

	err = db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.boltBucket)
		if bucket == nil {
			return store.ErrKeyNotFound
		}

		cursor := bucket.Cursor()
		prefix := []byte(keyPrefix)

		for key, v := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, v = cursor.Next() {
			hasResult = true

			dbIndex := binary.LittleEndian.Uint64(v[:metadataLen])
			v = v[metadataLen:]

			val := make([]byte, len(v))
			copy(val, v)

			if string(key) != keyPrefix {
				kv = append(kv, &store.KVPair{
					Key:       string(key),
					Value:     val,
					LastIndex: dbIndex,
				})
			}
		}
		return nil
	})

	if !hasResult {
		return nil, store.ErrKeyNotFound
	}

	return kv, err
}

// AtomicDelete deletes a value at "key" if the key has not been modified in the meantime,
// throws an error if this is the case.
func (b *BoltDB) AtomicDelete(_ context.Context, key string, previous *store.KVPair) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if previous == nil {
		return false, store.ErrPreviousNotSpecified
	}

	db, err := b.getDBHandle()
	if err != nil {
		return false, err
	}
	defer b.releaseDBHandle()

	var val []byte

	err = db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.boltBucket)
		if bucket == nil {
			return store.ErrKeyNotFound
		}

		val = bucket.Get([]byte(key))
		if val == nil {
			return store.ErrKeyNotFound
		}
		dbIndex := binary.LittleEndian.Uint64(val[:metadataLen])
		if dbIndex != previous.LastIndex {
			return store.ErrKeyModified
		}

		return bucket.Delete([]byte(key))
	})
	if err != nil {
		return false, err
	}
	return true, err
}

// AtomicPut puts a value at "key"
// if the key has not been modified since the last Put,
// throws an error if this is the case.
func (b *BoltDB) AtomicPut(_ context.Context, key string, value []byte, previous *store.KVPair, opts *store.WriteOptions) (bool, *store.KVPair, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	dbval := make([]byte, metadataLen)

	db, err := b.getDBHandle()
	if err != nil {
		return false, nil, err
	}
	defer b.releaseDBHandle()

	var dbIndex uint64

	err = db.Update(func(tx *bbolt.Tx) error {
		var err error
		bucket := tx.Bucket(b.boltBucket)
		if bucket == nil {
			if previous != nil {
				return store.ErrKeyNotFound
			}
			bucket, err = tx.CreateBucket(b.boltBucket)
			if err != nil {
				return err
			}
		}

		// AtomicPut is equivalent to Put if previous is nil and the key doesn't exist in the DB.
		val := bucket.Get([]byte(key))
		if previous == nil && len(val) != 0 {
			return store.ErrKeyExists
		}

		if previous != nil {
			if len(val) == 0 {
				return store.ErrKeyNotFound
			}
			dbIndex = binary.LittleEndian.Uint64(val[:metadataLen])
			if dbIndex != previous.LastIndex {
				return store.ErrKeyModified
			}
		}

		dbIndex = atomic.AddUint64(&b.dbIndex, 1)
		binary.LittleEndian.PutUint64(dbval, b.dbIndex)
		dbval = append(dbval, value...)

		return bucket.Put([]byte(key), dbval)
	})
	if err != nil {
		return false, nil, err
	}

	updated := &store.KVPair{
		Key:       key,
		Value:     value,
		LastIndex: dbIndex,
	}

	return true, updated, nil
}

// Close the db connection to the BoltDB.
func (b *BoltDB) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.PersistConnection {
		return b.client.Close()
	}

	b.reset()
	return nil
}

// DeleteTree deletes a range of keys with a given prefix.
func (b *BoltDB) DeleteTree(_ context.Context, keyPrefix string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	db, err := b.getDBHandle()
	if err != nil {
		return err
	}
	defer b.releaseDBHandle()

	return db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.boltBucket)
		if bucket == nil {
			return store.ErrKeyNotFound
		}

		cursor := bucket.Cursor()
		prefix := []byte(keyPrefix)

		for key, _ := cursor.Seek(prefix); bytes.HasPrefix(key, prefix); key, _ = cursor.Next() {
			_ = bucket.Delete(key)
		}
		return nil
	})
}

// NewLock has to implemented at the library level since it's not supported by BoltDB.
func (b *BoltDB) NewLock(_ context.Context, _ string, _ *store.LockOptions) (store.Locker, error) {
	return nil, store.ErrCallNotSupported
}

// Watch has to implemented at the library level since it's not supported by BoltDB.
func (b *BoltDB) Watch(_ context.Context, _ string, _ *store.ReadOptions) (<-chan *store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

// WatchTree has to implemented at the library level since it's not supported by BoltDB.
func (b *BoltDB) WatchTree(_ context.Context, _ string, _ *store.ReadOptions) (<-chan []*store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}
