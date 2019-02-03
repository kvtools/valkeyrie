package badgerdb

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/abronan/valkeyrie"
	"github.com/abronan/valkeyrie/store"
	"github.com/dgraph-io/badger"
)

var (
	// ErrMultipleEndpointsUnsupported is thrown when multiple endpoints specified for
	// BadgerDB. Endpoint has to be a local file path
	ErrMultipleEndpointsUnsupported = errors.New("badger supports one endpoint and should be a file path")
	ErrTooManyUpdateConflicts       = errors.New("unable to commit badger update transaction: too many conflicts")
)

// modify following options to change default BadgerDB behavior
// must NOT be updated concurrently with calling New
var Options = badger.DefaultOptions
var GCInterval = 5 * time.Minute
var ConflictMaxRetries = 5

//BadgerDB type implements the Store interface
type BadgerDB struct {
	db   *badger.DB
	opts badger.Options

	// closed when not zero, when closed db is not set to nil to not race with concurrent GC loop
	closed uint32
}

// Register registers boltdb to valkeyrie
func Register() {
	valkeyrie.AddStore(store.BADGERDB, New)
}

func New(endpoints []string, _ *store.Config) (store.Store, error) {
	if len(endpoints) > 1 {
		return nil, ErrMultipleEndpointsUnsupported
	}

	dir, _ := filepath.Split(endpoints[0])
	err := os.MkdirAll(dir, 0750)
	if err != nil {
		return nil, err
	}

	opts := Options
	opts.Dir = dir
	opts.ValueDir = dir

	b := &BadgerDB{
		opts: opts,
	}

	b.db, err = badger.Open(b.opts)
	if err != nil {
		return nil, err
	}

	// TODO(os) run GC loop

	return b, nil
}

func (b *BadgerDB) Get(key string, opts *store.ReadOptions) (*store.KVPair, error) {
	kv := &store.KVPair{Key: key}

	return kv, b.db.View(func(tx *badger.Txn) error {
		it, err := tx.Get([]byte(key))
		if err != nil {
			return err
		}

		kv.Value, err = it.ValueCopy(nil)
		if err != nil {
			return err
		}

		kv.LastIndex = it.Version()

		return nil
	})
}

func (b *BadgerDB) Put(key string, value []byte, opts *store.WriteOptions) error {
	for i := 0; i < ConflictMaxRetries; i++ {
		err := b.db.Update(func(tx *badger.Txn) error {
			if opts.TTL > 0 {
				return tx.SetWithTTL([]byte(key), value, opts.TTL)
			} else {
				return tx.Set([]byte(key), value)
			}
		})

		if err != badger.ErrConflict {
			return err
		}
	}

	return ErrTooManyUpdateConflicts
}

func (b *BadgerDB) Delete(key string) (err error) {
	for i := 0; i < ConflictMaxRetries; i++ {
		err = b.db.Update(func(tx *badger.Txn) error {
			return tx.Delete([]byte(key))
		})

		if err != badger.ErrConflict {
			break
		}
	}

	return ErrTooManyUpdateConflicts
}

func (b *BadgerDB) Exists(key string, opts *store.ReadOptions) (bool, error) {
	err := b.db.View(func(tx *badger.Txn) error {
		_, err := tx.Get([]byte(key))
		return err
	})

	if err == nil {
		return true, nil
	} else if err == badger.ErrKeyNotFound {
		return false, nil
	} else {
		return false, err
	}
}

func (b *BadgerDB) List(keyPrefix string, opts *store.ReadOptions) ([]*store.KVPair, error) {
	var res []*store.KVPair

	return res, b.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte(keyPrefix)

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			kv := &store.KVPair{
				Key:       string(item.Key()),
				LastIndex: item.Version(),
			}

			body, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			kv.Value = body
			res = append(res, kv)
		}

		return nil
	})
}

func (b *BadgerDB) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	if previous == nil {
		return false, store.ErrPreviousNotSpecified
	}

	for i := 0; i < ConflictMaxRetries; i++ {
		err := b.db.Update(func(tx *badger.Txn) error {
			k := []byte(key)

			it, err := tx.Get(k)
			if err != nil {
				return err
			}

			if it.Version() != previous.LastIndex {
				return store.ErrKeyModified
			}

			return tx.Delete(k)
		})

		if err == nil {
			return true, nil
		} else if err == badger.ErrConflict {
			continue
		} else if err == badger.ErrKeyNotFound {
			return false, store.ErrKeyNotFound
		} else {
			return false, err
		}
	}

	return false, ErrTooManyUpdateConflicts
}

func (b *BadgerDB) AtomicPut(key string, value []byte, previous *store.KVPair, opts *store.WriteOptions) (bool, *store.KVPair, error) {
	if previous == nil {
		return false, nil, store.ErrPreviousNotSpecified
	}

	res := &store.KVPair{Key: key}

	for i := 0; i < ConflictMaxRetries; i++ {
		err := b.db.Update(func(tx *badger.Txn) error {
			k := []byte(key)

			it, err := tx.Get(k)
			if err != nil {
				if err == badger.ErrKeyNotFound && previous == nil {
					// OK

				} else {
					return err
				}
			}

			if previous != nil && it.Version() != previous.LastIndex {
				return store.ErrKeyModified
			}

			if opts.TTL > 0 {
				err = tx.SetWithTTL(k, value, opts.TTL)
			} else {
				err = tx.Set(k, value)
			}

			if err != nil {
				return err
			}

			// read again to get version
			it, err = tx.Get(k)
			if err != nil {
				return err
			}

			res.LastIndex = it.Version()
			return nil
		})

		if err == nil {
			// make copy of input (to be consistent with other stores that do not retain reference to input args)
			res.Value = make([]byte, len(value))
			copy(res.Value, value)
			return true, res, nil
		} else if err == badger.ErrConflict {
			continue
		} else if err == badger.ErrKeyNotFound {
			return false, nil, store.ErrKeyNotFound
		} else {
			return false, nil, err
		}
	}

	return false, nil, ErrTooManyUpdateConflicts
}

// Close the db connection to the BadgerDB
func (b *BadgerDB) Close() {
	_ = b.db.Close()
}

// DeleteTree deletes a range of keys with a given prefix
func (b *BadgerDB) DeleteTree(keyPrefix string) error {

	prefix := []byte(keyPrefix)

	// transaction may conflict
ConflictRetry:
	for i := 0; i < ConflictMaxRetries; i++ {

		// always retry when TxnTooBig is signalled
	TxnTooBigRetry:
		for {
			txn := b.db.NewTransaction(true)
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false

			it := txn.NewIterator(opts)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				err := txn.Delete(it.Item().Key())
				if err == nil {
					continue
				}

				if err == badger.ErrTxnTooBig {
					err = txn.Commit(nil)

					// commit failed with conflict
					if err == badger.ErrConflict {
						continue ConflictRetry
					}

					if err != nil {
						return err
					}

					// open new transaction and continue
					continue TxnTooBigRetry
				} else {
					return err
				}

			}

			return nil
		}
	}

	return ErrTooManyUpdateConflicts
}

// NewLock has to implemented at the library level since its not supported by BadgerDB
func (b *BadgerDB) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, store.ErrCallNotSupported
}

// Watch has to implemented at the library level since its not supported by BadgerDB
func (b *BadgerDB) Watch(key string, stopCh <-chan struct{}, opts *store.ReadOptions) (<-chan *store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

// WatchTree has to implemented at the library level since its not supported by BadgerDB
func (b *BadgerDB) WatchTree(directory string, stopCh <-chan struct{}, opts *store.ReadOptions) (<-chan []*store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}
