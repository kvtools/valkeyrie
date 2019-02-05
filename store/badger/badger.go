package badgerdb

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/abronan/valkeyrie"
	"github.com/abronan/valkeyrie/store"
	"github.com/dgraph-io/badger"
)

var (
	// ErrMultipleEndpointsUnsupported is thrown when multiple endpoints specified for
	// BadgerDB. Endpoint has to be a local file path
	ErrMultipleEndpointsUnsupported = errors.New("badger: supports one endpoint and should be a file path")
	// Conflict management is left to the user of library, by default we retry operation up to defaultUpdateMaxAttempts
	ErrTooManyUpdateConflicts = errors.New("badger: too many transaction conflicts")
	ErrAlreadyClosed          = errors.New("badger: db already closed")
)

const (
	defaultUpdateMaxAttempts = 5
	defaultGCInterval        = 5 * time.Minute
	defaultGCDiscardRatio    = 0.7
	defaultNotifyChannelSize = 16
)

type (
	keyWatcher struct {
		out    chan<- *store.KVPair
		cancel <-chan struct{}
	}

	dirWatcher struct {
		prefix string
		out    chan<- []*store.KVPair
		cancel <-chan struct{}
	}

	BadgerDB struct {
		db                  *badger.DB
		opts                badger.Options
		conflictMaxAttempts int
		gcInterval          time.Duration
		gcDiscardRatio      float64

		lock        *sync.Mutex // for
		closed      bool
		keyWatchers map[string][]*keyWatcher
		dirWatchers []*dirWatcher
	}

	BadgerOpt func(b *BadgerDB)
)

var _ = store.Store(&BadgerDB{})

func WithConflictMaxAttempts(attempts int) BadgerOpt {
	return func(b *BadgerDB) {
		b.conflictMaxAttempts = attempts
	}
}

func WithGCInterval(int time.Duration) BadgerOpt {
	return func(b *BadgerDB) {
		b.gcInterval = int
	}
}

func WithGCDiscardRatio(r float64) BadgerOpt {
	return func(b *BadgerDB) {
		b.gcDiscardRatio = r
	}
}

func Register() {
	valkeyrie.AddStore(store.BADGERDB, New)
}

func New(endpoints []string, _ *store.Config) (store.Store, error) {
	if len(endpoints) > 1 {
		return nil, ErrMultipleEndpointsUnsupported
	}

	opts := badger.DefaultOptions
	opts.Dir = endpoints[0]
	opts.ValueDir = endpoints[0]

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	b := &BadgerDB{
		db:                  db,
		conflictMaxAttempts: defaultUpdateMaxAttempts,
		gcInterval:          defaultGCInterval,
		gcDiscardRatio:      defaultGCDiscardRatio,
		lock:                &sync.Mutex{},
		keyWatchers:         make(map[string][]*keyWatcher),
	}

	go b.runGcLoop()

	return b, nil
}

// Constructor that allows more fine control over options when needed
func NewBadgerDB(badgerOpts badger.Options, opts ...BadgerOpt) (*BadgerDB, error) {
	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, err
	}

	b := &BadgerDB{
		db:                  db,
		conflictMaxAttempts: defaultUpdateMaxAttempts,
		gcInterval:          defaultGCInterval,
		gcDiscardRatio:      defaultGCDiscardRatio,
		lock:                &sync.Mutex{},
		keyWatchers:         make(map[string][]*keyWatcher),
	}

	for _, o := range opts {
		o(b)
	}

	go b.runGcLoop()

	return b, nil
}

func (b *BadgerDB) runGcLoop() {
	ticker := time.NewTicker(b.gcInterval)
	defer ticker.Stop()
	for range ticker.C {
		b.lock.Lock()
		closed := b.closed
		b.lock.Unlock()
		if closed {
			break
		}

		// One call would only result in removal of at max one log file.
		// As an optimization, you could also immediately re-run it whenever it returns nil error
		//(indicating a successful value log GC), as shown below.
	again:
		err := b.db.RunValueLogGC(b.gcDiscardRatio)
		if err == nil {
			goto again
		}
	}
}

func (b *BadgerDB) notify(key string, val *store.KVPair) {
	b.lock.Lock()
	b.notifyKeyWatchers(key, val)
	b.notifyDirWatchers(key)
	b.lock.Unlock()
}

func (b *BadgerDB) Get(key string, opts *store.ReadOptions) (*store.KVPair, error) {
	kv := &store.KVPair{Key: key}

	err := b.db.View(func(tx *badger.Txn) error {
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

	if err == badger.ErrKeyNotFound {
		return nil, store.ErrKeyNotFound
	}

	return kv, err
}

func (b *BadgerDB) Put(key string, value []byte, opts *store.WriteOptions) error {
	if opts != nil && opts.IsDir {
		key = toDirectory(key)
	}

	var err error

	for i := 0; i < b.conflictMaxAttempts; i++ {
		res := &store.KVPair{Key: key}

		err := b.db.Update(func(tx *badger.Txn) error {
			k := []byte(key)

			if opts != nil && opts.TTL > 0 {
				err = tx.SetWithTTL(k, value, opts.TTL)
			} else {
				err = tx.Set(k, value)
			}

			if err != nil {
				return err
			}

			// read again to get version
			it, err := tx.Get(k)
			if err != nil {
				return err
			}

			res.LastIndex = it.Version()

			return nil
		})

		if err == nil {
			res.Value = make([]byte, len(value))
			copy(res.Value, value)
			b.notify(key, res)
		}

		if err != badger.ErrConflict {
			return err
		}
	}

	return ErrTooManyUpdateConflicts
}

func (b *BadgerDB) Delete(key string) error {
	for i := 0; i < b.conflictMaxAttempts; i++ {
		err := b.db.Update(func(tx *badger.Txn) error {
			return tx.Delete([]byte(key))
		})

		if err == nil {
			b.notify(key, nil)
		}

		if err != badger.ErrConflict {
			return err
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

func (b *BadgerDB) List(prefix string, opts *store.ReadOptions) ([]*store.KVPair, error) {
	return b.list(prefix, true)
}

func (b *BadgerDB) list(prefix string, checkRoot bool) ([]*store.KVPair, error) {
	prefix = strings.TrimSuffix(prefix, "/")

	kvs := []*store.KVPair{}
	found := false

	err := b.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte(prefix)

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			found = true
			item := it.Item()
			k := item.Key()

			// ignore self in listing
			if bytes.Equal(trimDirectoryKey(k), prefix) {
				continue
			}

			kv := &store.KVPair{
				Key:       string(k),
				LastIndex: item.Version(),
			}

			body, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			kv.Value = body
			kvs = append(kvs, kv)
		}

		return nil
	})

	if err == nil && !found && checkRoot {
		return nil, store.ErrKeyNotFound
	}

	return kvs, err
}

func (b *BadgerDB) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	if previous == nil {
		return false, store.ErrPreviousNotSpecified
	}

	for i := 0; i < b.conflictMaxAttempts; i++ {
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
			b.notify(key, nil)
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
	if opts != nil && opts.IsDir {
		key = toDirectory(key)
	}

	kv := &store.KVPair{Key: key}

	for i := 0; i < b.conflictMaxAttempts; i++ {
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

			if previous == nil && it != nil {
				return store.ErrKeyExists
			}

			if previous != nil && it.Version() != previous.LastIndex {
				return store.ErrKeyModified
			}

			if opts != nil && opts.TTL > 0 {
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

			kv.LastIndex = it.Version()
			return nil
		})

		if err == nil {
			// make copy of input (to be consistent with other stores that do not retain reference to input args)
			kv.Value = make([]byte, len(value))
			copy(kv.Value, value)

			b.notify(key, kv)

			return true, kv, nil
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
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.closed {
		return
	}

	b.closed = true

	_ = b.db.Close()

	// close all watchers
	for _, kw := range b.keyWatchers {
		for _, w := range kw {
			close(w.out)
		}
	}

	for _, dw := range b.dirWatchers {
		close(dw.out)
	}
}

// DeleteTree deletes a range of keys with a given prefix
func (b *BadgerDB) DeleteTree(keyPrefix string) (err error) {
	prefix := []byte(keyPrefix)

	defer func() {
		if err == nil {
			b.notifyDirWatchers(keyPrefix)
		}
	}()

	// transaction may conflict
ConflictRetry:
	for i := 0; i < b.conflictMaxAttempts; i++ {

		// always retry when TxnTooBig is signalled
	TxnTooBigRetry:
		for {
			txn := b.db.NewTransaction(true)
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false

			it := txn.NewIterator(opts)

			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				k := it.Item().KeyCopy(nil)

				err := txn.Delete(k)
				if err == nil {
					b.notifyKeyWatchers(string(k), nil)
					continue
				}

				it.Close()
				if err != badger.ErrTxnTooBig {
					return err
				}

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
			}

			it.Close()
			err := txn.Commit(nil)

			// commit failed with conflict
			if err == badger.ErrConflict {
				continue ConflictRetry
			}

			return err
		}
	}

	return ErrTooManyUpdateConflicts
}

// NewLock has to implemented at the library level since its not supported by BadgerDB
func (b *BadgerDB) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, store.ErrCallNotSupported
}

func (b *BadgerDB) Watch(key string, stopCh <-chan struct{}, opts *store.ReadOptions) (<-chan *store.KVPair, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.closed {
		return nil, ErrAlreadyClosed
	}

	kv, err := b.Get(key, opts)
	if err != nil {
		return nil, err
	}

	out := make(chan *store.KVPair, defaultNotifyChannelSize)
	out <- kv

	watcher := &keyWatcher{
		out:    out,
		cancel: stopCh,
	}

	b.keyWatchers[key] = append(b.keyWatchers[key], watcher)

	return out, nil
}

func (b *BadgerDB) WatchTree(prefix string, stopCh <-chan struct{}, opts *store.ReadOptions) (<-chan []*store.KVPair, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.closed {
		return nil, ErrAlreadyClosed
	}

	kvs, err := b.list(prefix, false)
	if err != nil {
		return nil, err
	}

	out := make(chan []*store.KVPair, defaultNotifyChannelSize)
	out <- kvs

	watcher := &dirWatcher{
		prefix: prefix,
		out:    out,
		cancel: stopCh,
	}

	b.dirWatchers = append(b.dirWatchers, watcher)

	return out, nil
}

func (b *BadgerDB) notifyKeyWatchers(key string, state *store.KVPair) {
	i := 0

	kv := b.keyWatchers[key]

	for _, e := range kv {
		select {
		case <-e.cancel:
			close(e.out)
			continue
		default:
		}

		if state != nil {
			select {
			case e.out <- state:
			default:
				close(e.out)
				continue
			}
		} else {
			close(e.out)
			continue
		}

		kv[i] = e
		i += 1
	}

	b.keyWatchers[key] = kv[:i]
}

func (b *BadgerDB) notifyDirWatchers(key string) {
	i := 0

	for _, e := range b.dirWatchers {
		select {
		case <-e.cancel:
			close(e.out)
			continue
		default:
		}

		if strings.HasPrefix(key, e.prefix) {
			kvs, _ := b.list(e.prefix, false)
			select {
			case e.out <- kvs:
			default:
				close(e.out)
				continue
			}
		}

		b.dirWatchers[i] = e
		i += 1
	}

	b.dirWatchers = b.dirWatchers[:i]
}

func toDirectory(key string) string {
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}

	return key
}

func trimDirectoryKey(key []byte) []byte {
	if isDirectoryKey(key) {
		return key[:len(key)-1]
	}

	return key
}

func isDirectoryKey(key []byte) bool {
	return len(key) > 0 && key[len(key)-1] == '/'
}
