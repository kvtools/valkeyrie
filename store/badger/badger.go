package badgerdb

import (
	"errors"
	"github.com/abronan/valkeyrie"
	"github.com/abronan/valkeyrie/store"
	"github.com/dgraph-io/badger"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrMultipleEndpointsUnsupported is thrown when multiple endpoints specified for
	// BadgerDB. Endpoint has to be a local file path
	ErrMultipleEndpointsUnsupported = errors.New("badger supports one endpoint and should be a file path")
	ErrTooManyUpdateConflicts       = errors.New("unable to commit badger update transaction: too many conflicts")
)

const (
	// how many attempts on of BadgerDB conflict
	defaultUpdateMaxAttempts = 5
	defaultGCInterval        = 5 * time.Minute
	defaultGCDiscardRatio    = 0.7
)

type (
	keyWatcher struct {
		opts   *store.ReadOptions
		out    chan<- *store.KVPair
		cancel <-chan struct{}
	}

	dirWatcher struct {
		prefix string
		opts   *store.ReadOptions
		out    chan<- []*store.KVPair
		cancel <-chan struct{}
	}

	//BadgerDB type implements the Store interface
	BadgerDB struct {
		db                  *badger.DB
		opts                badger.Options
		conflictMaxAttempts int
		gcInterval          time.Duration
		gcDiscardRatio      float64

		// closed when not zero, when closed db is not set to nil to not race with concurrent GC loop
		closed uint32

		lock        *sync.Mutex // for
		keyWatchers map[string][]*keyWatcher
		dirWatchers []*dirWatcher
	}

	BadgerOpt func(b *BadgerDB)
)

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
		if atomic.LoadUint32(&b.closed) > 0 {
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

func (b *BadgerDB) Get(key string, opts *store.ReadOptions) (*store.KVPair, error) {
	key = normalize(key)

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
	key = normalize(key)

	if opts != nil && opts.IsDir {
		key = toDirectory(key)
	}

	for i := 0; i < b.conflictMaxAttempts; i++ {
		err := b.db.Update(func(tx *badger.Txn) error {
			if opts != nil && opts.TTL > 0 {
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
	key = normalize(key)

	for i := 0; i < b.conflictMaxAttempts; i++ {
		err = b.db.Update(func(tx *badger.Txn) error {
			return tx.Delete([]byte(key))
		})

		if err != badger.ErrConflict {
			return err
		}
	}

	return ErrTooManyUpdateConflicts
}

func (b *BadgerDB) Exists(key string, opts *store.ReadOptions) (bool, error) {
	key = normalize(key)

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

func (b *BadgerDB) List(directory string, opts *store.ReadOptions) ([]*store.KVPair, error) {
	directory = toDirectory(normalize(directory))

	res := []*store.KVPair{}

	err := b.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte(directory)

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			k := string(item.Key())

			// ignore self in listing
			if k == directory {
				continue
			}

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

	return res, err
}

func (b *BadgerDB) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	if previous == nil {
		return false, store.ErrPreviousNotSpecified
	}

	key = normalize(key)

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

	key = normalize(key)

	if opts != nil && opts.IsDir {
		key = toDirectory(key)
	}

	res := &store.KVPair{Key: key}

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
	atomic.StoreUint32(&b.closed, 1)

	_ = b.db.Close()
}

// DeleteTree deletes a range of keys with a given prefix
func (b *BadgerDB) DeleteTree(keyPrefix string) error {

	prefix := []byte(toDirectory(normalize(keyPrefix)))

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
				err := txn.Delete(it.Item().Key())
				if err == nil {
					continue
				}

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

			return nil
		}
	}

	return ErrTooManyUpdateConflicts
}

// NewLock has to implemented at the library level since its not supported by BadgerDB
func (b *BadgerDB) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, store.ErrCallNotSupported
}

//
func (b *BadgerDB) Watch(key string, stopCh <-chan struct{}, opts *store.ReadOptions) (<-chan *store.KVPair, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	kv, err := b.Get(key, opts)
	if err != nil {
		return nil, err
	}

	out := make(chan *store.KVPair, 1)
	out <- kv

	watcher := &keyWatcher{
		opts:   opts,
		out:    out,
		cancel: stopCh,
	}

	b.keyWatchers[key] = append(b.keyWatchers[key], watcher)

	return out, nil
}

func (b *BadgerDB) WatchTree(prefix string, stopCh <-chan struct{}, opts *store.ReadOptions) (<-chan []*store.KVPair, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	prefix = toDirectory(normalize(prefix))

	kvs, err := b.List(prefix, opts)
	if err != nil {
		return nil, err
	}

	// create small notify buffer to handle random spikes of notifications
	out := make(chan []*store.KVPair, 1)
	out <- kvs

	watcher := &dirWatcher{
		prefix: prefix,
		opts:   opts,
		out:    out,
		cancel: stopCh,
	}

	b.dirWatchers = append(b.dirWatchers, watcher)

	return out, nil
}

func (b *BadgerDB) notify(key string, p *store.KVPair) {
	b.lock.Lock()

	b.notifyKeyWatchers(key, p)
	b.notifyDirWatchers(key)

	b.lock.Unlock()
}

func (b *BadgerDB) notifyKeyWatchers(key string, p *store.KVPair) {
	i := 0

	kv := b.keyWatchers[key]

	for _, e := range kv {
		select {
		case <-e.cancel:
			close(e.out)
			continue
		default:
		}

		select {
		case e.out <- p:
		default:
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

		if !strings.HasPrefix(key, e.prefix) {
			continue
		}

		kvs, _ := b.List(e.prefix, nil)
		select {
		case e.out <- kvs:
		default:
			close(e.out)
			continue
		}

		b.dirWatchers[i] = e
		i += 1
	}

	b.dirWatchers = b.dirWatchers[:i]
}

func normalize(key string) string {
	return store.Normalize(key)
}

func toDirectory(key string) string {
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}

	return key

}
