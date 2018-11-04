package aerospike

import (
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/abronan/valkeyrie"
	"github.com/abronan/valkeyrie/store"
	api "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
)

// Aerospike is the receiver type for the
// Store interface
type Aerospike struct {
	sync.Mutex
	client    *api.Client
	namespace string
}

var (
	// ErrAerospikeNamespaceMissing is thrown when Namespace config option is missing
	ErrAerospikeNamespaceMissing = errors.New("Namespace (Bucket) config option missing")
)

// Register registers aerospike to valkeyrie
func Register() {
	valkeyrie.AddStore(store.AEROSPIKE, New)
}

// New creates a new Aerospike client given a list
// of endpoints and optional tls config
func New(endpoints []string, options *store.Config) (store.Store, error) {

	s := &Aerospike{}

	hosts := []*api.Host{}
	for _, endpoint := range endpoints {
		h := strings.Split(endpoint, ":")
		p, err := strconv.Atoi(h[1])
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, api.NewHost(h[0], p))
	}

	// Set options
	if (options == nil) || (len(options.Bucket) == 0) {
		return nil, ErrAerospikeNamespaceMissing
	}
	s.namespace = options.Bucket
	policy := api.NewClientPolicy()
	if options != nil {
		if options.TLS != nil {
			policy.TlsConfig = options.TLS
		}
		if options.ConnectionTimeout != 0 {
			policy.Timeout = options.ConnectionTimeout
		}
		if options.Username != "" {
			policy.User = options.Username
		}
		if options.Password != "" {
			policy.Password = options.Password
		}
		if options.SyncPeriod != 0 {
			policy.TendInterval = options.SyncPeriod
		}
	}

	// Creates a new client
	client, err := api.NewClientWithPolicyAndHost(policy, hosts...)
	if err != nil {
		return nil, err
	}
	s.client = client

	return s, nil
}

// newKey initializes a key from namespace
func (s *Aerospike) newKey(key string) (*api.Key, error) {
	var akey *api.Key
	ikey, err := strconv.ParseInt(key, 10, 64)
	if err == nil {
		akey, err = api.NewKey(s.namespace, "", ikey)
		if err != nil {
			return nil, err
		}
	} else {
		akey, err = api.NewKey(s.namespace, "", key)
		if err != nil {
			return nil, err
		}
	}

	return akey, err
}

// Get the value at "key", returns the last modified index
// to use in conjunction to CAS calls
func (s *Aerospike) Get(key string, opts *store.ReadOptions) (*store.KVPair, error) {
	policy := api.NewPolicy()
	akey, err := s.newKey(key)
	if err != nil {
		return nil, err
	}

	record, err := s.client.Get(policy, akey)
	if record == nil {
		return nil, store.ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}

	return &store.KVPair{Key: key, Value: record.Bins["bin"].([]byte), LastIndex: uint64(record.Generation)}, nil
}

// Put a value at "key"
func (s *Aerospike) Put(key string, value []byte, opts *store.WriteOptions) error {
	akey, err := s.newKey(key)
	if err != nil {
		return err
	}

	policy := api.NewWritePolicy(0, 0)
	policy.SendKey = true
	if opts != nil && opts.TTL > 0 {
		policy.Expiration = uint32(opts.TTL / time.Second)
	}
	bin := api.NewBin("bin", value)
	err = s.client.PutBins(policy, akey, bin)
	return err
}

// Delete a value at "key"
func (s *Aerospike) Delete(key string) error {
	akey, err := s.newKey(key)
	if err != nil {
		return err
	}
	exists, err := s.client.Delete(nil, akey)
	if !exists {
		return store.ErrKeyNotFound
	}
	return err
}

// Exists checks that the key exists inside the store
func (s *Aerospike) Exists(key string, opts *store.ReadOptions) (bool, error) {
	policy := api.NewPolicy()

	akey, err := s.newKey(key)
	if err != nil {
		return false, err
	}

	exists, err := s.client.Exists(policy, akey)
	if !exists {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// List records of a given substring
func (s *Aerospike) List(keyPrefix string, opts *store.ReadOptions) ([]*store.KVPair, error) {
	stmt := api.NewStatement(s.namespace, "")
	// stmt.SetPredExp(
	// 	api.NewPredExpStringBin("PK"),
	// 	api.NewPredExpStringValue(keyPrefix+".*"),
	// 	api.NewPredExpStringRegex(3),
	// )
	rs, err := s.client.Query(nil, stmt)
	if err != nil {
		return nil, err
	}

	kv := []*store.KVPair{}

	hasResult := false
	for res := range rs.Results() {
		if res.Err != nil {
			return nil, res.Err
		}
		if res.Record.Key.Value() != nil && res.Record.Key.Value().String() != keyPrefix {
			hasResult = true
			if strings.HasPrefix(res.Record.Key.Value().String(), keyPrefix) {
				kv = append(kv, &store.KVPair{
					Key:       res.Record.Key.Value().String(),
					Value:     res.Record.Bins["bin"].([]byte),
					LastIndex: uint64(res.Record.Generation),
				})
			}
		}
	}

	if !hasResult {
		return kv, store.ErrKeyNotFound
	}

	return kv, nil
}

// DeleteTree deletes a range of keys under a given substring
func (s *Aerospike) DeleteTree(keyFilter string) error {
	rs, err := s.List(keyFilter, nil)
	if err != nil {
		return err
	}
	for _, res := range rs {
		err = s.Delete(res.Key)
		if err != nil {
			return err
		}
	}
	return nil
}

// AtomicPut put a value at "key" if the key has not been
// modified in the meantime, throws an error if this is the case
func (s *Aerospike) AtomicPut(key string, value []byte, previous *store.KVPair, opts *store.WriteOptions) (bool, *store.KVPair, error) {
	akey, err := s.newKey(key)
	if err != nil {
		return false, nil, err
	}

	policy := api.NewWritePolicy(0, 0)
	policy.SendKey = true
	if opts != nil && opts.TTL > 0 {
		policy.Expiration = uint32(opts.TTL / time.Second)
	}
	if previous != nil {
		policy.GenerationPolicy = api.EXPECT_GEN_EQUAL
		policy.Generation = uint32(previous.LastIndex)
	} else {
		policy.RecordExistsAction = api.CREATE_ONLY
	}

	bin := api.NewBin("bin", value)
	err = s.client.PutBins(policy, akey, bin)
	if err != nil {
		if ae, ok := err.(types.AerospikeError); ok && ae.ResultCode() == types.KEY_EXISTS_ERROR {
			return false, nil, store.ErrKeyExists
		}
		return false, nil, store.ErrKeyModified
	}

	updated := &store.KVPair{
		Key:       key,
		Value:     value,
		LastIndex: uint64(atomic.AddUint32(&policy.Generation, 1)),
	}

	return true, updated, nil
}

// AtomicDelete deletes a value at "key" if the key has not
// been modified in the meantime, throws an error if this is the case
func (s *Aerospike) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	if previous == nil {
		return false, store.ErrKeyModified
	}

	// Extra Get operation to check on the key
	pair, err := s.Get(key, nil)
	if err != nil {
		return false, err
	}
	if pair.LastIndex != previous.LastIndex {
		return false, store.ErrKeyModified
	}

	if err := s.Delete(key); err != nil {
		return false, err
	}

	return true, nil
}

// Close closes the client connection
func (s *Aerospike) Close() {
	s.client.Close()
}

// NewLock has to implemented at the library level since its not supported by Aerospike
func (s *Aerospike) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, store.ErrCallNotSupported
}

// Watch has to implemented at the library level since its not supported by Aerospike
func (s *Aerospike) Watch(key string, stopCh <-chan struct{}, opts *store.ReadOptions) (<-chan *store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

// WatchTree has to implemented at the library level since its not supported by Aerospike
func (s *Aerospike) WatchTree(directory string, stopCh <-chan struct{}, opts *store.ReadOptions) (<-chan []*store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}
