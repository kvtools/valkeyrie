// Package redis contains the Redis store implementation.
package redis

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/kvtools/valkeyrie"
	"github.com/kvtools/valkeyrie/store"
)

const (
	noExpiration   = time.Duration(0)
	defaultLockTTL = 60 * time.Second
)

var (
	// ErrMultipleEndpointsUnsupported is thrown when there are
	// multiple endpoints specified for Redis.
	ErrMultipleEndpointsUnsupported = errors.New("redis: does not support multiple endpoints")

	// ErrTLSUnsupported is thrown when tls config is given.
	ErrTLSUnsupported = errors.New("redis does not support tls")

	// ErrAbortTryLock is thrown when a user stops trying to seek the lock
	// by sending a signal to the stop chan,
	// this is used to verify if the operation succeeded.
	ErrAbortTryLock = errors.New("redis: lock operation aborted")
)

// Register registers Redis to valkeyrie.
func Register() {
	valkeyrie.AddStore(store.REDIS, New)
}

// New creates a new Redis client given a list  of endpoints and optional tls config.
func New(ctx context.Context, endpoints []string, options *store.Config) (store.Store, error) {
	return NewWithCodec(ctx, endpoints, options, &RawCodec{})
}

// NewWithCodec creates a new Redis client with codec config.
func NewWithCodec(ctx context.Context, endpoints []string, options *store.Config, codec Codec) (store.Store, error) {
	if len(endpoints) > 1 {
		return nil, ErrMultipleEndpointsUnsupported
	}
	if options != nil && options.TLS != nil {
		return nil, ErrTLSUnsupported
	}

	var password string
	if options != nil && options.Password != "" {
		password = options.Password
	}

	return newRedis(ctx, endpoints, password, codec), nil
}

// Redis implements valkeyrie.Store interface with redis backend.
type Redis struct {
	client *redis.Client
	script *redis.Script
	codec  Codec
}

func newRedis(ctx context.Context, endpoints []string, password string, codec Codec) *Redis {
	// TODO: use *redis.ClusterClient if we support multiple endpoints.
	client := redis.NewClient(&redis.Options{
		Addr:         endpoints[0],
		DialTimeout:  5 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		Password:     password,
	})

	// Listen to Keyspace events.
	client.ConfigSet(ctx, "notify-keyspace-events", "KEA")

	var c Codec = &JSONCodec{}
	if codec != nil {
		c = codec
	}

	return &Redis{
		client: client,
		script: redis.NewScript(luaScript()),
		codec:  c,
	}
}

// Put a value at the specified key.
func (r *Redis) Put(ctx context.Context, key string, value []byte, opts *store.WriteOptions) error {
	expirationAfter := noExpiration
	if opts != nil && opts.TTL != 0 {
		expirationAfter = opts.TTL
	}

	return r.setTTL(ctx, normalize(key), &store.KVPair{
		Key:       key,
		Value:     value,
		LastIndex: sequenceNum(),
	}, expirationAfter)
}

func (r *Redis) setTTL(ctx context.Context, key string, val *store.KVPair, ttl time.Duration) error {
	valStr, err := r.codec.Encode(val)
	if err != nil {
		return err
	}

	return r.client.Set(ctx, key, valStr, ttl).Err()
}

// Get a value given its key.
func (r *Redis) Get(ctx context.Context, key string, _ *store.ReadOptions) (*store.KVPair, error) {
	return r.get(ctx, normalize(key))
}

func (r *Redis) get(ctx context.Context, key string) (*store.KVPair, error) {
	reply, err := r.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, store.ErrKeyNotFound
		}
		return nil, err
	}
	val := store.KVPair{}
	if err := r.codec.Decode(reply, &val); err != nil {
		return nil, err
	}

	if val.Key == "" {
		val.Key = key
	}

	return &val, nil
}

// Delete the value at the specified key.
func (r *Redis) Delete(ctx context.Context, key string) error {
	return r.client.Del(ctx, normalize(key)).Err()
}

// Exists verify if a Key exists in the store.
func (r *Redis) Exists(ctx context.Context, key string, _ *store.ReadOptions) (bool, error) {
	count, err := r.client.Exists(ctx, normalize(key)).Result()
	return count != 0, err
}

// Watch for changes on a key.
// glitch: we use notified-then-retrieve to retrieve *store.KVPair.
// so the responses may sometimes inaccurate.
func (r *Redis) Watch(ctx context.Context, key string, _ *store.ReadOptions) (<-chan *store.KVPair, error) {
	watchCh := make(chan *store.KVPair)
	nKey := normalize(key)

	get := getter(func() (interface{}, error) {
		pair, err := r.get(ctx, nKey)
		if err != nil {
			return nil, err
		}
		return pair, nil
	})

	push := pusher(func(v interface{}) {
		if val, ok := v.(*store.KVPair); ok {
			watchCh <- val
		}
	})

	sub := newSubscribe(ctx, r.client, regexWatch(nKey, false))

	go func(ctx context.Context, sub *subscribe, get getter, push pusher) {
		defer func() { _ = sub.Close() }()

		msgCh := sub.Receive(ctx)
		if err := watchLoop(ctx, msgCh, get, push); err != nil {
			log.Printf("watchLoop in Watch err: %v", err)
		}
	}(ctx, sub, get, push)

	return watchCh, nil
}

// WatchTree watches for changes on child nodes under a given directory.
func (r *Redis) WatchTree(ctx context.Context, directory string, _ *store.ReadOptions) (<-chan []*store.KVPair, error) {
	watchCh := make(chan []*store.KVPair)
	nKey := normalize(directory)

	get := getter(func() (interface{}, error) {
		pair, err := r.list(ctx, nKey)
		if err != nil {
			return nil, err
		}
		return pair, nil
	})

	push := pusher(func(v interface{}) {
		if p, ok := v.([]*store.KVPair); ok {
			watchCh <- p
		}
	})

	sub := newSubscribe(ctx, r.client, regexWatch(nKey, true))

	go func(ctx context.Context, sub *subscribe, get getter, push pusher) {
		defer func() { _ = sub.Close() }()

		msgCh := sub.Receive(ctx)
		if err := watchLoop(ctx, msgCh, get, push); err != nil {
			log.Printf("watchLoop in WatchTree err:%v\n", err)
		}
	}(ctx, sub, get, push)

	return watchCh, nil
}

// NewLock creates a lock for a given key.
// The returned Locker is not held and must be acquired
// with `.Lock`. The Value is optional.
func (r *Redis) NewLock(_ context.Context, key string, opts *store.LockOptions) (store.Locker, error) {
	ttl := defaultLockTTL
	var value []byte

	if opts != nil {
		if opts.TTL != 0 {
			ttl = opts.TTL
		}

		if len(opts.Value) != 0 {
			value = opts.Value
		}
	}

	return &redisLock{
		redis:    r,
		last:     nil,
		key:      key,
		value:    value,
		ttl:      ttl,
		unlockCh: make(chan struct{}),
	}, nil
}

// List the content of a given prefix.
func (r *Redis) List(ctx context.Context, directory string, _ *store.ReadOptions) ([]*store.KVPair, error) {
	return r.list(ctx, normalize(directory))
}

func (r *Redis) list(ctx context.Context, directory string) ([]*store.KVPair, error) {
	regex := scanRegex(directory) // for all keyed with $directory.
	allKeys, err := r.keys(ctx, regex)
	if err != nil {
		return nil, err
	}

	// TODO: need to handle when #key is too large.
	return r.mget(ctx, directory, allKeys...)
}

func (r *Redis) keys(ctx context.Context, regex string) ([]string, error) {
	const (
		startCursor  = 0
		endCursor    = 0
		defaultCount = 10
	)

	var allKeys []string

	keys, nextCursor, err := r.client.Scan(ctx, startCursor, regex, defaultCount).Result()
	if err != nil {
		return nil, err
	}

	allKeys = append(allKeys, keys...)

	for nextCursor != endCursor {
		keys, nextCursor, err = r.client.Scan(ctx, nextCursor, regex, defaultCount).Result()
		if err != nil {
			return nil, err
		}

		allKeys = append(allKeys, keys...)
	}

	if len(allKeys) == 0 {
		return nil, store.ErrKeyNotFound
	}

	return allKeys, nil
}

// mget values given their keys.
func (r *Redis) mget(ctx context.Context, directory string, keys ...string) ([]*store.KVPair, error) {
	replies, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	pairs := []*store.KVPair{}
	for i, reply := range replies {
		var sreply string
		if v, ok := reply.(string); ok {
			sreply = v
		}
		if sreply == "" {
			// empty reply.
			continue
		}

		pair := &store.KVPair{}
		if err := r.codec.Decode([]byte(sreply), pair); err != nil {
			return nil, err
		}

		if pair.Key == "" {
			pair.Key = keys[i]
		}

		if normalize(pair.Key) != directory {
			pairs = append(pairs, pair)
		}
	}
	return pairs, nil
}

// DeleteTree deletes a range of keys under a given directory.
// glitch: we list all available keys first and then delete them all
// it costs two operations on redis, so is not atomicity.
func (r *Redis) DeleteTree(ctx context.Context, directory string) error {
	regex := scanRegex(normalize(directory)) // for all keyed with $directory.

	allKeys, err := r.keys(ctx, regex)
	if err != nil {
		return err
	}

	return r.client.Del(ctx, allKeys...).Err()
}

// AtomicPut is an atomic CAS operation on a single value.
// Pass previous = nil to create a new key.
// We introduced script on this page, so atomicity is guaranteed.
func (r *Redis) AtomicPut(ctx context.Context, key string, value []byte, previous *store.KVPair, opts *store.WriteOptions) (bool, *store.KVPair, error) {
	expirationAfter := noExpiration
	if opts != nil && opts.TTL != 0 {
		expirationAfter = opts.TTL
	}

	newKV := &store.KVPair{
		Key:       key,
		Value:     value,
		LastIndex: sequenceNum(),
	}
	nKey := normalize(key)

	// if previous == nil, set directly.
	if previous == nil {
		if err := r.setNX(ctx, nKey, newKV, expirationAfter); err != nil {
			return false, nil, err
		}
		return true, newKV, nil
	}

	if err := r.cas(ctx, nKey, previous, newKV, formatSec(expirationAfter)); err != nil {
		return false, nil, err
	}
	return true, newKV, nil
}

func (r *Redis) setNX(ctx context.Context, key string, val *store.KVPair, expirationAfter time.Duration) error {
	valBlob, err := r.codec.Encode(val)
	if err != nil {
		return err
	}

	if !r.client.SetNX(ctx, key, valBlob, expirationAfter).Val() {
		return store.ErrKeyExists
	}
	return nil
}

func (r *Redis) cas(ctx context.Context, key string, oldPair, newPair *store.KVPair, secInStr string) error {
	newVal, err := r.codec.Encode(newPair)
	if err != nil {
		return err
	}

	oldVal, err := r.codec.Encode(oldPair)
	if err != nil {
		return err
	}

	return r.runScript(ctx, cmdCAS, key, oldVal, newVal, secInStr)
}

// AtomicDelete is an atomic delete operation on a single value
// the value will be deleted if previous matched the one stored in db.
func (r *Redis) AtomicDelete(ctx context.Context, key string, previous *store.KVPair) (bool, error) {
	if err := r.cad(ctx, normalize(key), previous); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Redis) cad(ctx context.Context, key string, old *store.KVPair) error {
	oldVal, err := r.codec.Encode(old)
	if err != nil {
		return err
	}

	return r.runScript(ctx, cmdCAD, key, oldVal)
}

// Close the store connection.
func (r *Redis) Close() error {
	return r.client.Close()
}

func (r *Redis) runScript(ctx context.Context, args ...interface{}) error {
	err := r.script.Run(ctx, r.client, nil, args...).Err()
	if err != nil && strings.Contains(err.Error(), "redis: key is not found") {
		return store.ErrKeyNotFound
	}
	if err != nil && strings.Contains(err.Error(), "redis: value has been changed") {
		return store.ErrKeyModified
	}
	return err
}

func regexWatch(key string, withChildren bool) string {
	if withChildren {
		// For all database and keys with $key prefix.
		return fmt.Sprintf("__keyspace*:%s*", key)
	}
	// For all database and keys with $key.
	return fmt.Sprintf("__keyspace*:%s", key)
}

// getter defines a func type which retrieves data from remote storage.
type getter func() (interface{}, error)

// pusher defines a func type which pushes data blob into watch channel.
type pusher func(interface{})

func watchLoop(ctx context.Context, msgCh chan *redis.Message, get getter, push pusher) error {
	// deliver the original data before we setup any events.
	pair, err := get()
	if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
		return err
	}

	if errors.Is(err, store.ErrKeyNotFound) {
		pair = &store.KVPair{}
	}

	push(pair)

	for m := range msgCh {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// retrieve and send back.
		pair, err := get()
		if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
			return err
		}

		// in case of watching a key that has been expired or deleted return and empty KV.
		if errors.Is(err, store.ErrKeyNotFound) && (m.Payload == "expired" || m.Payload == "del") {
			pair = &store.KVPair{}
		}

		push(pair)
	}

	return nil
}

type subscribe struct {
	pubsub  *redis.PubSub
	closeCh chan struct{}
}

func newSubscribe(ctx context.Context, client *redis.Client, regex string) *subscribe {
	return &subscribe{
		pubsub:  client.PSubscribe(ctx, regex),
		closeCh: make(chan struct{}),
	}
}

func (s *subscribe) Close() error {
	close(s.closeCh)
	return s.pubsub.Close()
}

func (s *subscribe) Receive(ctx context.Context) chan *redis.Message {
	msgCh := make(chan *redis.Message)
	go s.receiveLoop(ctx, msgCh)
	return msgCh
}

func (s *subscribe) receiveLoop(ctx context.Context, msgCh chan *redis.Message) {
	defer close(msgCh)

	for {
		select {
		case <-s.closeCh:
			return
		case <-ctx.Done():
			return
		default:
			msg, err := s.pubsub.ReceiveMessage(ctx)
			if err != nil {
				return
			}
			if msg != nil {
				msgCh <- msg
			}
		}
	}
}

type redisLock struct {
	redis    *Redis
	last     *store.KVPair
	unlockCh chan struct{}

	key   string
	value []byte
	ttl   time.Duration
}

func (l *redisLock) Lock(ctx context.Context) (<-chan struct{}, error) {
	lockHeld := make(chan struct{})

	success, err := l.tryLock(ctx, lockHeld)
	if err != nil {
		return nil, err
	}
	if success {
		return lockHeld, nil
	}

	// wait for changes on the key.
	watch, err := l.redis.Watch(ctx, l.key, nil)
	if err != nil {
		return nil, err
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ErrAbortTryLock
		case <-watch:
			success, err := l.tryLock(ctx, lockHeld)
			if err != nil {
				return nil, err
			}
			if success {
				return lockHeld, nil
			}
		}
	}
}

// tryLock return `true, nil` when it acquired and hold the lock
// and return `false, nil` when it can't lock now,
// and return `false, err` if any unexpected error happened underlying.
func (l *redisLock) tryLock(ctx context.Context, lockHeld chan struct{}) (bool, error) {
	success, item, err := l.redis.AtomicPut(ctx, l.key, l.value, l.last, &store.WriteOptions{
		TTL: l.ttl,
	})
	if success {
		l.last = item
		// keep holding.
		go l.holdLock(ctx, lockHeld)
		return true, nil
	}
	if errors.Is(err, store.ErrKeyNotFound) || errors.Is(err, store.ErrKeyModified) || errors.Is(err, store.ErrKeyExists) {
		return false, nil
	}
	return false, err
}

func (l *redisLock) holdLock(ctx context.Context, lockHeld chan struct{}) {
	defer close(lockHeld)

	hold := func() error {
		_, item, err := l.redis.AtomicPut(ctx, l.key, l.value, l.last, &store.WriteOptions{
			TTL: l.ttl,
		})
		if err == nil {
			l.last = item
		}
		return err
	}

	heartbeat := time.NewTicker(l.ttl / 3)
	defer heartbeat.Stop()

	for {
		select {
		case <-heartbeat.C:
			if err := hold(); err != nil {
				return
			}
		case <-l.unlockCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (l *redisLock) Unlock(ctx context.Context) error {
	l.unlockCh <- struct{}{}

	_, err := l.redis.AtomicDelete(ctx, l.key, l.last)
	if err != nil {
		return err
	}

	l.last = nil

	return nil
}

func scanRegex(directory string) string {
	return fmt.Sprintf("%s*", directory)
}

func normalize(key string) string {
	return strings.TrimPrefix(store.Normalize(key), "/")
}

func formatSec(dur time.Duration) string {
	return fmt.Sprintf("%d", int(dur/time.Second))
}

func sequenceNum() uint64 {
	// TODO: use uuid if we concerns collision probability of this number.
	return uint64(time.Now().Nanosecond())
}
