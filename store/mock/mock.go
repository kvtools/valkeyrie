// Package mock Mocks all Store functions using testify.Mock.
package mock

import (
	"context"

	"github.com/kvtools/valkeyrie/store"
	"github.com/stretchr/testify/mock"
)

// Mock store. Mocks all Store functions using testify.Mock.
type Mock struct {
	mock.Mock

	// Endpoints passed to InitializeMock.
	Endpoints []string

	// Options passed to InitializeMock.
	Options *store.Config
}

// New creates a Mock store.
func New(_ context.Context, endpoints []string, options *store.Config) (store.Store, error) {
	s := &Mock{}
	s.Endpoints = endpoints
	s.Options = options
	return s, nil
}

// Put mock.
func (s *Mock) Put(_ context.Context, key string, value []byte, opts *store.WriteOptions) error {
	args := s.Mock.Called(key, value, opts)
	return args.Error(0)
}

// Get mock.
func (s *Mock) Get(_ context.Context, key string, opts *store.ReadOptions) (*store.KVPair, error) {
	args := s.Mock.Called(key, opts)
	return args.Get(0).(*store.KVPair), args.Error(1)
}

// Delete mock.
func (s *Mock) Delete(_ context.Context, key string) error {
	args := s.Mock.Called(key)
	return args.Error(0)
}

// Exists mock.
func (s *Mock) Exists(_ context.Context, key string, opts *store.ReadOptions) (bool, error) {
	args := s.Mock.Called(key, opts)
	return args.Bool(0), args.Error(1)
}

// Watch mock.
func (s *Mock) Watch(ctx context.Context, key string, opts *store.ReadOptions) (<-chan *store.KVPair, error) {
	args := s.Mock.Called(key, opts)
	return args.Get(0).(<-chan *store.KVPair), args.Error(1)
}

// WatchTree mock.
func (s *Mock) WatchTree(_ context.Context, prefix string, opts *store.ReadOptions) (<-chan []*store.KVPair, error) {
	args := s.Mock.Called(prefix, opts)
	return args.Get(0).(chan []*store.KVPair), args.Error(1)
}

// NewLock mock.
func (s *Mock) NewLock(_ context.Context, key string, opts *store.LockOptions) (store.Locker, error) {
	args := s.Mock.Called(key, opts)
	return args.Get(0).(store.Locker), args.Error(1)
}

// List mock.
func (s *Mock) List(_ context.Context, prefix string, opts *store.ReadOptions) ([]*store.KVPair, error) {
	args := s.Mock.Called(prefix, opts)
	return args.Get(0).([]*store.KVPair), args.Error(1)
}

// DeleteTree mock.
func (s *Mock) DeleteTree(_ context.Context, prefix string) error {
	args := s.Mock.Called(prefix)
	return args.Error(0)
}

// AtomicPut mock.
func (s *Mock) AtomicPut(_ context.Context, key string, value []byte, previous *store.KVPair, opts *store.WriteOptions) (bool, *store.KVPair, error) {
	args := s.Mock.Called(key, value, previous, opts)
	return args.Bool(0), args.Get(1).(*store.KVPair), args.Error(2)
}

// AtomicDelete mock.
func (s *Mock) AtomicDelete(_ context.Context, key string, previous *store.KVPair) (bool, error) {
	args := s.Mock.Called(key, previous)
	return args.Bool(0), args.Error(1)
}

// Lock mock implementation of Locker.
type Lock struct {
	mock.Mock
}

// Lock mock.
func (l *Lock) Lock(ctx context.Context) (<-chan struct{}, error) {
	args := l.Mock.Called()
	return args.Get(0).(<-chan struct{}), args.Error(1)
}

// Unlock mock.
func (l *Lock) Unlock(_ context.Context) error {
	args := l.Mock.Called()
	return args.Error(0)
}

// Close mock.
func (s *Mock) Close() error { return nil }
