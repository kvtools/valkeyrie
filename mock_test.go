package valkeyrie

import (
	"context"

	"github.com/kvtools/valkeyrie/store"
)

const testStoreName = "mock"

func newStore(ctx context.Context, endpoints []string, options Config) (store.Store, error) {
	cfg, ok := options.(*Config)
	if !ok && cfg != nil {
		return nil, &store.InvalidConfigurationError{Store: testStoreName, Config: options}
	}

	return New(ctx, endpoints, cfg)
}

type Mock struct {
	cfg *Config
}

// New creates a new Mock client.
//
//nolint:gocritic
func New(_ context.Context, _ []string, cfg *Config) (*Mock, error) {
	return &Mock{cfg: cfg}, nil
}

func (m Mock) Put(_ context.Context, _ string, _ []byte, _ *store.WriteOptions) error {
	panic("implement me")
}

func (m Mock) Get(_ context.Context, _ string, _ *store.ReadOptions) (*store.KVPair, error) {
	panic("implement me")
}

func (m Mock) Delete(_ context.Context, _ string) error {
	panic("implement me")
}

func (m Mock) Exists(_ context.Context, _ string, _ *store.ReadOptions) (bool, error) {
	panic("implement me")
}

func (m Mock) Watch(_ context.Context, _ string, _ *store.ReadOptions) (<-chan *store.KVPair, error) {
	panic("implement me")
}

func (m Mock) WatchTree(_ context.Context, _ string, _ *store.ReadOptions) (<-chan []*store.KVPair, error) {
	panic("implement me")
}

func (m Mock) NewLock(_ context.Context, _ string, _ *store.LockOptions) (store.Locker, error) {
	panic("implement me")
}

func (m Mock) List(_ context.Context, _ string, _ *store.ReadOptions) ([]*store.KVPair, error) {
	panic("implement me")
}

func (m Mock) DeleteTree(_ context.Context, _ string) error {
	panic("implement me")
}

func (m Mock) AtomicPut(_ context.Context, _ string, _ []byte, _ *store.KVPair, _ *store.WriteOptions) (bool, *store.KVPair, error) {
	panic("implement me")
}

func (m Mock) AtomicDelete(_ context.Context, _ string, _ *store.KVPair) (bool, error) {
	panic("implement me")
}

func (m Mock) Close() error {
	panic("implement me")
}
