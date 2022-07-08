// Package valkeyrie Distributed Key/Value Store Abstraction Library written in Go.
package valkeyrie

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/kvtools/valkeyrie/store"
)

// Initialize creates a new Store object, initializing the client.
type Initialize func(ctx context.Context, endpoints []string, options *store.Config) (store.Store, error)

// Backend initializers.
var initializers = make(map[store.Backend]Initialize)

// NewStore creates an instance of store.
func NewStore(ctx context.Context, backend store.Backend, endpoints []string, options *store.Config) (store.Store, error) {
	if init, exists := initializers[backend]; exists {
		return init(ctx, endpoints, options)
	}

	return nil, fmt.Errorf("%w %s", store.ErrBackendNotSupported, supportedBackend())
}

// AddStore adds a new store backend to valkeyrie.
func AddStore(backend store.Backend, init Initialize) {
	initializers[backend] = init
}

// supportedBackend returns a comma separated list of all available stores in initializers.
func supportedBackend() string {
	keys := make([]string, 0, len(initializers))
	for k := range initializers {
		keys = append(keys, string(k))
	}

	sort.Strings(keys)
	return strings.Join(keys, ", ")
}
