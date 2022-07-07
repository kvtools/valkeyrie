package zookeeper

import (
	"context"
	"testing"
	"time"

	"github.com/kvtools/valkeyrie"
	"github.com/kvtools/valkeyrie/store"
	"github.com/kvtools/valkeyrie/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTimeout = 60 * time.Second

const client = "localhost:2181"

func makeZkClient(t *testing.T) store.Store {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	config := &store.Config{
		ConnectionTimeout: 3 * time.Second,
	}

	kv, err := New(ctx, []string{client}, config)
	require.NoErrorf(t, err, "cannot create store")

	return kv
}

func TestRegister(t *testing.T) {
	Register()

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	kv, err := valkeyrie.NewStore(ctx, store.ZK, []string{client}, nil)
	require.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*Zookeeper); !ok {
		t.Fatal("Error registering and initializing zookeeper")
	}
}

func TestZkStore(t *testing.T) {
	kv := makeZkClient(t)
	ttlKV := makeZkClient(t)

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	testutils.RunTestLock(t, kv)
	testutils.RunTestTTL(t, kv, ttlKV)
	testutils.RunCleanup(t, kv)
}
