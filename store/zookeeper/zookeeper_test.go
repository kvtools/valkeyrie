package zookeeper

import (
	"testing"
	"time"

	"github.com/kvtools/valkeyrie"
	"github.com/kvtools/valkeyrie/store"
	"github.com/kvtools/valkeyrie/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const client = "localhost:2181"

func makeZkClient(t *testing.T) store.Store {
	t.Helper()

	kv, err := New(
		[]string{client},
		&store.Config{
			ConnectionTimeout: 3 * time.Second,
		},
	)
	require.NoErrorf(t, err, "cannot create store")

	return kv
}

func TestRegister(t *testing.T) {
	Register()

	kv, err := valkeyrie.NewStore(store.ZK, []string{client}, nil)
	assert.NoError(t, err)
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
