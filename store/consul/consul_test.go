package consul

import (
	"testing"
	"time"

	"github.com/kvtools/valkeyrie"
	"github.com/kvtools/valkeyrie/store"
	"github.com/kvtools/valkeyrie/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const client = "localhost:8500"

func makeConsulClient(t *testing.T) store.Store {
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

	kv, err := valkeyrie.NewStore(store.CONSUL, []string{client}, nil)
	require.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*Consul); !ok {
		t.Fatal("Error registering and initializing consul")
	}
}

func TestConsulStore(t *testing.T) {
	kv := makeConsulClient(t)
	lockKV := makeConsulClient(t)
	ttlKV := makeConsulClient(t)

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	testutils.RunTestLock(t, kv)
	testutils.RunTestLockTTL(t, kv, lockKV)
	testutils.RunTestTTL(t, kv, ttlKV)
	testutils.RunCleanup(t, kv)
}

func TestGetActiveSession(t *testing.T) {
	kv := makeConsulClient(t)

	consul, ok := kv.(*Consul)
	require.True(t, ok)

	key := "foo"
	value := []byte("bar")

	// Put the first key with the Ephemeral flag
	err := kv.Put(key, value, &store.WriteOptions{TTL: 2 * time.Second})
	require.NoError(t, err)

	// Session should not be empty
	session, err := consul.getActiveSession(key)
	require.NoError(t, err)
	assert.NotEqual(t, session, "")

	// Delete the key
	err = kv.Delete(key)
	require.NoError(t, err)

	// Check the session again, it should return nothing
	session, err = consul.getActiveSession(key)
	require.NoError(t, err)
	assert.Equal(t, session, "")
}
