package etcdv3

import (
	"testing"
	"time"

	"github.com/kvtools/valkeyrie"
	"github.com/kvtools/valkeyrie/store"
	"github.com/kvtools/valkeyrie/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const client = "localhost:4001"

func makeEtcdV3Client(t *testing.T) store.Store {
	t.Helper()

	kv, err := New(
		[]string{client},
		&store.Config{
			ConnectionTimeout: 3 * time.Second,
			Username:          "test",
			Password:          "very-secure",
		},
	)
	require.NoErrorf(t, err, "cannot create store")

	return kv
}

func TestRegister(t *testing.T) {
	Register()

	kv, err := valkeyrie.NewStore(store.ETCDV3, []string{client}, nil)
	require.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*EtcdV3); !ok {
		t.Fatal("Error registering and initializing etcd with v3 client")
	}
}

func TestEtcdV3Store(t *testing.T) {
	kv := makeEtcdV3Client(t)
	lockKV := makeEtcdV3Client(t)
	ttlKV := makeEtcdV3Client(t)

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	testutils.RunTestLock(t, kv)
	testutils.RunTestLockTTL(t, kv, lockKV)
	testutils.RunTestListLock(t, kv)
	testutils.RunTestTTL(t, kv, ttlKV)
	testutils.RunCleanup(t, kv)
}

func TestKeepAlive(t *testing.T) {
	kv := makeEtcdV3Client(t)

	err := kv.Put("foo", []byte("bar"), &store.WriteOptions{
		TTL: 1 * time.Second,
	})
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	// The key should be gone because we didn't use KeepAlive
	pair, err := kv.Get("foo", nil)
	assert.Error(t, err, store.ErrKeyNotFound)
	assert.Nil(t, pair)

	// Put the key but now with a KeepAlive
	err = kv.Put("foo", []byte("bar"), &store.WriteOptions{
		TTL:       1 * time.Second,
		KeepAlive: true,
	})
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	// We should still be able to get the key after the TTL expires
	pair, err = kv.Get("foo", nil)
	require.NoError(t, err)
	assert.Equal(t, pair.Value, []byte("bar"))

	err = kv.Delete("foo")
	require.NoError(t, err)
}
