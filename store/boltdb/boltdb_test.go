package boltdb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/kvtools/valkeyrie"
	"github.com/kvtools/valkeyrie/store"
	"github.com/kvtools/valkeyrie/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTimeout = 60 * time.Second

func makeBoltDBClient(t *testing.T) store.Store {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	config := &store.Config{Bucket: "boltDBTest"}

	kv, err := New(ctx, []string{"/tmp/not_exist_dir/__boltdbtest"}, config)
	require.NoErrorf(t, err, "cannot create store")

	return kv
}

func TestRegister(t *testing.T) {
	Register()

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	config := &store.Config{Bucket: "boltDBTest"}

	kv, err := valkeyrie.NewStore(ctx, store.BOLTDB, []string{"/tmp/not_exist_dir/__boltdbtest"}, config)
	require.NoError(t, err)
	require.NotNil(t, kv)

	assert.IsTypef(t, kv, new(BoltDB), "Error registering and initializing boltDB")

	_ = os.Remove("/tmp/not_exist_dir/__boltdbtest")
}

// TestMultiplePersistConnection tests the second connection to a
// BoltDB fails when one is already open with PersistConnection flag.
func TestMultiplePersistConnection(t *testing.T) {
	config := &store.Config{
		Bucket:            "boltDBTest",
		ConnectionTimeout: 1 * time.Second,
		PersistConnection: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	kv, err := valkeyrie.NewStore(ctx, store.BOLTDB, []string{"/tmp/not_exist_dir/__boltdbtest"}, config)
	require.NoError(t, err)
	assert.NotNil(t, kv)

	assert.IsTypef(t, kv, new(BoltDB), "Error registering and initializing boltDB")

	// Must fail if multiple boltdb requests are made with a valid timeout.
	_, err = valkeyrie.NewStore(ctx, store.BOLTDB, []string{"/tmp/not_exist_dir/__boltdbtest"}, config)
	assert.Error(t, err)

	_ = os.Remove("/tmp/not_exist_dir/__boltdbtest")
}

// TestConcurrentConnection tests simultaneous get/put using
// two handles.
func TestConcurrentConnection(t *testing.T) {
	config := &store.Config{
		Bucket:            "boltDBTest",
		ConnectionTimeout: 1 * time.Second,
	}

	t.Cleanup(func() {
		_ = os.Remove("/tmp/__boltdbtest")
	})

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	kv1, err := valkeyrie.NewStore(ctx, store.BOLTDB, []string{"/tmp/__boltdbtest"}, config)
	require.NoError(t, err)
	assert.NotNil(t, kv1)

	kv2, err := valkeyrie.NewStore(ctx, store.BOLTDB, []string{"/tmp/__boltdbtest"}, config)
	require.NoError(t, err)
	assert.NotNil(t, kv2)

	key1 := "TestKV1"
	value1 := []byte("TestVal1")
	err = kv1.Put(ctx, key1, value1, nil)
	require.NoError(t, err)

	key2 := "TestKV2"
	value2 := []byte("TestVal2")
	err = kv2.Put(ctx, key2, value2, nil)
	require.NoError(t, err)

	pair1, err := kv1.Get(ctx, key1, nil)
	require.NoError(t, err)
	require.NotNil(t, pair1)
	assert.Equal(t, pair1.Value, value1)

	pair2, err := kv2.Get(ctx, key2, nil)
	require.NoError(t, err)
	require.NotNil(t, pair2)
	assert.Equal(t, pair2.Value, value2)

	// AtomicPut using kv1 and kv2 should succeed.
	_, _, err = kv1.AtomicPut(ctx, key1, []byte("TestnewVal1"), pair1, nil)
	require.NoError(t, err)

	_, _, err = kv2.AtomicPut(ctx, key2, []byte("TestnewVal2"), pair2, nil)
	require.NoError(t, err)

	testutils.RunTestCommon(t, kv1)
	testutils.RunTestCommon(t, kv2)

	_ = kv1.Close()
	_ = kv2.Close()
}

func TestBoltDBStore(t *testing.T) {
	kv := makeBoltDBClient(t)

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)

	_ = os.Remove("/tmp/not_exist_dir/__boltdbtest")
}

func TestGetAllKeys(t *testing.T) {
	kv := makeBoltDBClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	t.Cleanup(func() {
		_ = kv.Delete(ctx, "key1")
	})

	err := kv.Put(ctx, "key1", []byte("value1"), &store.WriteOptions{})
	require.NoError(t, err)

	pairs, err := kv.List(ctx, "", nil)
	require.NoError(t, err)
	assert.Len(t, pairs, 1)
}
