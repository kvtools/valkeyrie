package boltdb

import (
	"os"
	"testing"
	"time"

	"github.com/kvtools/valkeyrie"
	"github.com/kvtools/valkeyrie/store"
	"github.com/kvtools/valkeyrie/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeBoltDBClient(t *testing.T) store.Store {
	t.Helper()

	kv, err := New([]string{"/tmp/not_exist_dir/__boltdbtest"}, &store.Config{Bucket: "boltDBTest"})
	require.NoErrorf(t, err, "cannot create store")

	return kv
}

func TestRegister(t *testing.T) {
	Register()

	kv, err := valkeyrie.NewStore(
		store.BOLTDB,
		[]string{"/tmp/not_exist_dir/__boltdbtest"},
		&store.Config{Bucket: "boltDBTest"},
	)
	require.NoError(t, err)
	require.NotNil(t, kv)

	assert.IsTypef(t, kv, new(BoltDB), "Error registering and initializing boltDB")

	_ = os.Remove("/tmp/not_exist_dir/__boltdbtest")
}

// TestMultiplePersistConnection tests the second connection to a
// BoltDB fails when one is already open with PersistConnection flag.
func TestMultiplePersistConnection(t *testing.T) {
	kv, err := valkeyrie.NewStore(
		store.BOLTDB,
		[]string{"/tmp/not_exist_dir/__boltdbtest"},
		&store.Config{
			Bucket:            "boltDBTest",
			ConnectionTimeout: 1 * time.Second,
			PersistConnection: true,
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, kv)

	assert.IsTypef(t, kv, new(BoltDB), "Error registering and initializing boltDB")

	// Must fail if multiple boltdb requests are made with a valid timeout.
	_, err = valkeyrie.NewStore(
		store.BOLTDB,
		[]string{"/tmp/not_exist_dir/__boltdbtest"},
		&store.Config{
			Bucket:            "boltDBTest",
			ConnectionTimeout: 1 * time.Second,
			PersistConnection: true,
		},
	)
	assert.Error(t, err)

	_ = os.Remove("/tmp/not_exist_dir/__boltdbtest")
}

// TestConcurrentConnection tests simultaneous get/put using
// two handles.
func TestConcurrentConnection(t *testing.T) {
	kv1, err := valkeyrie.NewStore(
		store.BOLTDB,
		[]string{"/tmp/__boltdbtest"},
		&store.Config{
			Bucket:            "boltDBTest",
			ConnectionTimeout: 1 * time.Second,
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, kv1)

	kv2, err := valkeyrie.NewStore(
		store.BOLTDB,
		[]string{"/tmp/__boltdbtest"},
		&store.Config{
			Bucket:            "boltDBTest",
			ConnectionTimeout: 1 * time.Second,
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, kv2)

	key1 := "TestKV1"
	value1 := []byte("TestVal1")
	err = kv1.Put(key1, value1, nil)
	require.NoError(t, err)

	key2 := "TestKV2"
	value2 := []byte("TestVal2")
	err = kv2.Put(key2, value2, nil)
	require.NoError(t, err)

	pair1, err := kv1.Get(key1, nil)
	require.NoError(t, err)
	require.NotNil(t, pair1)
	assert.Equal(t, pair1.Value, value1)

	pair2, err := kv2.Get(key2, nil)
	require.NoError(t, err)
	require.NotNil(t, pair2)
	assert.Equal(t, pair2.Value, value2)

	// AtomicPut using kv1 and kv2 should succeed.
	_, _, err = kv1.AtomicPut(key1, []byte("TestnewVal1"), pair1, nil)
	require.NoError(t, err)

	_, _, err = kv2.AtomicPut(key2, []byte("TestnewVal2"), pair2, nil)
	require.NoError(t, err)

	testutils.RunTestCommon(t, kv1)
	testutils.RunTestCommon(t, kv2)

	kv1.Close()
	kv2.Close()

	_ = os.Remove("/tmp/__boltdbtest")
}

func TestBoltDBStore(t *testing.T) {
	kv := makeBoltDBClient(t)

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)

	_ = os.Remove("/tmp/not_exist_dir/__boltdbtest")
}

func TestGetAllKeys(t *testing.T) {
	kv := makeBoltDBClient(t)

	t.Cleanup(func() {
		_ = kv.Delete("key1")
	})

	err := kv.Put("key1", []byte("value1"), &store.WriteOptions{})
	require.NoError(t, err)

	pairs, err := kv.List("", nil)
	require.NoError(t, err)
	assert.Len(t, pairs, 1)
}
