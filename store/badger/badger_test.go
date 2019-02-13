package badgerdb

import (
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/abronan/valkeyrie"
	"github.com/abronan/valkeyrie/store"
	"github.com/abronan/valkeyrie/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// Suppress badger log output (not configurable in 1.5.4), should be configurable in next release
	log.SetOutput(ioutil.Discard)
}

func makeBadgerBBClient(t *testing.T) (store.Store, string) {
	tmpDir, err := ioutil.TempDir("", "badger_test")
	require.NoError(t, err)

	kv, err := New([]string{tmpDir}, &store.Config{})

	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	return kv, tmpDir
}

func TestRegister(t *testing.T) {
	Register()

	tmpDir, err := ioutil.TempDir("", "badgerdbtest")
	require.NoError(t, err)

	defer os.RemoveAll(tmpDir)

	kv, err := valkeyrie.NewStore(store.BADGERDB, []string{tmpDir}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*BadgerDB); !ok {
		t.Fatal("Error registering and initializing badgerdb")
	}
}

func TestBadgerDBStore(t *testing.T) {
	kv, dir := makeBadgerBBClient(t)

	defer os.RemoveAll(dir)

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	testutils.RunCleanup(t, kv)
}

func TestBadgerDB_TTL(t *testing.T) {
	kv, dir := makeBadgerBBClient(t)

	defer os.RemoveAll(dir)

	firstKey := "testPutTTL"
	firstValue := []byte("foo")

	secondKey := "second"
	secondValue := []byte("bar")

	// Put the first key with the Ephemeral flag
	err := kv.Put(firstKey, firstValue, &store.WriteOptions{TTL: 2 * time.Second})
	assert.NoError(t, err)

	// Put a second key with the Ephemeral flag
	err = kv.Put(secondKey, secondValue, &store.WriteOptions{TTL: 2 * time.Second})
	assert.NoError(t, err)

	// Get on firstKey should work
	pair, err := kv.Get(firstKey, nil)
	assert.NoError(t, err)
	checkPairNotNil(t, pair)

	// Get on secondKey should work
	pair, err = kv.Get(secondKey, nil)
	assert.NoError(t, err)
	checkPairNotNil(t, pair)

	// Let the session expire
	time.Sleep(3 * time.Second)

	// Get on firstKey shouldn't work
	pair, err = kv.Get(firstKey, nil)
	assert.Error(t, err)
	assert.Nil(t, pair)

	// Get on secondKey shouldn't work
	pair, err = kv.Get(secondKey, nil)
	assert.Error(t, err)
	assert.Nil(t, pair)
}

func checkPairNotNil(t *testing.T, pair *store.KVPair) {
	if assert.NotNil(t, pair) {
		if !assert.NotNil(t, pair.Value) {
			t.Fatal("test failure, value is nil")
		}
	} else {
		t.Fatal("test failure, pair is nil")
	}
}

func TestBadgerDB_VersionSequence(t *testing.T) {
	kv, dir := makeBadgerBBClient(t)

	defer os.RemoveAll(dir)

	key := "/test"
	val := []byte{1, 2, 3, 4, 5}
	var prev *store.KVPair
	var err error

	for i := 0; i < 50; i++ {
		_, prev, err = kv.AtomicPut(key, val, prev, nil)
		require.NoError(t, err)
	}

	require.Equal(t, val, prev.Value)
	require.EqualValues(t, 49, prev.LastIndex)

	kv.Close()

	// should reload last version sequence
	kv, err = New([]string{dir}, nil)
	require.NoError(t, err)

	// put some random stuff
	require.NoError(t, kv.Put("/abc", []byte{1, 2}, nil))

	for i := 0; i < 50; i++ {
		_, prev, err = kv.AtomicPut(key, val, prev, nil)
		require.NoError(t, err)
	}

	require.Equal(t, val, prev.Value)
	require.EqualValues(t, 100, prev.LastIndex)

	prev, err = kv.Get(key, nil)
	require.NoError(t, err)
	require.EqualValues(t, 100, prev.LastIndex)
}
