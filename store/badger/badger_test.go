package badgerdb

import (
	"github.com/abronan/valkeyrie"
	"github.com/abronan/valkeyrie/store"
	"github.com/abronan/valkeyrie/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func makeBadgerBBClient(t *testing.T) store.Store {
	tmpDir, err := ioutil.TempDir("", "badger_test")
	require.NoError(t, err)

	kv, err := New([]string{tmpDir}, &store.Config{})

	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	return kv
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

func TestZkStore(t *testing.T) {
	kv := makeBadgerBBClient(t)

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	testutils.RunTestTTL(t, kv, kv)
	testutils.RunCleanup(t, kv)
}
