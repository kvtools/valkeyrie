package aerospike

import (
	"testing"
	"time"

	"github.com/abronan/valkeyrie"
	"github.com/abronan/valkeyrie/store"
	"github.com/abronan/valkeyrie/testutils"
	"github.com/stretchr/testify/assert"
)

var (
	client = "localhost:3000"
)

func makeAerospikeClient(t *testing.T) store.Store {

	kv, err := New(
		[]string{client},
		&store.Config{
			Bucket:            "test",
			ConnectionTimeout: 3 * time.Second,
		},
	)

	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	return kv
}

func TestRegister(t *testing.T) {
	Register()

	kv, err := valkeyrie.NewStore(
		store.AEROSPIKE,
		[]string{client},
		&store.Config{
			Bucket: "test",
		},
	)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*Aerospike); !ok {
		t.Fatal("Error registering and initializing Aerospike")
	}
}

func TestAerospikeStore(t *testing.T) {
	kv := makeAerospikeClient(t)
	ttlKV := makeAerospikeClient(t)

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestTTL(t, kv, ttlKV)
	testutils.RunCleanup(t, kv)
}
