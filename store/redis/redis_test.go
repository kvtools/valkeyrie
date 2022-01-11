package redis

import (
	"testing"

	"github.com/kvtools/valkeyrie"
	"github.com/kvtools/valkeyrie/store"
	"github.com/kvtools/valkeyrie/testutils"
	"github.com/stretchr/testify/assert"
)

const client = "localhost:6379"

func makeRedisClient(t *testing.T) store.Store {
	t.Helper()

	kv := newRedis([]string{client}, "", nil)

	// NOTE: please turn on redis's notification
	// before you using watch/watchtree/lock related features
	kv.client.ConfigSet("notify-keyspace-events", "KA")

	return kv
}

func TestRegister(t *testing.T) {
	Register()

	kv, err := valkeyrie.NewStore(store.REDIS, []string{client}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*Redis); !ok {
		t.Fatal("Error registering and initializing redis")
	}
}

func TestRedisStore(t *testing.T) {
	kv := makeRedisClient(t)
	lockTTL := makeRedisClient(t)
	kvTTL := makeRedisClient(t)

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	testutils.RunTestLock(t, kv)
	testutils.RunTestLockTTL(t, kv, lockTTL)
	testutils.RunTestTTL(t, kv, kvTTL)
	testutils.RunCleanup(t, kv)
}
