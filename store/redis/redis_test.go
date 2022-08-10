package redis

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

const client = "localhost:6379"

func makeRedisClient(t *testing.T) store.Store {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	kv := newRedis(ctx, []string{client}, nil, "", nil)

	// NOTE: please turn on redis's notification
	// before you using watch/watchtree/lock related features.
	kv.client.ConfigSet(ctx, "notify-keyspace-events", "KA")

	return kv
}

func TestRegister(t *testing.T) {
	Register()

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	kv, err := valkeyrie.NewStore(ctx, store.REDIS, []string{client}, nil)
	require.NoError(t, err)
	assert.NotNil(t, kv)

	assert.IsTypef(t, kv, new(Redis), "Error registering and initializing Redis")
}

func TestRedisStore(t *testing.T) {
	kv := makeRedisClient(t)
	lockTTL := makeRedisClient(t)
	kvTTL := makeRedisClient(t)

	t.Cleanup(func() {
		testutils.RunCleanup(t, kv)
	})

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	testutils.RunTestLock(t, kv)
	testutils.RunTestLockTTL(t, kv, lockTTL)
	testutils.RunTestTTL(t, kv, kvTTL)
}
