// Package testutils test helpers.
package testutils

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kvtools/valkeyrie/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RunTestCommon tests the minimal required APIs which
// should be supported by all K/V backends.
func RunTestCommon(t *testing.T, kv store.Store) {
	t.Helper()

	testPutGetDeleteExists(t, kv)
	testList(t, kv)
	testDeleteTree(t, kv)
}

// RunTestListLock tests the list output for mutexes
// and checks that internal side keys are not listed.
func RunTestListLock(t *testing.T, kv store.Store) {
	t.Helper()

	testListLockKey(t, kv)
}

// RunTestAtomic tests the Atomic operations by the K/V
// backends.
func RunTestAtomic(t *testing.T, kv store.Store) {
	t.Helper()

	testAtomicPut(t, kv)
	testAtomicPutCreate(t, kv)
	testAtomicPutWithSlashSuffixKey(t, kv)
	testAtomicDelete(t, kv)
}

// RunTestWatch tests the watch/monitor APIs supported
// by the K/V backends.
func RunTestWatch(t *testing.T, kv store.Store) {
	t.Helper()

	testWatch(t, kv)
	testWatchTree(t, kv)
}

// RunTestLock tests the KV pair Lock/Unlock APIs supported
// by the K/V backends.
func RunTestLock(t *testing.T, kv store.Store) {
	t.Helper()

	testLockUnlock(t, kv)
}

// RunTestLockTTL tests the KV pair Lock with TTL APIs supported
// by the K/V backends.
func RunTestLockTTL(t *testing.T, kv store.Store, backup store.Store) {
	t.Helper()

	testLockTTL(t, kv, backup)
}

// RunTestTTL tests the TTL functionality of the K/V backend.
func RunTestTTL(t *testing.T, kv store.Store, backup store.Store) {
	t.Helper()

	testPutTTL(t, kv, backup)
}

func checkPairNotNil(t *testing.T, pair *store.KVPair) {
	t.Helper()

	require.NotNilf(t, pair, "pair is nil")
	require.NotNilf(t, pair.Value, "value is nil")
}

func testPutGetDeleteExists(t *testing.T, kv store.Store) {
	t.Helper()

	ctx := context.Background()

	// Get a not exist key should return ErrKeyNotFound.
	_, err := kv.Get(ctx, "testPutGetDelete_not_exist_key", nil)
	assert.ErrorIs(t, err, store.ErrKeyNotFound)

	value := []byte("bar")
	for _, key := range []string{
		"testPutGetDeleteExists",
		"testPutGetDeleteExists/",
		"testPutGetDeleteExists/testbar/",
		"testPutGetDeleteExists/testbar/testfoobar",
	} {
		// Put the key.
		err = kv.Put(ctx, key, value, nil)
		require.NoError(t, err)

		// Get should return the value and an incremented index.
		pair, err := kv.Get(ctx, key, nil)
		require.NoError(t, err)
		checkPairNotNil(t, pair)
		assert.Equal(t, pair.Value, value)
		assert.NotEqual(t, pair.LastIndex, 0)

		// Exists should return true.
		exists, err := kv.Exists(key, nil)
		require.NoError(t, err)
		assert.True(t, exists)

		// Delete the key.
		err = kv.Delete(key)
		require.NoError(t, err)

		// Get should fail.
		pair, err = kv.Get(ctx, key, nil)
		assert.Error(t, err)
		assert.Nil(t, pair)

		// Exists should return false.
		exists, err = kv.Exists(key, nil)
		require.NoError(t, err)
		assert.False(t, exists)
	}
}

func testWatch(t *testing.T, kv store.Store) {
	t.Helper()

	key := "testWatch"
	value := []byte("world")
	newValue := []byte("world!")

	ctx := context.Background()

	// Put the key.
	err := kv.Put(ctx, key, value, nil)
	require.NoError(t, err)

	stopCh := make(<-chan struct{})
	events, err := kv.Watch(key, stopCh, nil)
	require.NoError(t, err)
	require.NotNil(t, events)

	// Update loop.
	go func() {
		timeout := time.After(1 * time.Second)
		tick := time.NewTicker(250 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-timeout:
				return
			case <-tick.C:
				err := kv.Put(ctx, key, newValue, nil)
				if assert.NoError(t, err) {
					continue
				}
				return
			}
		}
	}()

	// Check for updates.
	eventCount := 1
	for {
		select {
		case event := <-events:
			assert.NotNil(t, event)
			assert.Equal(t, event.Key, key)
			if eventCount == 1 {
				assert.Equal(t, event.Value, value)
			} else {
				assert.Equal(t, event.Value, newValue)
			}
			eventCount++
			// We received all the events we wanted to check.
			if eventCount >= 4 {
				return
			}
		case <-time.After(4 * time.Second):
			t.Fatal("Timeout reached")
			return
		}
	}
}

func testWatchTree(t *testing.T, kv store.Store) {
	t.Helper()

	dir := "testWatchTree"

	node1 := "testWatchTree/node1"
	value1 := []byte("node1")

	node2 := "testWatchTree/node2"
	value2 := []byte("node2")

	node3 := "testWatchTree/node3"
	value3 := []byte("node3")

	ctx := context.Background()

	err := kv.Put(ctx, node1, value1, nil)
	require.NoError(t, err)
	err = kv.Put(ctx, node2, value2, nil)
	require.NoError(t, err)
	err = kv.Put(ctx, node3, value3, nil)
	require.NoError(t, err)

	stopCh := make(<-chan struct{})
	events, err := kv.WatchTree(dir, stopCh, nil)
	require.NoError(t, err)
	require.NotNil(t, events)

	// Update loop.
	go func() {
		time.Sleep(500 * time.Millisecond)

		err := kv.Delete(node3)
		require.NoError(t, err)
	}()

	// Check for updates.
	eventCount := 1
	for {
		select {
		case event := <-events:
			assert.NotNil(t, event)
			// We received the Delete event on a child node
			// Exit test successfully.
			if eventCount == 2 {
				return
			}
			eventCount++
		case <-time.After(4 * time.Second):
			t.Fatal("Timeout reached")
			return
		}
	}
}

func testAtomicPut(t *testing.T, kv store.Store) {
	t.Helper()

	key := "testAtomicPut"
	value := []byte("world")

	ctx := context.Background()

	// Put the key.
	err := kv.Put(ctx, key, value, nil)
	require.NoError(t, err)

	// Get should return the value and an incremented index.
	pair, err := kv.Get(ctx, key, nil)
	require.NoError(t, err)
	checkPairNotNil(t, pair)
	assert.Equal(t, pair.Value, value)
	assert.NotEqual(t, pair.LastIndex, 0)

	// This CAS should fail: previous exists.
	success, _, err := kv.AtomicPut(key, []byte("WORLD"), nil, nil)
	assert.Error(t, err)
	assert.False(t, success)

	// This CAS should succeed.
	success, _, err = kv.AtomicPut(key, []byte("WORLD"), pair, nil)
	require.NoError(t, err)
	assert.True(t, success)

	// This CAS should fail, key has wrong index.
	pair.LastIndex = 6744
	success, _, err = kv.AtomicPut(key, []byte("WORLDWORLD"), pair, nil)
	assert.Equal(t, err, store.ErrKeyModified)
	assert.False(t, success)
}

func testAtomicPutCreate(t *testing.T, kv store.Store) {
	t.Helper()

	ctx := context.Background()

	// Use a key in a new directory to ensure Stores will create directories
	// that don't yet exist.
	key := "testAtomicPutCreate/create"
	value := []byte("putcreate")

	// AtomicPut the key, previous = nil indicates create.
	success, _, err := kv.AtomicPut(key, value, nil, nil)
	require.NoError(t, err)
	assert.True(t, success)

	// Get should return the value and an incremented index.
	pair, err := kv.Get(ctx, key, nil)
	require.NoError(t, err)
	checkPairNotNil(t, pair)
	assert.Equal(t, pair.Value, value)

	// Attempting to create again should fail.
	success, _, err = kv.AtomicPut(key, value, nil, nil)
	assert.ErrorIs(t, err, store.ErrKeyExists)
	assert.False(t, success)

	// This CAS should succeed, since it has the value from Get().
	success, _, err = kv.AtomicPut(key, []byte("PUTCREATE"), pair, nil)
	require.NoError(t, err)
	assert.True(t, success)
}

func testAtomicPutWithSlashSuffixKey(t *testing.T, kv store.Store) {
	t.Helper()

	k1 := "testAtomicPutWithSlashSuffixKey/key/"
	success, _, err := kv.AtomicPut(k1, []byte{}, nil, nil)
	require.NoError(t, err)
	assert.True(t, success)
}

func testAtomicDelete(t *testing.T, kv store.Store) {
	t.Helper()

	key := "testAtomicDelete"
	value := []byte("world")

	ctx := context.Background()

	// Put the key.
	err := kv.Put(ctx, key, value, nil)
	require.NoError(t, err)

	// Get should return the value and an incremented index.
	pair, err := kv.Get(ctx, key, nil)
	require.NoError(t, err)
	checkPairNotNil(t, pair)
	assert.Equal(t, pair.Value, value)
	assert.NotEqual(t, pair.LastIndex, 0)

	tempIndex := pair.LastIndex

	// AtomicDelete should fail.
	pair.LastIndex = 6744
	success, err := kv.AtomicDelete(key, pair)
	assert.Error(t, err)
	assert.False(t, success)

	// AtomicDelete should succeed.
	pair.LastIndex = tempIndex
	success, err = kv.AtomicDelete(key, pair)
	require.NoError(t, err)
	assert.True(t, success)

	// Delete a non-existent key; should fail.
	success, err = kv.AtomicDelete(key, pair)
	assert.ErrorIs(t, err, store.ErrKeyNotFound)
	assert.False(t, success)
}

func testLockUnlock(t *testing.T, kv store.Store) {
	t.Helper()

	ctx := context.Background()

	key := "testLockUnlock"
	value := []byte("bar")

	// We should be able to create a new lock on key.
	lock, err := kv.NewLock(key, &store.LockOptions{
		Value:          value,
		TTL:            2 * time.Second,
		DeleteOnUnlock: true,
	})
	require.NoError(t, err)
	require.NotNil(t, lock)

	// Lock should successfully succeed or block.
	lockChan, err := lock.Lock(nil)
	require.NoError(t, err)
	assert.NotNil(t, lockChan)

	// Get should work.
	pair, err := kv.Get(ctx, key, nil)
	require.NoError(t, err)
	checkPairNotNil(t, pair)
	assert.Equal(t, pair.Value, value)
	assert.NotEqual(t, pair.LastIndex, 0)

	// Unlock should succeed.
	err = lock.Unlock()
	require.NoError(t, err)

	// Lock should succeed again.
	lockChan, err = lock.Lock(nil)
	require.NoError(t, err)
	assert.NotNil(t, lockChan)

	// Get should work.
	pair, err = kv.Get(ctx, key, nil)
	require.NoError(t, err)
	checkPairNotNil(t, pair)
	assert.Equal(t, pair.Value, value)
	assert.NotEqual(t, pair.LastIndex, 0)

	err = lock.Unlock()
	require.NoError(t, err)
}

func testLockTTL(t *testing.T, kv store.Store, otherConn store.Store) {
	t.Helper()

	ctx := context.Background()

	key := "testLockTTL"
	value := []byte("bar")

	renewCh := make(chan struct{})

	// We should be able to create a new lock on key.
	lock, err := otherConn.NewLock(key, &store.LockOptions{
		Value:     value,
		TTL:       2 * time.Second,
		RenewLock: renewCh,
	})
	require.NoError(t, err)
	require.NotNil(t, lock)

	// Lock should successfully succeed.
	lockChan, err := lock.Lock(nil)
	require.NoError(t, err)
	assert.NotNil(t, lockChan)

	// Get should work.
	pair, err := otherConn.Get(ctx, key, nil)
	require.NoError(t, err)
	checkPairNotNil(t, pair)
	assert.Equal(t, pair.Value, value)
	assert.NotEqual(t, pair.LastIndex, 0)

	time.Sleep(3 * time.Second)

	done := make(chan struct{})
	stop := make(chan struct{})

	value = []byte("foobar")

	// Create a new lock with another connection.
	lock, err = kv.NewLock(
		key,
		&store.LockOptions{
			Value: value,
			TTL:   3 * time.Second,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, lock)

	// Lock should block, the session on the lock
	// is still active and renewed periodically.
	go func(<-chan struct{}) {
		_, _ = lock.Lock(stop)
		done <- struct{}{}
	}(done)

	select {
	case <-done:
		t.Fatal("Lock succeeded on a key that is supposed to be locked by another client")
	case <-time.After(4 * time.Second):
		// Stop requesting the lock as we are blocked as expected.
		stop <- struct{}{}
		break
	}

	// Close the connection.
	otherConn.Close()

	// Force to stop the session renewal for the lock.
	close(renewCh)

	// Let the session on the lock expire.
	time.Sleep(3 * time.Second)
	locked := make(chan struct{})

	// Lock should now succeed for the other client.
	go func(<-chan struct{}) {
		lockChan, err = lock.Lock(nil)
		require.NoError(t, err)
		assert.NotNil(t, lockChan)
		locked <- struct{}{}
	}(locked)

	select {
	case <-locked:
		break
	case <-time.After(4 * time.Second):
		t.Fatal("Unable to take the lock, timed out")
	}

	// Get should work with the new value.
	pair, err = kv.Get(ctx, key, nil)
	require.NoError(t, err)
	checkPairNotNil(t, pair)
	assert.Equal(t, pair.Value, value)
	assert.NotEqual(t, pair.LastIndex, 0)

	err = lock.Unlock()
	require.NoError(t, err)
}

func testPutTTL(t *testing.T, kv store.Store, otherConn store.Store) {
	t.Helper()

	firstKey := "testPutTTL"
	firstValue := []byte("foo")

	secondKey := "second"
	secondValue := []byte("bar")

	ctx := context.Background()

	// Put the first key with the Ephemeral flag.
	err := otherConn.Put(ctx, firstKey, firstValue, &store.WriteOptions{TTL: 2 * time.Second})
	require.NoError(t, err)

	// Put a second key with the Ephemeral flag.
	err = otherConn.Put(ctx, secondKey, secondValue, &store.WriteOptions{TTL: 2 * time.Second})
	require.NoError(t, err)

	// Get on firstKey should work.
	pair, err := kv.Get(ctx, firstKey, nil)
	require.NoError(t, err)
	checkPairNotNil(t, pair)

	// Get on secondKey should work.
	pair, err = kv.Get(ctx, secondKey, nil)
	require.NoError(t, err)
	checkPairNotNil(t, pair)

	// Close the connection.
	otherConn.Close()

	// Let the session expire.
	time.Sleep(3 * time.Second)

	// Get on firstKey shouldn't work.
	pair, err = kv.Get(ctx, firstKey, nil)
	assert.Error(t, err)
	assert.Nil(t, pair)

	// Get on secondKey shouldn't work.
	pair, err = kv.Get(ctx, secondKey, nil)
	assert.Error(t, err)
	assert.Nil(t, pair)
}

func testList(t *testing.T, kv store.Store) {
	t.Helper()

	parentKey := "testList"
	childKey := "testList/child"
	subfolderKey := "testList/subfolder"

	ctx := context.Background()

	// Put the parent key.
	err := kv.Put(ctx, parentKey, nil, &store.WriteOptions{IsDir: true})
	require.NoError(t, err)

	// Put the first child key.
	err = kv.Put(ctx, childKey, []byte("first"), nil)
	require.NoError(t, err)

	// Put the second child key which is also a directory.
	err = kv.Put(ctx, subfolderKey, []byte("second"), &store.WriteOptions{IsDir: true})
	require.NoError(t, err)

	// Put child keys under secondKey.
	for i := 1; i <= 3; i++ {
		key := "testList/subfolder/key" + strconv.Itoa(i)
		err = kv.Put(ctx, key, []byte("value"), nil)
		require.NoError(t, err)
	}

	// List should work and return five child entries.
	for _, parent := range []string{parentKey, parentKey + "/"} {
		pairs, err := kv.List(parent, nil)
		require.NoError(t, err)
		assert.Len(t, pairs, 5)
	}

	// List on childKey should return 0 keys.
	pairs, err := kv.List(childKey, nil)
	require.NoError(t, err)
	assert.NotNil(t, pairs)
	assert.Empty(t, pairs)

	// List on subfolderKey should return 3 keys without the directory.
	pairs, err = kv.List(subfolderKey, nil)
	require.NoError(t, err)
	assert.Len(t, pairs, 3)

	// List should fail: the key does not exist.
	pairs, err = kv.List("idontexist", nil)
	assert.ErrorIs(t, err, store.ErrKeyNotFound)
	assert.Nil(t, pairs)
}

func testListLockKey(t *testing.T, kv store.Store) {
	t.Helper()

	listKey := "testListLockSide"

	ctx := context.Background()

	err := kv.Put(ctx, listKey, []byte("val"), &store.WriteOptions{IsDir: true})
	require.NoError(t, err)

	err = kv.Put(ctx, listKey+"/subfolder", []byte("val"), &store.WriteOptions{IsDir: true})
	require.NoError(t, err)

	// Put keys under subfolder.
	for i := 1; i <= 3; i++ {
		key := listKey + "/subfolder/key" + strconv.Itoa(i)
		err := kv.Put(ctx, key, []byte("val"), nil)
		require.NoError(t, err)

		// We lock the child key.
		lock, err := kv.NewLock(key, &store.LockOptions{Value: []byte("locked"), TTL: 2 * time.Second})
		require.NoError(t, err)
		require.NotNil(t, lock)

		lockChan, err := lock.Lock(nil)
		require.NoError(t, err)
		assert.NotNil(t, lockChan)
	}

	// List children of the root directory (`listKey`), this should
	// not output any `___lock` entries and must contain 4 results.
	pairs, err := kv.List(listKey, nil)
	require.NoError(t, err)
	assert.Len(t, pairs, 4)

	for _, pair := range pairs {
		if strings.Contains(pair.Key, "___lock") {
			assert.FailNow(t, "tesListLockKey: found a key containing lock suffix '___lock'")
		}
	}
}

func testDeleteTree(t *testing.T, kv store.Store) {
	t.Helper()

	prefix := "testDeleteTree"

	firstKey := "testDeleteTree/first"
	firstValue := []byte("first")

	secondKey := "testDeleteTree/second"
	secondValue := []byte("second")

	ctx := context.Background()

	// Put the first key.
	err := kv.Put(ctx, firstKey, firstValue, nil)
	require.NoError(t, err)

	// Put the second key.
	err = kv.Put(ctx, secondKey, secondValue, nil)
	require.NoError(t, err)

	// Get should work on the first Key.
	pair, err := kv.Get(ctx, firstKey, nil)
	require.NoError(t, err)
	checkPairNotNil(t, pair)
	assert.Equal(t, pair.Value, firstValue)
	assert.NotEqual(t, 0, pair.LastIndex)

	// Get should work on the second Key.
	pair, err = kv.Get(ctx, secondKey, nil)
	require.NoError(t, err)
	checkPairNotNil(t, pair)
	assert.Equal(t, pair.Value, secondValue)
	assert.NotEqual(t, 0, pair.LastIndex)

	// Delete Values under directory `nodes`.
	err = kv.DeleteTree(prefix)
	require.NoError(t, err)

	// Get should fail on both keys.
	pair, err = kv.Get(ctx, firstKey, nil)
	assert.Error(t, err)
	assert.Nil(t, pair)

	pair, err = kv.Get(ctx, secondKey, nil)
	assert.Error(t, err)
	assert.Nil(t, pair)
}

// RunCleanup cleans up keys introduced by the tests.
func RunCleanup(t *testing.T, kv store.Store) {
	t.Helper()

	for _, key := range []string{
		"testAtomicPutWithSlashSuffixKey",
		"testPutGetDeleteExists",
		"testWatch",
		"testWatchTree",
		"testAtomicPut",
		"testAtomicPutCreate",
		"testAtomicDelete",
		"testLockUnlock",
		"testLockTTL",
		"testPutTTL",
		"testList/subfolder",
		"testList",
		"testListLockSide/subfolder",
		"testListLockSide",
		"testDeleteTree",
	} {
		err := kv.DeleteTree(key)
		if err != nil {
			assert.ErrorIsf(t, err, store.ErrKeyNotFound, "failed to delete tree key %s", key)
		}

		err = kv.Delete(key)
		if err != nil {
			assert.ErrorIsf(t, err, store.ErrKeyNotFound, "failed to delete key %s", key)
		}
	}
}
