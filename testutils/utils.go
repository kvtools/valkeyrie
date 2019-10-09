package testutils

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/abronan/valkeyrie/store"
	"github.com/stretchr/testify/assert"
)

// RunTestCommon tests the minimal required APIs which
// should be supported by all K/V backends
func RunTestCommon(t *testing.T, kv store.Store) {
	testPutGetDeleteExists(t, kv)
	testList(t, kv)
	testDeleteTree(t, kv)
}

// RunTestListLock tests the list output for mutexes
// and checks that internal side keys are not listed
func RunTestListLock(t *testing.T, kv store.Store) {
	testListLockKey(t, kv)
}

// RunTestAtomic tests the Atomic operations by the K/V
// backends
func RunTestAtomic(t *testing.T, kv store.Store) {
	testAtomicPut(t, kv)
	testAtomicPutCreate(t, kv)
	testAtomicPutWithSlashSuffixKey(t, kv)
	testAtomicDelete(t, kv)
}

// RunTestWatch tests the watch/monitor APIs supported
// by the K/V backends.
func RunTestWatch(t *testing.T, kv store.Store) {
	testWatch(t, kv)
	testWatchTree(t, kv)
}

// RunTestLock tests the KV pair Lock/Unlock APIs supported
// by the K/V backends.
func RunTestLock(t *testing.T, kv store.Store) {
	testLockUnlock(t, kv)
}

// RunTestLockTTL tests the KV pair Lock with TTL APIs supported
// by the K/V backends.
func RunTestLockTTL(t *testing.T, kv store.Store, backup store.Store) {
	testLockTTL(t, kv, backup)
}

// RunTestTTL tests the TTL functionality of the K/V backend.
func RunTestTTL(t *testing.T, kv store.Store, backup store.Store) {
	testPutTTL(t, kv, backup)
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

func testPutGetDeleteExists(t *testing.T, kv store.Store) {
	// Get a not exist key should return ErrKeyNotFound
	pair, err := kv.Get("testPutGetDelete_not_exist_key", nil)
	assert.Equal(t, store.ErrKeyNotFound, err)

	value := []byte("bar")
	for _, key := range []string{
		"testPutGetDeleteExists",
		"testPutGetDeleteExists/",
		"testPutGetDeleteExists/testbar/",
		"testPutGetDeleteExists/testbar/testfoobar",
	} {

		// Put the key
		err = kv.Put(key, value, nil)
		assert.NoError(t, err)

		// Get should return the value and an incremented index
		pair, err = kv.Get(key, nil)
		assert.NoError(t, err)
		checkPairNotNil(t, pair)
		assert.Equal(t, pair.Value, value)
		assert.NotEqual(t, pair.LastIndex, 0)

		// Exists should return true
		exists, err := kv.Exists(key, nil)
		assert.NoError(t, err)
		assert.True(t, exists)

		// Delete the key
		err = kv.Delete(key)
		assert.NoError(t, err)

		// Get should fail
		pair, err = kv.Get(key, nil)
		assert.Error(t, err)
		assert.Nil(t, pair)
		assert.Nil(t, pair)

		// Exists should return false
		exists, err = kv.Exists(key, nil)
		assert.NoError(t, err)
		assert.False(t, exists)
	}
}

func testWatch(t *testing.T, kv store.Store) {
	key := "testWatch"
	value := []byte("world")
	newValue := []byte("world!")

	// Put the key
	err := kv.Put(key, value, nil)
	assert.NoError(t, err)

	stopCh := make(<-chan struct{})
	events, err := kv.Watch(key, stopCh, nil)
	assert.NoError(t, err)
	assert.NotNil(t, events)

	// Update loop
	go func() {
		timeout := time.After(1 * time.Second)
		tick := time.Tick(250 * time.Millisecond)
		for {
			select {
			case <-timeout:
				return
			case <-tick:
				err := kv.Put(key, newValue, nil)
				if assert.NoError(t, err) {
					continue
				}
				return
			}
		}
	}()

	// Check for updates
	eventCount := 1
	for {
		select {
		case event := <-events:
			assert.NotNil(t, event)
			if eventCount == 1 {
				assert.Equal(t, event.Key, key)
				assert.Equal(t, event.Value, value)
			} else {
				assert.Equal(t, event.Key, key)
				assert.Equal(t, event.Value, newValue)
			}
			eventCount++
			// We received all the events we wanted to check
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
	dir := "testWatchTree"

	node1 := "testWatchTree/node1"
	value1 := []byte("node1")

	node2 := "testWatchTree/node2"
	value2 := []byte("node2")

	node3 := "testWatchTree/node3"
	value3 := []byte("node3")

	err := kv.Put(node1, value1, nil)
	assert.NoError(t, err)
	err = kv.Put(node2, value2, nil)
	assert.NoError(t, err)
	err = kv.Put(node3, value3, nil)
	assert.NoError(t, err)

	stopCh := make(<-chan struct{})
	events, err := kv.WatchTree(dir, stopCh, nil)
	assert.NoError(t, err)
	assert.NotNil(t, events)

	// Update loop
	go func() {
		timeout := time.After(500 * time.Millisecond)
		for {
			select {
			case <-timeout:
				err := kv.Delete(node3)
				assert.NoError(t, err)
				return
			}
		}
	}()

	// Check for updates
	eventCount := 1
	for {
		select {
		case event := <-events:
			assert.NotNil(t, event)
			// We received the Delete event on a child node
			// Exit test successfully
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
	key := "testAtomicPut"
	value := []byte("world")

	// Put the key
	err := kv.Put(key, value, nil)
	assert.NoError(t, err)

	// Get should return the value and an incremented index
	pair, err := kv.Get(key, nil)
	assert.NoError(t, err)
	checkPairNotNil(t, pair)
	assert.Equal(t, pair.Value, value)
	assert.NotEqual(t, pair.LastIndex, 0)

	// This CAS should fail: previous exists.
	success, _, err := kv.AtomicPut(key, []byte("WORLD"), nil, nil)
	assert.Error(t, err)
	assert.False(t, success)

	// This CAS should succeed
	success, _, err = kv.AtomicPut(key, []byte("WORLD"), pair, nil)
	assert.NoError(t, err)
	assert.True(t, success)

	// This CAS should fail, key has wrong index.
	pair.LastIndex = 6744
	success, _, err = kv.AtomicPut(key, []byte("WORLDWORLD"), pair, nil)
	assert.Equal(t, err, store.ErrKeyModified)
	assert.False(t, success)
}

func testAtomicPutCreate(t *testing.T, kv store.Store) {
	// Use a key in a new directory to ensure Stores will create directories
	// that don't yet exist.
	key := "testAtomicPutCreate/create"
	value := []byte("putcreate")

	// AtomicPut the key, previous = nil indicates create.
	success, _, err := kv.AtomicPut(key, value, nil, nil)
	assert.NoError(t, err)
	assert.True(t, success)

	// Get should return the value and an incremented index
	pair, err := kv.Get(key, nil)
	assert.NoError(t, err)
	checkPairNotNil(t, pair)
	assert.Equal(t, pair.Value, value)

	// Attempting to create again should fail.
	success, _, err = kv.AtomicPut(key, value, nil, nil)
	assert.Equal(t, store.ErrKeyExists, err)
	assert.False(t, success)

	// This CAS should succeed, since it has the value from Get()
	success, _, err = kv.AtomicPut(key, []byte("PUTCREATE"), pair, nil)
	assert.NoError(t, err)
	assert.True(t, success)
}

func testAtomicPutWithSlashSuffixKey(t *testing.T, kv store.Store) {
	k1 := "testAtomicPutWithSlashSuffixKey/key/"
	success, _, err := kv.AtomicPut(k1, []byte{}, nil, nil)
	assert.Nil(t, err)
	assert.True(t, success)
}

func testAtomicDelete(t *testing.T, kv store.Store) {
	key := "testAtomicDelete"
	value := []byte("world")

	// Put the key
	err := kv.Put(key, value, nil)
	assert.NoError(t, err)

	// Get should return the value and an incremented index
	pair, err := kv.Get(key, nil)
	assert.NoError(t, err)
	checkPairNotNil(t, pair)
	assert.Equal(t, pair.Value, value)
	assert.NotEqual(t, pair.LastIndex, 0)

	tempIndex := pair.LastIndex

	// AtomicDelete should fail
	pair.LastIndex = 6744
	success, err := kv.AtomicDelete(key, pair)
	assert.Error(t, err)
	assert.False(t, success)

	// AtomicDelete should succeed
	pair.LastIndex = tempIndex
	success, err = kv.AtomicDelete(key, pair)
	assert.NoError(t, err)
	assert.True(t, success)

	// Delete a non-existent key; should fail
	success, err = kv.AtomicDelete(key, pair)
	assert.Equal(t, store.ErrKeyNotFound, err)
	assert.False(t, success)
}

func testLockUnlock(t *testing.T, kv store.Store) {
	key := "testLockUnlock"
	value := []byte("bar")

	// We should be able to create a new lock on key
	lock, err := kv.NewLock(key, &store.LockOptions{
		Value:          value,
		TTL:            2 * time.Second,
		DeleteOnUnlock: true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, lock)

	// Lock should successfully succeed or block
	lockChan, err := lock.Lock(nil)
	assert.NoError(t, err)
	assert.NotNil(t, lockChan)

	// Get should work
	pair, err := kv.Get(key, nil)
	assert.NoError(t, err)
	checkPairNotNil(t, pair)
	assert.Equal(t, pair.Value, value)
	assert.NotEqual(t, pair.LastIndex, 0)

	// Unlock should succeed
	err = lock.Unlock()
	assert.NoError(t, err)

	// Lock should succeed again
	lockChan, err = lock.Lock(nil)
	assert.NoError(t, err)
	assert.NotNil(t, lockChan)

	// Get should work
	pair, err = kv.Get(key, nil)
	assert.NoError(t, err)
	checkPairNotNil(t, pair)
	assert.Equal(t, pair.Value, value)
	assert.NotEqual(t, pair.LastIndex, 0)

	err = lock.Unlock()
	assert.NoError(t, err)
}

func testLockTTL(t *testing.T, kv store.Store, otherConn store.Store) {
	key := "testLockTTL"
	value := []byte("bar")

	renewCh := make(chan struct{})

	// We should be able to create a new lock on key
	lock, err := otherConn.NewLock(key, &store.LockOptions{
		Value:     value,
		TTL:       2 * time.Second,
		RenewLock: renewCh,
	})
	assert.NoError(t, err)
	assert.NotNil(t, lock)

	// Lock should successfully succeed
	lockChan, err := lock.Lock(nil)
	assert.NoError(t, err)
	assert.NotNil(t, lockChan)

	// Get should work
	pair, err := otherConn.Get(key, nil)
	assert.NoError(t, err)
	checkPairNotNil(t, pair)
	assert.Equal(t, pair.Value, value)
	assert.NotEqual(t, pair.LastIndex, 0)

	time.Sleep(3 * time.Second)

	done := make(chan struct{})
	stop := make(chan struct{})

	value = []byte("foobar")

	// Create a new lock with another connection
	lock, err = kv.NewLock(
		key,
		&store.LockOptions{
			Value: value,
			TTL:   3 * time.Second,
		},
	)
	assert.NoError(t, err)
	assert.NotNil(t, lock)

	// Lock should block, the session on the lock
	// is still active and renewed periodically
	go func(<-chan struct{}) {
		_, _ = lock.Lock(stop)
		done <- struct{}{}
	}(done)

	select {
	case _ = <-done:
		t.Fatal("Lock succeeded on a key that is supposed to be locked by another client")
	case <-time.After(4 * time.Second):
		// Stop requesting the lock as we are blocked as expected
		stop <- struct{}{}
		break
	}

	// Close the connection
	otherConn.Close()

	// Force stop the session renewal for the lock
	close(renewCh)

	// Let the session on the lock expire
	time.Sleep(3 * time.Second)
	locked := make(chan struct{})

	// Lock should now succeed for the other client
	go func(<-chan struct{}) {
		lockChan, err = lock.Lock(nil)
		assert.NoError(t, err)
		assert.NotNil(t, lockChan)
		locked <- struct{}{}
	}(locked)

	select {
	case _ = <-locked:
		break
	case <-time.After(4 * time.Second):
		t.Fatal("Unable to take the lock, timed out")
	}

	// Get should work with the new value
	pair, err = kv.Get(key, nil)
	assert.NoError(t, err)
	checkPairNotNil(t, pair)
	assert.Equal(t, pair.Value, value)
	assert.NotEqual(t, pair.LastIndex, 0)

	err = lock.Unlock()
	assert.NoError(t, err)
}

func testPutTTL(t *testing.T, kv store.Store, otherConn store.Store) {
	firstKey := "testPutTTL"
	firstValue := []byte("foo")

	secondKey := "second"
	secondValue := []byte("bar")

	// Put the first key with the Ephemeral flag
	err := otherConn.Put(firstKey, firstValue, &store.WriteOptions{TTL: 2 * time.Second})
	assert.NoError(t, err)

	// Put a second key with the Ephemeral flag
	err = otherConn.Put(secondKey, secondValue, &store.WriteOptions{TTL: 2 * time.Second})
	assert.NoError(t, err)

	// Get on firstKey should work
	pair, err := kv.Get(firstKey, nil)
	assert.NoError(t, err)
	checkPairNotNil(t, pair)

	// Get on secondKey should work
	pair, err = kv.Get(secondKey, nil)
	assert.NoError(t, err)
	checkPairNotNil(t, pair)

	// Close the connection
	otherConn.Close()

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

func testList(t *testing.T, kv store.Store) {
	parentKey := "testList"
	childKey := "testList/child"
	subfolderKey := "testList/subfolder"

	// Put the parent key
	err := kv.Put(parentKey, nil, &store.WriteOptions{IsDir: true})
	assert.NoError(t, err)

	// Put the first child key
	err = kv.Put(childKey, []byte("first"), nil)
	assert.NoError(t, err)

	// Put the second child key which is also a directory
	err = kv.Put(subfolderKey, []byte("second"), &store.WriteOptions{IsDir: true})
	assert.NoError(t, err)

	// Put child keys under secondKey
	for i := 1; i <= 3; i++ {
		key := "testList/subfolder/key" + strconv.Itoa(i)
		err := kv.Put(key, []byte("value"), nil)
		assert.NoError(t, err)
	}

	// List should work and return five child entries
	for _, parent := range []string{parentKey, parentKey + "/"} {
		pairs, err := kv.List(parent, nil)
		assert.NoError(t, err)
		if assert.NotNil(t, pairs) {
			assert.Equal(t, 5, len(pairs))
		}
	}

	// List on childKey should return 0 keys
	pairs, err := kv.List(childKey, nil)
	assert.NoError(t, err)
	if assert.NotNil(t, pairs) {
		assert.Equal(t, 0, len(pairs))
	}

	// List on subfolderKey should return 3 keys without the directory
	pairs, err = kv.List(subfolderKey, nil)
	assert.NoError(t, err)
	if assert.NotNil(t, pairs) {
		assert.Equal(t, 3, len(pairs))
	}

	// List should fail: the key does not exist
	pairs, err = kv.List("idontexist", nil)
	assert.Equal(t, store.ErrKeyNotFound, err)
	assert.Nil(t, pairs)
}

func testListLockKey(t *testing.T, kv store.Store) {
	listKey := "testListLockSide"

	err := kv.Put(listKey, []byte("val"), &store.WriteOptions{IsDir: true})
	assert.NoError(t, err)

	err = kv.Put(listKey+"/subfolder", []byte("val"), &store.WriteOptions{IsDir: true})
	assert.NoError(t, err)

	// Put keys under subfolder.
	for i := 1; i <= 3; i++ {
		key := listKey + "/subfolder/key" + strconv.Itoa(i)
		err := kv.Put(key, []byte("val"), nil)
		assert.NoError(t, err)

		// We lock the child key
		lock, err := kv.NewLock(key, &store.LockOptions{Value: []byte("locked"), TTL: 2 * time.Second})
		assert.NoError(t, err)
		assert.NotNil(t, lock)

		lockChan, err := lock.Lock(nil)
		assert.NoError(t, err)
		assert.NotNil(t, lockChan)
	}

	// List children of the root directory (`listKey`), this should
	// not output any `___lock` entries and must contain 4 results.
	pairs, err := kv.List(listKey, nil)
	assert.NoError(t, err)
	assert.NotNil(t, pairs)
	assert.Equal(t, 4, len(pairs))

	for _, pair := range pairs {
		if strings.Contains(string(pair.Key), "___lock") {
			assert.FailNow(t, "tesListLockKey: found a key containing lock suffix '___lock'")
		}
	}
}

func testDeleteTree(t *testing.T, kv store.Store) {
	prefix := "testDeleteTree"

	firstKey := "testDeleteTree/first"
	firstValue := []byte("first")

	secondKey := "testDeleteTree/second"
	secondValue := []byte("second")

	// Put the first key
	err := kv.Put(firstKey, firstValue, nil)
	assert.NoError(t, err)

	// Put the second key
	err = kv.Put(secondKey, secondValue, nil)
	assert.NoError(t, err)

	// Get should work on the first Key
	pair, err := kv.Get(firstKey, nil)
	assert.NoError(t, err)
	checkPairNotNil(t, pair)
	assert.Equal(t, pair.Value, firstValue)
	assert.NotEqual(t, pair.LastIndex, 0)

	// Get should work on the second Key
	pair, err = kv.Get(secondKey, nil)
	assert.NoError(t, err)
	checkPairNotNil(t, pair)
	assert.Equal(t, pair.Value, secondValue)
	assert.NotEqual(t, pair.LastIndex, 0)

	// Delete Values under directory `nodes`
	err = kv.DeleteTree(prefix)
	assert.NoError(t, err)

	// Get should fail on both keys
	pair, err = kv.Get(firstKey, nil)
	assert.Error(t, err)
	assert.Nil(t, pair)

	pair, err = kv.Get(secondKey, nil)
	assert.Error(t, err)
	assert.Nil(t, pair)
}

// RunCleanup cleans up keys introduced by the tests
func RunCleanup(t *testing.T, kv store.Store) {
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
		assert.True(t, err == nil || err == store.ErrKeyNotFound, fmt.Sprintf("failed to delete tree key %s: %v", key, err))
		err = kv.Delete(key)
		assert.True(t, err == nil || err == store.ErrKeyNotFound, fmt.Sprintf("failed to delete key %s: %v", key, err))
	}
}
