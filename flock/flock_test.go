package flock

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// WARNING : this test will delete the file located at "testLockPath". Choose an adequate temporary file name.
const testLockPath = "/tmp/bitcask_unit_test_lock" // file path to use for the lock

func TestTryLock(t *testing.T) {
	// test that basic locking functionnalities are consistent

	// make sure there is no present lock when startng this test
	os.Remove(testLockPath)

	assert := assert.New(t)

	lock1 := New(testLockPath)
	lock2 := New(testLockPath)

	locked1, err := lock1.TryLock()
	assert.True(locked1)
	assert.NoError(err)

	locked2, err := lock2.TryLock()
	assert.False(locked2)
	//assert.Error(err)  // gofrs.Flock.TryLock may return "not locked" without an error value

	err = lock1.Unlock()
	assert.NoError(err)

	locked2, err = lock2.TryLock()
	assert.True(locked2)
	assert.NoError(err)

	err = lock2.Unlock()
	assert.NoError(err)
}

func TestLock(t *testing.T) {
	assert := assert.New(t)

	// make sure there is no present lock when startng this test
	os.Remove(testLockPath)

	wg := &sync.WaitGroup{}
	wg.Add(2)

	taken := make(chan struct{})

	// goroutine 1 : take lock, close "taken" chan to signal that lock is taken, wait a bit, then release lock.
	go func() {
		defer wg.Done()

		lock := New(testLockPath)

		err := lock.Lock()
		assert.NoError(err)

		close(taken)
		<-time.After(time.Millisecond)

		err = lock.Unlock()
		assert.NoError(err)
	}()

	// goroutine 2 : wait for the "taken" signal, take the lock, then release it.
	// lock.Lock() should block until lock is available, so no error should be
	// returned, and lock should be taken successfully
	go func() {
		defer wg.Done()

		<-taken

		lock := New(testLockPath)

		err := lock.Lock()
		assert.NoError(err)

		err = lock.Unlock()
		assert.NoError(err)
	}()

	wg.Wait()
}

var lockerCount int64

// lockAndCount tries to take a lock on "lockpath"
// if it fails : it returns 0 and stop there
// if it succeeds :
//   1- it sets a defer function to release the lock in the same fashion as "func (b *Bitcask) Close()"
//   2- it increments the shared "lockerCount" above
//   3- it waits for a short amount of time
//   4- it decrements "lockerCount"
//   5- it returns the value it has seen at step 2.
//
// If the locking and unlocking behave as we expect them to,
// instructions 1-5 should be in a critical section,
// and the only possible value at step 2 should be "1".
//
// Returning a value > 0 indicates this function successfully acquired the lock,
// returning a value == 0 indicates that TryLock failed.

func lockAndCount(lockpath string) int64 {
	lock := New(lockpath)
	ok, _ := lock.TryLock()
	if !ok {
		return 0
	}
	defer func() {
		lock.Unlock()
	}()

	x := atomic.AddInt64(&lockerCount, 1)
	// emulate a workload :
	<-time.After(1 * time.Microsecond)
	atomic.AddInt64(&lockerCount, -1)

	return x
}

// locker will call the lock function above in a loop, until one of the following holds :
//  - reading from the "timeout" channel doesn't block
//  - the number of calls to "lock()" that indicate the lock was successfully taken reaches "successfullLockCount"
func locker(t *testing.T, id int, lockPath string, successfulLockCount int, timeout <-chan struct{}) {
	timedOut := false

	failCount := 0
	max := int64(0)

lockloop:
	for successfulLockCount > 0 {
		select {
		case <-timeout:
			timedOut = true
			break lockloop
		default:
		}

		x := lockAndCount(lockPath)

		if x > 0 {
			// if x indicates the lock was taken : decrement the counter
			successfulLockCount--
		}

		if x > 1 {
			// if x indicates an invalid value : increase the failCount and update max accordingly
			failCount++
			if x > max {
				max = x
			}
		}
	}

	// check failure cases :
	if timedOut {
		t.Fail()
		t.Logf("[runner %02d] timed out", id)
	}
	if failCount > 0 {
		t.Fail()
		t.Logf("[runner %02d] lockCounter was > 1 on %2.d occasions, max seen value was %2.d", id, failCount, max)
	}
}

func TestRaceLock(t *testing.T) {
	// test parameters, written in code :
	//   you may want to tweak these values for testing

	goroutines := 20         // number of concurrent "locker" goroutines to launch
	successfulLockCount := 50                 // how many times a "locker" will successfully take the lock before halting

	// make sure there is no present lock when startng this test
	os.Remove(testLockPath)

	// timeout implemented in code
	// (the lock acquisition depends on the behavior of the filesystem,
	//	avoid sending CI in endless loop if something fishy happens on the test server ...)
	// tweak this value if needed ; comment out the "close(ch)" instruction below
	timeout := 10 * time.Second
	ch := make(chan struct{})
	go func() {
		<-time.After(timeout)
		close(ch)
	}()

	wg := &sync.WaitGroup{}
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			locker(t, id, testLockPath, successfulLockCount, ch)
			wg.Done()
		}(i)
	}

	wg.Wait()
}
