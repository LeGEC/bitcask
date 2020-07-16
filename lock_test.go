package bitcask

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofrs/flock"
)

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
	lock := flock.New(lockpath)
	ok, _ := lock.TryLock()
	if !ok {
		return 0
	}
	defer func() {
		os.Remove(lock.Path())
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

func TestLock(t *testing.T) {
	// test parameters, written in code :
	//   you may want to tweak these values for testing

	// WARNING : this test will delete the file located at "lockPath". Choose an adequate temporary file name.
	lockPath := "/tmp/bitcask_unit_test_lock" // file path to use for the lock
	goroutines := 20                          // number of concurrent "locker" goroutines to launch
	successfulLockCount := 50                 // how many times a "locker" will successfully take the lock before halting

	// make sure there is no present lock when startng this test
	os.Remove(lockPath)

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
			locker(t, id, lockPath, successfulLockCount, ch)
			wg.Done()
		}(i)
	}

	wg.Wait()
}
