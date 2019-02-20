package testcase

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-locks/distlock/driver"
)

var expiry = time.Second * 3

func RunLockTest(d driver.IDriver, t *testing.T) {
	var counter int32
	var waitGroup = new(sync.WaitGroup)
	var name, value, other = "test.lock.name", "test.lock.value", "test.lock.value.other"

	/* competition of lock in multiple goroutine */
	for i := 0; i < 50; i++ {
		waitGroup.Add(1)
		go func() {
			ok, _ := d.Lock(name, value, expiry)
			if ok {
				atomic.AddInt32(&counter, 1)
			}
			waitGroup.Done()
		}()
	}
	waitGroup.Wait()
	if counter != 1 {
		t.Errorf("unexpected result, expect = 1, but = %d", counter)
	}

	/* only assign the same value can unlock the mutex */
	d.Unlock(name, other)
	ok1, _ := d.Lock(name, value, expiry)
	d.Unlock(name, value)
	ok2, _ := d.Lock(name, value, expiry)
	d.Unlock(name, value)
	if ok1 || !ok2 {
		t.Errorf("unexpected result, expect = [false,true] but = [%#v,%#v]", ok1, ok2)
	}

	/* the broadcast notification which triggered by unlock */
	if wd, ok := d.(driver.IWatcher); ok {
		commonWatchAndNotifyTest(
			wd, name,
			func() bool {
				ok, _ := d.Lock(name, value, expiry)
				return ok
			},
			func() {
				d.Unlock(name, value)
			},
		)
	}
}

func RunTouchTest(d driver.IDriver, t *testing.T) {
	var name, value, other = "test.touch.name", "test.touch.value", "test.touch.value.other"
	/* only after locked and with the same value that the touch action can be succeed */
	ok1 := d.Touch(name, value, expiry)
	d.Lock(name, value, expiry)
	ok2 := d.Touch(name, value, expiry)
	ok3 := d.Touch(name, other, expiry)
	time.Sleep(expiry)
	ok4 := d.Touch(name, value, expiry)
	if ok1 || !ok2 || ok3 || ok4 {
		t.Errorf("unexpected result, expect = [false,true,false,false] but = [%#v,%#v,%#v,%#v]", ok1, ok2, ok3, ok4)
	}
}

func RunRLockTest(d driver.IRWDriver, t *testing.T) {
	var counter int32
	var waitGroup = new(sync.WaitGroup)
	var name, value, other = "test.rlock.name", "test.rlock.value", "test.rlock.value.other"

	/* read locks are not mutually exclusive */
	for i := 0; i < 50; i++ {
		waitGroup.Add(1)
		go func() {
			ok, _ := d.RLock(name, value, expiry)
			if ok {
				atomic.AddInt32(&counter, 1)
			}
			waitGroup.Done()
		}()
	}
	waitGroup.Wait()
	if counter != 50 {
		t.Errorf("unexpected result, expect = 50, but = %d", counter)
	}

	/* write is mutually exclusive with read,
	 * no read lock can enter in after write lock if even it is failed */
	ok1, _ := d.WLock(name, value, expiry)
	ok2, _ := d.RLock(name, value, expiry)
	if ok1 || ok2 {
		t.Errorf("unexpected result, expect = [false,false], but = [%#v,%#v]", ok1, ok2)
	}

	/* only assign the same value can unlock the mutex
	 * after read unlock, if write lock tried before, it will be preferential */
	for i := 0; i < 50; i++ {
		d.RUnlock(name, other)
	}
	d.RTouch(name, value, expiry)
	ok3, _ := d.WLock(name, value, expiry)
	for i := 0; i < 50; i++ {
		d.RUnlock(name, value)
	}
	ok4, _ := d.RLock(name, value, expiry)
	ok5, _ := d.WLock(name, value, expiry)
	d.WUnlock(name, value)
	ok6, _ := d.RLock(name, value, expiry)
	d.RUnlock(name, value)
	if ok3 || ok4 || !ok5 || !ok6 {
		t.Errorf("unexpected result, expect = [false,false,true,true], but = [%#v,%#v,%#v,%#v]", ok3, ok4, ok5, ok6)
	}

	/* the broadcast notification which triggered by unlock */
	if wd, ok := d.(driver.IWatcher); ok {
		commonWatchAndNotifyTest(
			wd, name,
			func() bool {
				ok, _ := d.RLock(name, value, expiry)
				return ok
			},
			func() {
				d.RUnlock(name, value)
			},
		)
	}
}

func RunRTouchTest(d driver.IRWDriver, t *testing.T) {
	var name, value, other = "test.rtouch.name", "test.rtouch.value", "test.rtouch.value.other"
	/* only after locked and with the same value that the touch action can be succeed */
	ok1 := d.RTouch(name, value, expiry)
	d.RLock(name, value, expiry)
	ok2 := d.RTouch(name, value, expiry)
	ok3 := d.RTouch(name, other, expiry)
	time.Sleep(expiry) // wait for expired
	ok4 := d.RTouch(name, value, expiry)
	if ok1 || !ok2 || ok3 || ok4 {
		t.Errorf("unexpected result, expect = [false,true,false,false] but = [%#v,%#v,%#v,%#v]", ok1, ok2, ok3, ok4)
	}
}

func RunWLockTest(d driver.IRWDriver, t *testing.T) {
	var counter int32
	var waitGroup = new(sync.WaitGroup)
	var name, value = "test.wlock.name", "test.wlock.value"

	/* write locks are mutually exclusive */
	for i := 0; i < 50; i++ {
		waitGroup.Add(1)
		go func() {
			ok, _ := d.WLock(name, value, expiry)
			if ok {
				atomic.AddInt32(&counter, 1)
			}
			waitGroup.Done()
		}()
	}
	waitGroup.Wait()
	if counter != 1 {
		t.Errorf("unexpected result, expect = 1, but = %d", counter)
	}

	/* if write lock tried before, it will be preferential,
	 * the read lock is not available until the write is expired or unlocked */
	d.WUnlock(name, value)
	ok1, _ := d.RLock(name, value, expiry)
	ok2, _ := d.WLock(name, value, expiry)
	ok3, _ := d.RLock(name, value, expiry)
	time.Sleep(expiry)
	ok4, _ := d.RLock(name, value, expiry)
	d.RUnlock(name, value)
	if !ok1 || ok2 || ok3 || !ok4 {
		t.Errorf("unexpected result, expect = [true,false,false,true], but = [%#v,%#v,%#v,%#v]", ok1, ok2, ok3, ok4)
	}

	/* the broadcast notification which triggered by unlock */
	if wd, ok := d.(driver.IWatcher); ok {
		commonWatchAndNotifyTest(
			wd, name,
			func() bool {
				ok, _ := d.WLock(name, value, expiry)
				return ok
			},
			func() {
				d.WUnlock(name, value)
			},
		)
	}
}

func RunWTouchTest(d driver.IRWDriver, t *testing.T) {
	var name, value, other = "test.wtouch.name", "test.wtouch.value", "test.wtouch.value.other"
	/* only after locked and with the same value that the touch action can be succeed */
	ok1 := d.WTouch(name, value, expiry)
	d.WLock(name, value, expiry)
	ok2 := d.WTouch(name, value, expiry)
	ok3 := d.WTouch(name, other, expiry)
	time.Sleep(expiry) // wait for expired
	ok4 := d.WTouch(name, value, expiry)
	if ok1 || !ok2 || ok3 || ok4 {
		t.Errorf("unexpected result, expect = [false,true,false,false] but = [%#v,%#v,%#v,%#v]", ok1, ok2, ok3, ok4)
	}
}

func commonWatchAndNotifyTest(d driver.IWatcher, name string, lock func() bool, unlock func()) {
	var waitGroup sync.WaitGroup
	for i := 0; i < 2; i++ {
		waitGroup.Add(1)
		go func() {
			msgCounter := 0
			notifyChan := d.Watch(name)
			for {
				select {
				case <-notifyChan:
					msgCounter++
					if msgCounter == 10 {
						waitGroup.Done()
					}
				}
			}
		}()
	}
	waitGroup.Add(1)
	go func() {
		/* make ensure all channels are ready */
		time.Sleep(time.Millisecond * 1000)
		for i := 1; i <= 10; i++ {
			if lock() {
				unlock()
			}
		}
		waitGroup.Done()
	}()
	waitGroup.Wait()
}
