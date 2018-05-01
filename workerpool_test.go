package workerpool

import (
	"errors"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClose(t *testing.T) {
	assert := assert.New(t)
	goroutinesBefore := runtime.NumGoroutine()

	pool := NewPool(PoolSize(10))
	assert.InDelta(goroutinesBefore+10, runtime.NumGoroutine(), 5)

	pool.Close()
	assert.InDelta(goroutinesBefore, runtime.NumGoroutine(), 5)
}

func TestSubmit(t *testing.T) {
	assert := assert.New(t)

	pool := NewPool()
	batch := pool.NewBatch()
	counter := uint32(0)

	for i := 0; i < 10; i++ {
		batch.Submit(func() error {
			if atomic.AddUint32(&counter, 1) == 10 {
				return errors.New("test")
			}
			return nil
		})
	}

	err := batch.Wait()
	assert.EqualError(err, "test")
	assert.EqualValues(10, counter)
}

func BenchmarkSubmit(b *testing.B) {
	pool := NewPool(
		SetupHooks(runtime.LockOSThread),
		TeardownHooks(runtime.UnlockOSThread),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.NewBatch().Submit(func() error {
			return nil
		}).Wait()
	}
}

func BenchmarkSubmitParallel(b *testing.B) {
	pool := NewPool(
		SetupHooks(runtime.LockOSThread),
		TeardownHooks(runtime.UnlockOSThread),
	)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pool.NewBatch().Submit(func() error {
				return nil
			}).Wait()
		}
	})
}
