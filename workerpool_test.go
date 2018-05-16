package workerpool

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPoolClose(t *testing.T) {
	assert := assert.New(t)
	goroutinesBefore := runtime.NumGoroutine()

	pool := NewPool(PoolSize(10))
	assert.InDelta(goroutinesBefore+10, runtime.NumGoroutine(), 5)

	pool.Close()
	assert.InDelta(goroutinesBefore, runtime.NumGoroutine(), 5)
}

func TestNewBatchWithContext(t *testing.T) {
	assert := assert.New(t)

	pool := NewPool(PoolSize(1))
	batch, ctx := pool.NewBatchWithContext(context.Background())
	counter := uint32(0)

	batch.Submit(func() error {
		atomic.AddUint32(&counter, 1)
		return errors.New("test")
	})
	batch.Submit(func() error {
		atomic.AddUint32(&counter, 1)
		return ctx.Err()
	})

	err := batch.Wait()
	assert.Equal(ctx.Err(), context.Canceled)
	assert.EqualError(err, "test")
	assert.EqualValues(1, counter)
}

func TestBatchSubmit(t *testing.T) {
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
