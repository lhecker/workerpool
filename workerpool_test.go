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

	pool := NewPool(
		WithBlockingFifoQueue(0),
		WithPoolSize(10),
	)
	assert.InDelta(goroutinesBefore+10, runtime.NumGoroutine(), 5)

	pool.Close()
	assert.InDelta(goroutinesBefore, runtime.NumGoroutine(), 5)
}

func TestNewBatchWithContextFifo(t *testing.T) {
	assert := assert.New(t)

	pool := NewPool(
		WithBlockingFifoQueue(0),
		WithPoolSize(1),
	)
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
	assert.EqualValues(2, counter)
}

func TestBatchSubmitFifo(t *testing.T) {
	assert := assert.New(t)

	pool := NewPool(
		WithBlockingFifoQueue(0),
	)
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

func BenchmarkSubmitFifo(b *testing.B) {
	pool := NewPool(
		WithBlockingFifoQueue(2*runtime.GOMAXPROCS(0)),
		WithSetupHooks(runtime.LockOSThread),
		WithTeardownHooks(runtime.UnlockOSThread),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pool.NewBatch().Submit(func() error {
			return nil
		}).Wait()
	}
}

func BenchmarkSubmitParallelFifo(b *testing.B) {
	pool := NewPool(
		WithBlockingFifoQueue(2*runtime.GOMAXPROCS(0)),
		WithSetupHooks(runtime.LockOSThread),
		WithTeardownHooks(runtime.UnlockOSThread),
	)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = pool.NewBatch().Submit(func() error {
				return nil
			}).Wait()
		}
	})
}

func TestNewBatchWithContextLifo(t *testing.T) {
	assert := assert.New(t)

	pool := NewPool(
		WithBoundedLifoQueue(16),
		WithPoolSize(1),
	)
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
	assert.EqualValues(2, counter)
}

func TestBatchSubmitLifo(t *testing.T) {
	assert := assert.New(t)

	pool := NewPool(
		WithBoundedLifoQueue(16),
	)
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

func BenchmarkSubmitLifo(b *testing.B) {
	pool := NewPool(
		WithBoundedLifoQueue(2*runtime.GOMAXPROCS(0)),
		WithSetupHooks(runtime.LockOSThread),
		WithTeardownHooks(runtime.UnlockOSThread),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pool.NewBatch().Submit(func() error {
			return nil
		}).Wait()
	}
}

func BenchmarkSubmitParallelLifo(b *testing.B) {
	pool := NewPool(
		WithBoundedLifoQueue(2*runtime.GOMAXPROCS(0)),
		WithSetupHooks(runtime.LockOSThread),
		WithTeardownHooks(runtime.UnlockOSThread),
	)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = pool.NewBatch().Submit(func() error {
				return nil
			}).Wait()
		}
	})
}
