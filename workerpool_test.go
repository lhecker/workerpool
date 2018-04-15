package workerpool

import (
	"errors"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCgoPool_Stop(t *testing.T) {
	assert := assert.New(t)
	goroutinesBefore := runtime.NumGoroutine()

	pool := NewPool(10)
	assert.InDelta(goroutinesBefore+10, runtime.NumGoroutine(), 5)

	pool.Close()
	assert.InDelta(goroutinesBefore, runtime.NumGoroutine(), 5)
}

func TestCgoPoolBatch_Submit(t *testing.T) {
	assert := assert.New(t)

	pool := NewPool(0)
	batch := pool.NewBatch()
	counter := uint32(0)
	counterPtr := &counter

	for i := 0; i < 10; i++ {
		i := i
		batch.Submit(func() error {
			atomic.AddUint32(counterPtr, 1)

			if i == 9 {
				return errors.New("test")
			}
			return nil
		})
	}

	err := batch.Wait()
	assert.EqualError(err, "test")
	assert.EqualValues(10, counter)
}
