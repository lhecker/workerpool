package workerpool

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
)

var (
	ErrBatchCanceled = errors.New("batch canceled")
)

// Pool represents a worker pool suitable for continued use with cgo calls.
// Its primary function, NewBatch, can be used to create and submit a batch of work.
type Pool interface {
	// Close processes any remaining queued up jobs and then stops all worker goroutines.
	//
	// This function MUST NOT be:
	// - called twice
	// - called concurrently with any other function (especially Submit())
	Close()

	// NewBatch can be used to submit a bunch of jobs to the pool and wait for their completion.
	NewBatch() Batch

	// NewBatchWithContext is identical to NewBatch, except that it returns a context derived from the given one.
	//
	// The derived Context is canceled the first time a function passed to Submit()
	// returns a non-nil error or the first time Wait returns, whichever occurs first.
	NewBatchWithContext(ctx context.Context) (Batch, context.Context)
}

// Batch can be used to submit a bunch of work and wait for its completion.
type Batch interface {
	// Submit asynchronously and immediately submits the given function to the worker pool for execution.
	// Call Wait() to retrieve the result.
	Submit(f func() error) Batch

	// Wait blocks until all submitted functions in this batch have finished processing
	// and returns the first error if any happened.
	Wait() error
}

type queue interface {
	Close()
	Push(j job)
	Pop() (job, bool)
}

// NewPool creates a new worker pool.
func NewPool(options ...Option) Pool {
	opts := &poolOptions{}
	for _, apply := range options {
		apply(opts)
	}
	if opts.poolSize == 0 {
		opts.poolSize = runtime.NumCPU()
	}
	if opts.queue == nil {
		opts.queue = newFifoQueue(0)
	}

	s := &pool{
		queue: opts.queue,
	}

	for i := 0; i < opts.poolSize; i++ {
		s.wg.Add(1)
		go s.worker(opts)
	}

	return s
}

type pool struct {
	queue queue
	wg    sync.WaitGroup
}

func (s *pool) worker(opts *poolOptions) {
	defer s.wg.Done()

	opts.runSetupHooks()
	defer opts.runTeardownHooks()

	for {
		job, ok := s.queue.Pop()
		if !ok {
			break
		}

		job.execute()
	}
}

func (s *pool) Close() {
	s.queue.Close()
	s.wg.Wait()
}

func (s *pool) NewBatch() Batch {
	return &batch{
		pool: s,
	}
}

func (s *pool) NewBatchWithContext(ctx context.Context) (Batch, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	return &batch{
		pool:   s,
		cancel: cancel,
	}, ctx
}

type batch struct {
	pool   *pool
	cancel context.CancelFunc

	wg     sync.WaitGroup
	err    error
	hasErr uint32
}

func (s *batch) Submit(f func() error) Batch {
	s.wg.Add(1)
	s.pool.queue.Push(job{
		batch: s,
		fn:    f,
	})
	return s
}

func (s *batch) Wait() error {
	s.wg.Wait()
	if s.cancel != nil {
		s.cancel()
	}
	return s.err
}

func (s *batch) setError(err error) {
	if atomic.CompareAndSwapUint32(&s.hasErr, 0, 1) {
		s.err = err
		if s.cancel != nil {
			s.cancel()
		}
	}
}

type job struct {
	batch *batch
	fn    func() error
}

func (s job) execute() {
	batch := s.batch
	defer batch.wg.Done()

	err := s.fn()
	if err != nil {
		batch.setError(err)
	}
}
