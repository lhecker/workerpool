package workerpool

import (
	"runtime"
	"sync"
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

	// NewBatch can be used to submit a bunch of jobs and wait for their completion.
	NewBatch() Batch

	// Submit synchronously submits the given function to the worker pool for execution.
	Submit(f func() error) error
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

// NewPool creates a new worker pool.
func NewPool(size int) Pool {
	if size == 0 {
		size = runtime.GOMAXPROCS(0)
	}

	s := &pool{
		queue: make(chan job, size),
	}

	for i := 0; i < size; i++ {
		s.wg.Add(1)
		go s.worker()
	}

	return s
}

type pool struct {
	queue chan job
	done  uint32
	wg    sync.WaitGroup
}

func (s *pool) worker() {
	defer s.wg.Done()

	// Lock this goroutine to an actual thread beforehand already.
	// That way long-running cgo calls will not cause this to happen later on.
	runtime.LockOSThread()

	for job := range s.queue {
		job.execute()
	}
}

func (s *pool) Close() {
	close(s.queue)
	s.wg.Wait()
}

func (s *pool) NewBatch() Batch {
	return &batch{
		pool:   s,
		shared: &shared{},
	}
}

func (s *pool) Submit(f func() error) error {
	return s.NewBatch().Submit(f).Wait()
}

type batch struct {
	pool   *pool
	shared *shared
}

func (s *batch) Submit(f func() error) Batch {
	s.shared.wg.Add(1)
	s.pool.queue <- job{
		fn:     f,
		shared: s.shared,
	}
	return s
}

func (s *batch) Wait() error {
	s.shared.wg.Wait()
	return s.shared.err
}

type shared struct {
	wg      sync.WaitGroup
	errOnce sync.Once
	err     error
}

type job struct {
	fn     func() error
	shared *shared
}

func (s job) execute() {
	defer s.shared.wg.Done()

	if s.shared.err != nil {
		return
	}

	err := s.fn()
	if err != nil {
		s.shared.errOnce.Do(func() {
			s.shared.err = err
		})
	}
}
