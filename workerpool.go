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
func NewPool(options ...Option) Pool {
	numCPU := runtime.NumCPU()

	opts := &poolOptions{
		poolSize: numCPU,
	}
	for _, apply := range options {
		apply(opts)
	}

	s := &pool{
		queue: make(chan job, opts.queueSize),
	}

	for i := 0; i < opts.poolSize; i++ {
		s.wg.Add(1)
		go s.worker(opts)
	}

	return s
}

type pool struct {
	queue chan job
	wg    sync.WaitGroup
}

func (s *pool) worker(opts *poolOptions) {
	defer s.wg.Done()

	opts.runSetupHooks()
	defer opts.runTeardownHooks()

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
		pool: s,
	}
}

type batch struct {
	pool    *pool
	wg      sync.WaitGroup
	errOnce sync.Once
	err     error
}

func (s *batch) Submit(f func() error) Batch {
	s.wg.Add(1)
	s.pool.queue <- job{
		batch: s,
		fn:    f,
	}
	return s
}

func (s *batch) Wait() error {
	s.wg.Wait()
	return s.err
}

type job struct {
	batch *batch
	fn    func() error
}

func (s job) execute() {
	defer s.batch.wg.Done()

	// NOTE: This is a voluntary race condition and might trigger Go's race detector.
	//
	// What I'd like to do here is to read .err using relaxed memory ordering to check whether it
	// has "probably" been set already. On all commonly used platforms this works reasonably well.
	// Especially since we only write .err when protected by .errOnce and only "really" read
	// and use its value after the batch's WaitGroup has returned from Wait().
	if s.batch.err != nil {
		return
	}

	err := s.fn()
	if err != nil {
		s.batch.errOnce.Do(func() {
			s.batch.err = err
		})
	}
}
