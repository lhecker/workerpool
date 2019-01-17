package workerpool

import (
	"sync"
)

type fifoQueue chan job

func newFifoQueue(size int) fifoQueue {
	return make(chan job, size)
}

func (s fifoQueue) Close() {
	close(s)
}

func (s fifoQueue) Push(j job) {
	s <- j
}

func (s fifoQueue) Pop() (job, bool) {
	j, ok := <-s
	return j, ok
}

type lifoQueue struct {
	popCond sync.Cond
	queue   []job
	count   int
	head    int
}

func newLifoQueue(size int) *lifoQueue {
	return &lifoQueue{
		popCond: sync.Cond{L: &sync.Mutex{}},
		queue:   make([]job, size),
	}
}

func (s *lifoQueue) Close() {
	s.popCond.L.Lock()
	defer s.popCond.L.Unlock()

	for _, j := range s.queue {
		if j.batch != nil {
			j.batch.setError(ErrBatchCanceled)
		}
	}

	s.queue = nil
}

func (s *lifoQueue) Push(j job) {
	var (
		existing job
	)

	s.popCond.L.Lock()
	{
		if len(s.queue) == 0 {
			s.popCond.L.Unlock()
			panic("push on closed lifoQueue")
		}

		existing = s.queue[s.head]
		s.queue[s.head] = j
		s.head = (s.head + 1) % len(s.queue)

		if s.count < len(s.queue) {
			s.count++
		}
	}
	s.popCond.L.Unlock()

	if existing.batch != nil {
		existing.batch.setError(ErrBatchCanceled)
	}

	s.popCond.Signal()
}

func (s *lifoQueue) Pop() (job, bool) {
	var j job

	s.popCond.L.Lock()
	{
		if len(s.queue) == 0 {
			s.popCond.L.Unlock()
			return job{}, false
		}

		for s.count == 0 {
			s.popCond.Wait()
		}

		idx := s.head - 1
		j = s.queue[idx]
		s.queue[idx] = job{}

		if s.head > 0 {
			s.head--
		} else {
			s.head = len(s.queue) - 1
		}

		s.count--
	}
	s.popCond.L.Unlock()

	return j, true
}
