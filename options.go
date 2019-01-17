package workerpool

// Option sets pool options in the call to NewPool.
type Option func(*poolOptions)

// WithPoolSize sets the size of the pool.
//
// This function panics if a value of 0 or less is given.
func WithPoolSize(size int) Option {
	if size <= 0 {
		panic("invalid pool size")
	}

	return func(o *poolOptions) {
		o.poolSize = size
	}
}

// WithBlockingFifoQueue sets the size of the pool.
//
// This function panics if a value of 0 or less is given.
func WithBlockingFifoQueue(size int) Option {
	return func(o *poolOptions) {
		o.queue = newFifoQueue(size)
	}
}

// WithBoundedLifoQueue sets the size of the pool.
//
// This function panics if a value of 0 or less is given.
func WithBoundedLifoQueue(size int) Option {
	if size == 0 {
		panic("invalid size")
	}

	return func(o *poolOptions) {
		o.queue = newLifoQueue(size)
	}
}

// WithSetupHooks adds the given callbacks to the list of functions to be
// executed when a worker is spawned and before any jobs are processed.
func WithSetupHooks(hooks ...func()) Option {
	return func(o *poolOptions) {
		o.setupHooks = append(o.setupHooks, hooks...)
	}
}

// WithTeardownHooks adds the given callbacks to the list of functions to be
// executed when a worker exits and after any jobs are processed.
func WithTeardownHooks(hooks ...func()) Option {
	return func(o *poolOptions) {
		o.teardownHooks = append(o.teardownHooks, hooks...)
	}
}

type poolOptions struct {
	poolSize      int
	queue         queue
	setupHooks    []func()
	teardownHooks []func()
}

func (s *poolOptions) runSetupHooks() {
	for _, hook := range s.setupHooks {
		hook()
	}
}

func (s *poolOptions) runTeardownHooks() {
	for idx := len(s.teardownHooks) - 1; idx >= 0; idx-- {
		s.teardownHooks[idx]()
	}
}
