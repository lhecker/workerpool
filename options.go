package workerpool

// Option sets pool options in the call to NewPool.
type Option func(*poolOptions)

// PoolSize sets the size of the pool.
//
// This function panics if a value of 0 or less is given.
func PoolSize(size int) Option {
	if size <= 0 {
		panic("invalid pool size")
	}

	return func(o *poolOptions) {
		o.poolSize = size
	}
}

// QueueSize sets the depth of the worker channel.
//
// This function panics if a value of less than 0 is given.
// Normally you never need to specify this option.
func QueueSize(size int) Option {
	if size < 0 {
		panic("invalid queue size")
	}

	return func(o *poolOptions) {
		o.queueSize = size
	}
}

// SetupHooks adds the given callbacks to the list of functions to be
// executed when a worker is spawned and before any jobs are processed.
func SetupHooks(hooks ...func()) Option {
	return func(o *poolOptions) {
		o.setupHooks = append(o.setupHooks, hooks...)
	}
}

// TeardownHooks adds the given callbacks to the list of functions to be
// executed when a worker exits and after any jobs are processed.
func TeardownHooks(hooks ...func()) Option {
	return func(o *poolOptions) {
		o.teardownHooks = append(o.teardownHooks, hooks...)
	}
}

type poolOptions struct {
	poolSize      int
	queueSize     int
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
