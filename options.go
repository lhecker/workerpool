package workerpool

type Option func(*poolOptions)

func PoolSize(size int) Option {
	if size == 0 {
		panic("invalid pool size")
	}

	return func(o *poolOptions) {
		o.poolSize = size
	}
}

func QueueSize(size int) Option {
	return func(o *poolOptions) {
		o.queueSize = size
	}
}

func SetupHooks(hooks ...func()) Option {
	return func(o *poolOptions) {
		o.setupHooks = append(o.setupHooks, hooks...)
	}
}

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
