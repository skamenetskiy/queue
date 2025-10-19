package queue

import (
	"context"
	"runtime"
)

func New[T any](config Config[T]) Queue[T] {
	if config.Context == nil {
		config.Context = context.Background()
	}
	if config.ErrorHandler == nil {
		config.ErrorHandler = func(_ error) {}
	}
	if config.Runner == nil {
		panic("cannot create queue with nil runner")
	}
	return &queue[T]{
		Config: config,
	}
}

type Queue[T any] interface {
	Push(T)
	Start() StopFn
}

type RunnerFn[T any] func(context.Context, T) error

type StopFn func(err error)

type Config[T any] struct {
	Context      context.Context
	Runner       RunnerFn[T]
	ErrorHandler func(error)
	QueueSize    int
	NumWorkers   int
}

type queue[T any] struct {
	Config[T]
	line chan T
	errs chan error
}

func (q *queue[T]) Push(data T) {
	go func(data T) { q.line <- data }(data)
}

func (q *queue[T]) Start() StopFn {
	ctx, stop := context.WithCancelCause(q.Context)
	queueSize := q.QueueSize
	if queueSize == 0 {
		queueSize = 100
	}
	q.line = make(chan T, queueSize)
	errsSize := queueSize / 10
	if errsSize < 1 {
		errsSize = 1
	}
	q.errs = make(chan error, errsSize)
	numWorkers := q.NumWorkers
	if numWorkers == 0 {
		numWorkers = runtime.NumCPU()
	}
	for i := 1; i <= numWorkers; i++ {
		go q.work(context.WithValue(ctx, workerIDContextKey, i))
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-q.errs:
				go q.ErrorHandler(err)
			}
		}
	}()
	return func(err error) {
		stop(err)
		close(q.line)
		close(q.errs)
	}
}

func (q *queue[T]) work(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-q.line:
			func() {
				defer func() {
					if err := recover(); err != nil {
						q.errs <- err.(error)
					}
				}()
				if err := q.Runner(ctx, data); err != nil {
					q.errs <- err
				}
			}()
		}
	}
}

var workerIDContextKey struct{}

func WorkerID(ctx context.Context) int {
	key := ctx.Value(workerIDContextKey)
	if id, ok := key.(int); ok {
		return id
	}
	return 0
}
