package queue

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
)

// New factory generates a new Queue, configured by Config.
func New[T any](config Config[T]) Queue[T] {
	q := &queue[T]{
		Config: config,
	}
	return q
}

// Config struct
type Config[T any] struct {
	// Base context of the queue. Used as base for every execution.
	Context context.Context

	// Runner is required, if not configure the Start func will return an error.
	Runner RunnerFn[T]

	// ErrorHandler is optional to handle errors, returned by runners.
	ErrorHandler func(error)

	// QueueSize is the maximum size of items in queue, if overloaded the next
	// item, pushed, will be blocked until there's a place in queue. Default 100.
	QueueSize int

	// NumWorkers defines the number of workers to handle the queue items.
	NumWorkers int
}

// Queue interface
type Queue[T any] interface {
	// Start the workers.
	Start() error

	// Stop the workers.
	Stop(error)

	// Push an item for execution.
	Push(T)
}

// RunnerFn is the queue handler.
type RunnerFn[T any] func(context.Context, T) error

// StartFn func.
type StartFn func() error

// StopFn func.
type StopFn func(err error)

type queue[T any] struct {
	Config[T]
	startMu sync.Once
	stopMu  sync.Once
	line    chan T
	errs    chan error
	stopFn  StopFn
}

func (q *queue[T]) Push(data T) {
	go func(data T) { q.line <- data }(data)
}

func (q *queue[T]) Start() error {
	if q.Context == nil {
		q.Context = context.Background()
	}
	if q.ErrorHandler == nil {
		q.ErrorHandler = func(_ error) {}
	}
	if q.Runner == nil {
		return errors.New("cannot create queue with nil runner")
	}
	q.startMu.Do(func() {
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
		q.stopFn = func(err error) {
			stop(err)
			close(q.line)
			close(q.errs)
		}
	})
	return nil
}

func (q *queue[T]) Stop(err error) {
	if q.stopFn != nil {
		q.stopMu.Do(func() {
			q.stopFn(err)
		})
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
						q.errs <- fmt.Errorf("panic: %v", err)
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
