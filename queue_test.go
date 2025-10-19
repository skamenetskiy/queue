package queue

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

func TestQueue(t *testing.T) {
	done := make(chan struct{})
	type testData struct {
		V string
	}
	q := New[testData](Config[testData]{
		Context:    t.Context(),
		NumWorkers: 3,
		QueueSize:  100,
		ErrorHandler: func(err error) {
			if err.Error() != "test 5555" && err.Error() != "panic: test 8888" {
				t.Error("expected test 5555 error, got", err.Error(), "instead")
			}
		},
		Runner: func(ctx context.Context, data testData) error {
			workerID := WorkerID(ctx)
			if data.V == "test100" {
				t.Logf("Worker ID: %d", workerID)
				t.Logf("Test 100 passed")
			}
			if data.V == "test9999" {
				t.Logf("Worker ID: %d", workerID)
				t.Logf("Test 9999 passed")
				done <- struct{}{}
			}
			if data.V == "test8888" {
				panic("test 8888")
			}
			if data.V == "test5555" {
				return errors.New("test 5555")
			}
			return nil
		},
	})
	go func() {
		time.Sleep(10 * time.Second)
		done <- struct{}{}
	}()
	stop := q.Start()
	defer stop(errors.New("stop"))
	for i := 0; i < 10_000; i++ {
		q.Push(testData{V: "test" + strconv.Itoa(i)})
	}
	<-done
}
