package queue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

type testData struct {
	V string
}

var q Queue[testData]

func TestMain(m *testing.M) {
	done := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q = New[testData](Config[testData]{
		Context:    ctx,
		NumWorkers: 3,
		QueueSize:  100,
		ErrorHandler: func(err error) {
			if err.Error() != "test 5555" && err.Error() != "panic: test 8888" {
				fmt.Println("expected test 5555 error, got", err.Error(), "instead")
				os.Exit(1)
			}
		},
		Runner: func(ctx context.Context, data testData) error {
			workerID := WorkerID(ctx)
			if data.V == "test100" {
				fmt.Println("Worker ID:", workerID)
				fmt.Println("Test 100 passed")
			}
			if data.V == "test9999" {
				fmt.Println("Worker ID:", workerID)
				fmt.Println("Test 9999 passed")
				time.Sleep(time.Second)
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

	if err := q.Start(); err != nil {
		fmt.Println("start queue:", err)
		os.Exit(1)
	}
	defer q.Stop(errors.New("stop"))

	if code := m.Run(); code != 0 {
		os.Exit(code)
	}
	<-done
}

func TestPush(t *testing.T) {
	for i := 0; i < 10_000; i++ {
		q.Push(testData{V: "test" + strconv.Itoa(i)})
	}
}
