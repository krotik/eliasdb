/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package pools

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type testTask struct {
	task         func() error
	errorHandler func(e error)
}

func (t *testTask) Run() error {
	return t.task()
}

func (t *testTask) HandleError(e error) {
	t.errorHandler(e)
}

func TestDefaultTaskQueue(t *testing.T) {
	var taskFinishCounter int
	var tq DefaultTaskQueue

	if res := tq.Size(); res != 0 {
		t.Error("Initial size should be empty not: ", res)
		return
	}

	if res := tq.Pop(); res != nil {
		t.Error("Unexpected result: ", res)
		return
	}

	tq.Clear()

	if res := tq.Size(); res != 0 {
		t.Error("Initial size should be empty not: ", res)
		return
	}

	if res := tq.Pop(); res != nil {
		t.Error("Unexpected result: ", res)
		return
	}

	tq.Push(&testTask{func() error {
		taskFinishCounter++
		return nil
	}, nil})
	tq.Push(&testTask{func() error {
		taskFinishCounter++
		return nil
	}, nil})
	tq.Push(&testTask{func() error {
		taskFinishCounter++
		return nil
	}, nil})

	if res := tq.Size(); res != 3 {
		t.Error("Unexpected result: ", res)
		return
	}

	// Execute the functions

	tq.Pop().Run()

	if res := tq.Size(); res != 2 {
		t.Error("Unexpected result: ", res)
		return
	}

	tq.Pop().Run()

	if res := tq.Size(); res != 1 {
		t.Error("Unexpected result: ", res)
		return
	}

	tq.Pop().Run()

	if res := tq.Size(); res != 0 {
		t.Error("Unexpected result: ", res)
		return
	}

	if res := tq.Pop(); res != nil {
		t.Error("Unexpected result: ", res)
		return
	}

	if taskFinishCounter != 3 {
		t.Error("Unexpected result: ", taskFinishCounter)
		return
	}
}

func TestThreadPool(t *testing.T) {
	var taskFinishCounter int
	taskFinishCounterLock := &sync.Mutex{}

	tp := NewThreadPool()

	tp.SetWorkerCount(-10, true)
	tp.TooManyThreshold = 1

	if status := tp.Status(); status != StatusStopped {
		t.Error("Unexpected status:", status)
		return
	}

	tp.SetWorkerCount(3, true)

	if status := tp.Status(); status != StatusRunning {
		t.Error("Unexpected status:", status)
		return
	}

	if workers := len(tp.workerMap); workers != 3 {
		t.Error("Unepxected state:", workers)
		return
	}

	tp.AddTask(&testTask{func() error {
		taskFinishCounterLock.Lock()
		taskFinishCounter++
		taskFinishCounterLock.Unlock()
		return nil
	}, nil})
	tp.AddTask(&testTask{func() error {
		taskFinishCounterLock.Lock()
		taskFinishCounter++
		taskFinishCounterLock.Unlock()
		return nil
	}, nil})
	tp.AddTask(&testTask{func() error {
		taskFinishCounterLock.Lock()
		taskFinishCounter++
		taskFinishCounterLock.Unlock()
		return nil
	}, nil})

	tp.JoinAll()

	if workers := len(tp.workerMap); workers != 0 {
		t.Error("Unepxected state:", workers)
		return
	}

	if taskFinishCounter != 3 {
		t.Error("Unexpected result: ", taskFinishCounter)
		return
	}

	if status := tp.Status(); status != StatusStopped {
		t.Error("Unexpected status:", status)
		return
	}

	tp.AddTask(&testTask{func() error {
		taskFinishCounterLock.Lock()
		taskFinishCounter++
		taskFinishCounterLock.Unlock()
		return nil
	}, nil})
	tp.AddTask(&testTask{func() error {
		taskFinishCounterLock.Lock()
		taskFinishCounter++
		taskFinishCounterLock.Unlock()
		return nil
	}, nil})
	tp.AddTask(&testTask{func() error {
		taskFinishCounterLock.Lock()
		taskFinishCounter++
		taskFinishCounterLock.Unlock()
		return nil
	}, nil})
	tp.AddTask(&testTask{func() error {
		taskFinishCounterLock.Lock()
		taskFinishCounter++
		taskFinishCounterLock.Unlock()
		time.Sleep(10 * time.Millisecond)
		return nil
	}, nil})

	if status := tp.Status(); status != StatusStopped {
		t.Error("Unexpected status:", status)
		return
	}

	tp.SetWorkerCount(3, false)

	if workers := len(tp.workerMap); workers != 3 {
		t.Error("Unepxected state:", workers)
		return
	}

	// Let the workers go into the idle state

	time.Sleep(20 * time.Millisecond)

	// Reduce the number of workers

	tp.SetWorkerCount(1, true)

	if workers := len(tp.workerMap); workers != 1 {
		t.Error("Unepxected state:", workers)
		return
	}

	tp.AddTask(&testTask{func() error {
		taskFinishCounterLock.Lock()
		taskFinishCounter++
		taskFinishCounterLock.Unlock()
		return nil
	}, nil})
	tp.AddTask(&testTask{func() error {
		taskFinishCounterLock.Lock()
		taskFinishCounter++
		taskFinishCounterLock.Unlock()
		time.Sleep(10 * time.Millisecond)
		return nil
	}, nil})

	// Set the kill value

	tp.workerKill = -1

	if status := tp.Status(); status != StatusStopping {
		t.Error("Unexpected status:", status)
		return
	}

	tp.WaitAll()

	tp.SetWorkerCount(-5, true)

	if workers := len(tp.workerMap); workers != 0 {
		t.Error("Unepxected state:", workers)
		return
	}

	tp.AddTask(&testTask{func() error {
		taskFinishCounterLock.Lock()
		taskFinishCounter++
		taskFinishCounterLock.Unlock()
		return nil
	}, nil})

	tp.WaitAll()

	if taskFinishCounter != 9 {
		t.Error("Unexpected result: ", taskFinishCounter)
		return
	}

	tp.SetWorkerCount(1, false)

	tp.WaitAll()

	if taskFinishCounter != 10 {
		t.Error("Unexpected result: ", taskFinishCounter)
		return
	}

	tp.SetWorkerCount(0, true)

	if status := tp.Status(); status != StatusStopped {
		t.Error("Unexpected status:", status)
		return
	}
}

func TestThreadPoolThresholds(t *testing.T) {
	var taskFinishCounter int
	taskFinishCounterLock := &sync.Mutex{}

	task := &testTask{func() error {
		time.Sleep(time.Millisecond * 5)
		taskFinishCounterLock.Lock()
		taskFinishCounter++
		taskFinishCounterLock.Unlock()
		return nil
	}, nil}

	var buf bytes.Buffer

	tp := NewThreadPool()

	tp.TooFewThreshold = 1
	tp.TooManyThreshold = 5

	tp.TooFewCallback = func() {
		taskFinishCounterLock.Lock()
		buf.WriteString("low")
		taskFinishCounterLock.Unlock()
	}
	tp.TooManyCallback = func() {
		taskFinishCounterLock.Lock()
		buf.WriteString("high")
		taskFinishCounterLock.Unlock()
	}

	tp.SetWorkerCount(10, false)

	for i := 0; i < 10; i++ {
		tp.AddTask(task)
	}

	if wc := tp.WorkerCount(); wc != 10 {
		t.Error("Unexpected result:", wc)
		return
	}

	tp.SetWorkerCount(10, false)

	tp.WaitAll()

	if wc := tp.WorkerCount(); wc != 10 {
		t.Error("Unexpected result:", wc)
		return
	}

	tp.SetWorkerCount(10, false)

	for i := 0; i < 10; i++ {
		tp.AddTask(task)
	}

	tp.WaitAll()

	if wc := tp.WorkerCount(); wc != 10 {
		t.Error("Unexpected result:", wc)
		return
	}

	if taskFinishCounter != 20 {
		t.Error("Unexpected result:", taskFinishCounter)
		return
	}

	tp.JoinAll()

	if wc := tp.WorkerCount(); wc != 0 {
		t.Error("Unexpected result:", wc)
		return
	}

	// Check that the callbacks where triggered twice each

	if !strings.Contains(buf.String(), "high") {
		t.Error("Unexpected result:", buf.String())
		return
	}
	if !strings.Contains(buf.String(), "low") {
		t.Error("Unexpected result:", buf.String())
		return
	}
}

func TestThreadPoolIdleTaskPanic(t *testing.T) {

	defer func() {
		if r := recover(); r == nil {
			t.Error("Error handling on the idle task did not cause a panic")
		}
	}()

	// Run error handling function of idle task

	idleTask := &idleTask{}
	idleTask.HandleError(nil)
}

func TestThreadPoolErrorHandling(t *testing.T) {

	// Test error normal task handling

	var buf bytes.Buffer

	task := &testTask{func() error {
		return errors.New("testerror")
	}, func(e error) {
		buf.WriteString(e.Error())
	}}

	tp := NewThreadPool()

	tp.AddTask(task)

	if buf.String() != "" {
		t.Error("Unexpected result:", buf.String())
	}

	tp.SetWorkerCount(1, false)
	tp.JoinAll()

	if buf.String() != "testerror" {
		t.Error("Unexpected result:", buf.String())
	}
}
