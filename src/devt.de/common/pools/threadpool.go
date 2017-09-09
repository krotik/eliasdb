/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package pools contains object pooling utilities.
*/
package pools

import (
	"math"
	"sync"
	"time"
)

/*
Different states of a thread pool.
*/
const (
	StatusRunning  = "Running"
	StatusStopping = "Stopping"
	StatusStopped  = "Stopped"
)

/*
Task is a task which should be run in a thread.
*/
type Task interface {

	/*
		Run the task.
	*/
	Run() error

	/*
		HandleError handles an error which occured during the run method.
	*/
	HandleError(e error)
}

/*
TaskQueue is a queue of tasks for a thread pool.
*/
type TaskQueue interface {

	/*
		Clear the queue of all pending tasks
	*/
	Clear()

	/*
		Pop returns the next task from the queue.
	*/
	Pop() Task
	/*
		Push adds another task to the queue.
	*/
	Push(t Task)

	/*
		Size returns the size of the queue.
	*/
	Size() int
}

/*
DefaultTaskQueue implements a simple (FIFO) task queue for a thread pool.
*/
type DefaultTaskQueue struct {
	queue []Task
}

/*
Clear the queue of all pending tasks
*/
func (tq *DefaultTaskQueue) Clear() {
	tq.queue = make([]Task, 0)
}

/*
Pop returns the next task from the queue.
*/
func (tq *DefaultTaskQueue) Pop() Task {
	var task Task

	if len(tq.queue) > 0 {
		task = tq.queue[0]
		tq.queue = tq.queue[1:]
	}

	return task
}

/*
Push adds another task to the queue.
*/
func (tq *DefaultTaskQueue) Push(t Task) {
	tq.queue = append(tq.queue, t)
}

/*
Size returns the size of the queue.
*/
func (tq *DefaultTaskQueue) Size() int {
	return len(tq.queue)
}

/*
ThreadPool creates a pool of threads which process tasks according to a given
task queue. The threads are kept in an idle state if no more tasks are available.
They resume immediately once a new task is added.
*/
type ThreadPool struct {

	// Task regulation

	queue     TaskQueue   // Task queue for thread pool
	queueLock *sync.Mutex // Lock for queue

	// Worker regulation

	workerIDCount uint64                       // Id counter for worker tasks
	workerMap     map[uint64]*ThreadPoolWorker // Map of all workers
	workerIdleMap map[uint64]*ThreadPoolWorker // Map of all idle workers
	workerMapLock *sync.Mutex                  // Lock for worker map
	workerKill    int                          // Count of workers which should die
	newTaskCond   *sync.Cond                   // Waiting condition for new tasks

	// Callbacks to regulate load

	RegulationLock *sync.Mutex // Lock for regulation variables

	TooManyThreshold int    // Threshold for too many tasks
	TooManyCallback  func() // Callback for too many tasks
	tooManyTriggered bool   // Flag if too many tasks threshold was passed

	TooFewThreshold int    // Threshold for too few tasks
	TooFewCallback  func() // Callback for too few tasks
	tooFewTriggered bool   // Flag if too many tasks threshold was passed
}

/*
NewThreadPool creates a new thread pool.
*/
func NewThreadPool() *ThreadPool {
	return NewThreadPoolWithQueue(&DefaultTaskQueue{})
}

/*
NewThreadPoolWithQueue creates a new thread pool with a specific task queue.
*/
func NewThreadPoolWithQueue(q TaskQueue) *ThreadPool {
	return &ThreadPool{q, &sync.Mutex{},
		0, make(map[uint64]*ThreadPoolWorker),
		make(map[uint64]*ThreadPoolWorker), &sync.Mutex{},
		0, sync.NewCond(&sync.Mutex{}), &sync.Mutex{},
		math.MaxInt32, func() {}, false, 0, func() {}, false}
}

/*
AddTask adds a task to the thread pool.
*/
func (tp *ThreadPool) AddTask(t Task) {
	tp.queueLock.Lock()
	defer tp.queueLock.Unlock()

	tp.queue.Push(t)

	// Reset too few flag

	tp.RegulationLock.Lock()

	if tp.tooFewTriggered && tp.TooFewThreshold < tp.queue.Size() {
		tp.tooFewTriggered = false
	}

	// Check too many

	if !tp.tooManyTriggered && tp.TooManyThreshold <= tp.queue.Size() {
		tp.tooManyTriggered = true
		tp.TooManyCallback()
	}

	tp.RegulationLock.Unlock()

	// Wake up a waiting worker

	tp.newTaskCond.Signal()
}

/*
getTask is called by a worker to request a new task. The worker is expected to finish
if this function returns nil.
*/
func (tp *ThreadPool) getTask() Task {
	var returnIdleTask = true

	// Check if tasks should be stopped

	tp.workerMapLock.Lock()
	if tp.workerKill > 0 {
		tp.workerKill--
		tp.workerMapLock.Unlock()
		return nil

	} else if tp.workerKill == -1 {

		// Check for special worker kill value which is used when workers should
		// be killed when no more tasks are available.

		returnIdleTask = false
	}
	tp.workerMapLock.Unlock()

	// Check if there is a task available

	tp.queueLock.Lock()
	task := tp.queue.Pop()
	tp.queueLock.Unlock()

	if task != nil {
		return task
	}

	tp.RegulationLock.Lock()

	// Reset too many flag

	if tp.tooManyTriggered && tp.TooManyThreshold > tp.queue.Size() {
		tp.tooManyTriggered = false
	}

	// Check too few

	if !tp.tooFewTriggered && tp.TooFewThreshold >= tp.queue.Size() {
		tp.tooFewTriggered = true
		tp.TooFewCallback()
	}

	tp.RegulationLock.Unlock()

	if returnIdleTask {

		// No new task available return idle task

		return &idleTask{tp}
	}

	return nil
}

/*
SetWorkerCount sets the worker count of this pool. If the wait flag is true then
this call will return after the pool has reached the requested worker count.
*/
func (tp *ThreadPool) SetWorkerCount(count int, wait bool) {

	tp.workerMapLock.Lock()
	workerCount := len(tp.workerMap)
	tp.workerMapLock.Unlock()

	if count < 0 {
		count = 0
	}

	if workerCount < count {

		// More workers are needed

		tp.workerMapLock.Lock()

		// Make sure no more workers are killed

		tp.workerKill = 0

		for len(tp.workerMap) != count {
			worker := &ThreadPoolWorker{tp.workerIDCount, tp}
			go worker.run()
			tp.workerMap[tp.workerIDCount] = worker
			tp.workerIDCount++
		}

		tp.workerMapLock.Unlock()

	} else if workerCount > count {

		// Fewer workers are needed

		tp.workerMapLock.Lock()
		tp.workerKill = workerCount - count
		tp.workerMapLock.Unlock()

		tp.newTaskCond.Broadcast()

		if wait {
			for true {
				tp.workerMapLock.Lock()
				workerCount = len(tp.workerMap)
				tp.workerMapLock.Unlock()

				if workerCount == count {
					break
				}

				time.Sleep(5 * time.Nanosecond)

				// Broadcast again since sine workers might be now waiting

				tp.newTaskCond.Broadcast()
			}
		}
	}
}

/*
Status returns the current status of the thread pool.
*/
func (tp *ThreadPool) Status() string {
	var status string

	tp.workerMapLock.Lock()
	workerCount := len(tp.workerMap)
	workerKill := tp.workerKill
	tp.workerMapLock.Unlock()

	if workerCount > 0 {
		if workerKill == -1 {
			status = StatusStopping
		} else {
			status = StatusRunning
		}
	} else {
		status = StatusStopped
	}

	return status
}

/*
WorkerCount returns the current count of workers.
*/
func (tp *ThreadPool) WorkerCount() int {
	tp.workerMapLock.Lock()
	defer tp.workerMapLock.Unlock()
	return len(tp.workerMap)
}

/*
WaitAll waits for all workers to become idle.
*/
func (tp *ThreadPool) WaitAll() {

	// Wake up all workers

	tp.newTaskCond.Broadcast()

	time.Sleep(5 * time.Nanosecond)

	for true {

		tp.workerMapLock.Lock()
		tp.queueLock.Lock()

		// Get total number of workers and idle workers

		workerCount := len(tp.workerMap)
		workerIdleCount := len(tp.workerIdleMap)

		// Get number of pending tasks

		tasks := tp.queue.Size()

		tp.queueLock.Unlock()
		tp.workerMapLock.Unlock()

		// Only leave this loop if either no workers are left or if all
		// tasks are done and all workers are idle

		if workerCount == 0 || (workerCount == workerIdleCount && tasks == 0) {
			break
		}

		time.Sleep(5 * time.Nanosecond)

		// Broadcast again and again until all workers are idle

		tp.newTaskCond.Broadcast()
	}
}

/*
JoinAll processes all remaining tasks and kills off all workers afterwards.
*/
func (tp *ThreadPool) JoinAll() {

	// Tell all workers to die

	tp.workerMapLock.Lock()
	tp.workerKill = -1
	tp.workerMapLock.Unlock()

	tp.newTaskCond.Broadcast()

	for true {

		tp.workerMapLock.Lock()
		tp.queueLock.Lock()

		// Get total number of workers

		workerCount := len(tp.workerMap)

		// Get number of pending tasks

		tasks := tp.queue.Size()

		tp.queueLock.Unlock()
		tp.workerMapLock.Unlock()

		// Only leave this loop if no workers are existing and all tasks are done

		if workerCount == 0 && tasks == 0 {
			break
		}

		time.Sleep(5 * time.Nanosecond)

		// Broadcast again and again until all workers are dead

		tp.newTaskCond.Broadcast()
	}
}

/*
ThreadPoolWorker models a worker in the thread pool.
*/
type ThreadPoolWorker struct {
	id   uint64      // ID of the thread pool worker
	pool *ThreadPool // Thread pool of this worker
}

/*
run lets this worker run tasks.
*/
func (w *ThreadPoolWorker) run() {

	for true {

		// Try to get the next task

		task := w.pool.getTask()

		// Exit if there is not new task

		if task == nil {
			break
		}

		_, isIdleTask := task.(*idleTask)

		if isIdleTask {

			// Register this worker as idle

			w.pool.workerMapLock.Lock()
			w.pool.workerIdleMap[w.id] = w
			w.pool.workerMapLock.Unlock()
		}

		// Run the task

		if err := task.Run(); err != nil {
			task.HandleError(err)
		}

		if isIdleTask {
			w.pool.workerMapLock.Lock()
			delete(w.pool.workerIdleMap, w.id)
			w.pool.workerMapLock.Unlock()
		}
	}

	// Remove worker from workerMap

	w.pool.workerMapLock.Lock()
	delete(w.pool.workerMap, w.id)
	w.pool.workerMapLock.Unlock()
}

/*
idleTask is the internal idle task.
*/
type idleTask struct {
	tp *ThreadPool
}

/*
Run the idle task.
*/
func (t *idleTask) Run() error {
	t.tp.newTaskCond.L.Lock()
	t.tp.newTaskCond.Wait()
	t.tp.newTaskCond.L.Unlock()
	return nil
}

func (t *idleTask) HandleError(e error) {
	panic(e.Error())
}
