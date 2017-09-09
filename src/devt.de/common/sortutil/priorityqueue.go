/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package sortutil

import (
	"bytes"
	"container/heap"
	"fmt"
)

/*
PriorityQueue is like a regular queue where each element has a priority. Items with
higher priority are served first. Items with the same priority are returned in the
order they were added. Priority 0 is the highest priority with the priority
decreasing as the priority number increases.

It is possible to set a minimum priority function on the PriorityQueue object.
The function returns the current minimum priority level which should be returned
by the queue. If the current available priority is lower than this then len()
will return 0 and pop will return nil. If the function returns a negative value
then the value is ignored.
*/
type PriorityQueue struct {
	heap         *priorityQueueHeap // Heap which holds the values
	orderCounter int
	MinPriority  func() int // Function returning the minimum priority
}

/*
NewPriorityQueue creates a new priority queue.
*/
func NewPriorityQueue() *PriorityQueue {

	pqheap := make(priorityQueueHeap, 0)
	pq := &PriorityQueue{&pqheap, 0, func() int { return -1 }}

	heap.Init(pq.heap)

	return pq
}

/*
Clear clears the current queue contents.
*/
func (pq *PriorityQueue) Clear() {
	pqheap := make(priorityQueueHeap, 0)
	pq.heap = &pqheap
	pq.orderCounter = 0
	heap.Init(pq.heap)
}

/*
CurrentPriority returns the priority of the next item.
*/
func (pq *PriorityQueue) CurrentPriority() int {
	if len(*pq.heap) == 0 {
		return 0
	}

	return pq.heap.Peek().(*pqItem).priority
}

/*
Push adds a new element to the queue.
*/
func (pq *PriorityQueue) Push(value interface{}, priority int) {

	// Highest priority is 0 we can't go higher

	if priority < 0 {
		priority = 0
	}

	heap.Push(pq.heap, &pqItem{value, priority, pq.orderCounter, 0})
	pq.orderCounter++
}

/*
Peek returns the next item of the queue but does not remove it.
*/
func (pq *PriorityQueue) Peek() interface{} {
	minPriority := pq.MinPriority()

	if len(*pq.heap) == 0 || (minPriority > 0 && pq.heap.Peek().(*pqItem).priority > minPriority) {
		return nil
	}

	return pq.heap.Peek().(*pqItem).value
}

/*
Pop remove the next element from the queue and returns it.
*/
func (pq *PriorityQueue) Pop() interface{} {
	minPriority := pq.MinPriority()

	if len(*pq.heap) == 0 || (minPriority > 0 && pq.heap.Peek().(*pqItem).priority > minPriority) {
		return nil
	}

	return heap.Pop(pq.heap).(*pqItem).value
}

/*
Size returns the current queue size.
*/
func (pq *PriorityQueue) Size() int {
	minPriority := pq.MinPriority()

	if len(*pq.heap) == 0 || (minPriority > 0 && pq.heap.Peek().(*pqItem).priority > minPriority) {
		return 0
	}
	return len(*pq.heap)
}

/*
SizeCurrentPriority returns the queue size of all elements of the highest priority.
*/
func (pq *PriorityQueue) SizeCurrentPriority() int {
	minPriority := pq.MinPriority()

	if len(*pq.heap) == 0 || (minPriority > 0 && pq.heap.Peek().(*pqItem).priority > minPriority) {
		return 0
	}

	higestPriority := pq.heap.Peek().(*pqItem).priority
	counter := 0

	for _, item := range *pq.heap {
		if item.priority == higestPriority {
			counter++
		}
	}

	return counter
}

/*
String returns a string representation of the queue.
*/
func (pq *PriorityQueue) String() string {
	var ret bytes.Buffer

	ret.WriteString("[ ")

	for _, item := range *pq.heap {
		ret.WriteString(fmt.Sprintf("%v (%v) ", item.value, item.priority))
	}

	ret.WriteString("]")

	return ret.String()
}

// Internal datastructures
// =======================

/*
pqItem models an item in the priority queue.
*/
type pqItem struct {
	value    interface{} // Value which is held in the queue
	priority int         // Priority of the item
	order    int         // Order of adding
	index    int         // Item index in the heap (required by heap).
}

/*
priorityQueueHeap implements the heap.Interface and is the datastructure which
actually holds items.
*/
type priorityQueueHeap []*pqItem

func (pq priorityQueueHeap) Len() int { return len(pq) }
func (pq priorityQueueHeap) Less(i, j int) bool {
	if pq[i].priority != pq[j].priority {
		return pq[i].priority < pq[j].priority
	}

	return pq[i].order < pq[j].order
}
func (pq priorityQueueHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

/*
Push adds an item to the queue.
*/
func (pq *priorityQueueHeap) Push(x interface{}) {
	n := len(*pq)
	item := x.(*pqItem)

	item.index = n

	*pq = append(*pq, item)
}

/*
Pop removes an item from the queue.
*/
func (pq *priorityQueueHeap) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]

	item.index = -1
	*pq = old[0 : n-1]

	return item
}

/*
Peek returns the next item but does not remove it from the queue.
*/
func (pq *priorityQueueHeap) Peek() interface{} {
	q := *pq
	return q[0]
}
