/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package sortutil

import "container/heap"

/*
IntHeap is a classic heap with int values.
*/
type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

/*
Push adds an item to the heap.
*/
func (h *IntHeap) Push(x interface{}) {
	*h = append(*h, x.(int))
}

/*
Pop removes an item to the heap.
*/
func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]

	*h = old[0 : n-1]

	return x
}

/*
Peek returns the next item but does not remove it like Pop.
*/
func (h *IntHeap) Peek() int {
	return (*h)[0]
}

/*
RemoveFirst removes the first occurences of item r from the IntHeap.
*/
func (h *IntHeap) RemoveFirst(r int) {
	heapList := *h

	for i, item := range heapList {
		if item == r {
			if i+1 < len(heapList) {
				*h = append(heapList[:i], heapList[i+1:]...)
				heap.Fix(h, i)
				break
			} else {
				*h = heapList[:i]
			}
		}
	}
}

/*
RemoveAll removes all occurences of item r from the IntHeap.
*/
func (h *IntHeap) RemoveAll(r int) {
	newHeap := &IntHeap{}

	for len(*h) > 0 {
		item := heap.Pop(h)
		if item != r {
			heap.Push(newHeap, item)
		}
	}

	(*h) = *newHeap
}
