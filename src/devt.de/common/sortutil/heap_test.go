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
	"testing"
)

func TestIntHeap(t *testing.T) {
	h := &IntHeap{2, 1, 5}

	heap.Init(h)
	heap.Push(h, 3)
	heap.Push(h, 8)

	if (*h)[0] != 1 {
		t.Error("Unexpected minimum:", (*h)[0])
		return
	}

	if (*h)[len(*h)-1] != 8 {
		t.Error("Unexpected maximum:", (*h)[len(*h)-1])
		return
	}

	if res := h.Peek(); res != (*h)[0] {
		t.Error("Unexpected peek result:", res)
		return
	}

	var buf bytes.Buffer

	for h.Len() > 0 {
		buf.WriteString(fmt.Sprintf("%d ", heap.Pop(h)))
	}

	if buf.String() != "1 2 3 5 8 " {
		t.Error("Unexpected sort order:", buf.String())
	}

	buf.Reset()

	h = &IntHeap{2, 1, 5}

	heap.Init(h)
	heap.Push(h, 3)
	heap.Push(h, 3)
	heap.Push(h, 8)

	h.RemoveAll(3)

	for h.Len() > 0 {
		buf.WriteString(fmt.Sprintf("%d ", heap.Pop(h)))
	}

	if buf.String() != "1 2 5 8 " {
		t.Error("Unexpected sort order:", buf.String())
	}

	buf.Reset()

	h = &IntHeap{2, 1, 5}

	heap.Init(h)
	heap.Push(h, 3)
	heap.Push(h, 3)
	heap.Push(h, 8)

	h.RemoveFirst(3)

	for h.Len() > 0 {
		buf.WriteString(fmt.Sprintf("%d ", heap.Pop(h)))
	}

	if buf.String() != "1 2 3 5 8 " {
		t.Error("Unexpected sort order:", buf.String())
	}

	heap.Push(h, 3)
	heap.Push(h, 3)
	heap.Push(h, 8)

	h.RemoveFirst(3)
	h.RemoveFirst(3)
	h.RemoveFirst(8)

	if h.Len() != 0 {
		t.Error("Unexpected size:", h.Len())
		return
	}
}
