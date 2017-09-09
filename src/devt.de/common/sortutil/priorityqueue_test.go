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
	"fmt"
	"testing"
)

func TestPriorityQueue(t *testing.T) {

	pq := NewPriorityQueue()

	if pq.CurrentPriority() != 0 {
		t.Error("Unexpected priority:", pq.CurrentPriority())
		return
	}

	pq.Push("test1", 1)
	pq.Push("test8", 8)
	pq.Push("test2", 2)
	pq.Push("test5", 5)

	// Check contents:

	if res := fmt.Sprint(pq); res != "[ test1 (1) test5 (5) test2 (2) test8 (8) ]" {
		t.Error("Unexpected queue layout:", res)
		return
	}

	if pq.CurrentPriority() != 1 {
		t.Error("Unexpected priority:", pq.CurrentPriority())
		return
	}

	if pq.Size() != 4 {
		t.Error("Unexpected size:", pq.Size())
		return
	}

	if pq.SizeCurrentPriority() != 1 {
		t.Error("Unexpected size:", pq.SizeCurrentPriority())
		return
	}

	// Set minpriority function

	pq.MinPriority = func() int {
		return 1
	}

	peek := pq.Peek()
	if res := pq.Pop(); res != "test1" && res == peek {
		t.Error("Unexpected pop result:", res)
		return
	}

	if res := fmt.Sprint(pq); res != "[ test2 (2) test5 (5) test8 (8) ]" {
		t.Error("Unexpected queue layout:", res)
		return
	}

	peek = pq.Peek()
	if res := pq.Pop(); res != nil && res == peek {
		t.Error("Unexpected pop result:", res)
		return
	}

	if res := pq.Size(); res != 0 {
		t.Error("Unexpected pop result:", res)
		return
	}

	if res := pq.SizeCurrentPriority(); res != 0 {
		t.Error("Unexpected pop result:", res)
		return
	}

	pq.MinPriority = func() int { return -1 }

	peek = pq.Peek()
	if res := pq.Pop(); res != "test2" && res == peek {
		t.Error("Unexpected pop result:", res)
		return
	}

	peek = pq.Peek()

	if pq.CurrentPriority() != 5 {
		t.Error("Unexpected priority:", pq.CurrentPriority())
		return
	}

	if res := pq.Pop(); res != "test5" && res == peek {
		t.Error("Unexpected pop result:", res)
		return
	}

	if pq.CurrentPriority() != 8 {
		t.Error("Unexpected priority:", pq.CurrentPriority())
		return
	}

	peek = pq.Peek()
	if res := pq.Pop(); res != "test8" && res == peek {
		t.Error("Unexpected pop result:", res)
		return
	}

	pq.Push("test2", 9)

	if pq.CurrentPriority() != 9 {
		t.Error("Unexpected priority:", pq.CurrentPriority())
		return
	}

	pq.Clear()

	if pq.CurrentPriority() != 0 {
		t.Error("Unexpected priority:", pq.CurrentPriority())
		return
	}

	if res := pq.Size(); res != 0 {
		t.Error("Unexpected pop result:", res)
		return
	}

	if res := fmt.Sprint(pq); res != "[ ]" {
		t.Error("Unexpected queue layout:", res)
		return
	}

	// Test we can use it as a normal queue

	pq.Push("test1", 0)
	pq.Push("test8", -1)
	pq.Push("test2", 0)
	pq.Push("test5", 0)

	if res := pq.Size(); res != 4 {
		t.Error("Unexpected pop result:", res)
		return
	}

	if res := pq.SizeCurrentPriority(); res != 4 {
		t.Error("Unexpected pop result:", res)
		return
	}

	pq.MinPriority = func() int {
		return 0
	}

	peek = pq.Peek()
	if res := pq.Pop(); res != "test1" && res == peek {
		t.Error("Unexpected pop result:", res)
		return
	}

	if res := pq.Size(); res != 3 {
		t.Error("Unexpected pop result:", res)
		return
	}

	peek = pq.Peek()
	if res := pq.Pop(); res != "test8" && res == peek {
		t.Error("Unexpected pop result:", res)
		return
	}

	peek = pq.Peek()
	if res := pq.Pop(); res != "test2" && res == peek {
		t.Error("Unexpected pop result:", res)
		return
	}

	peek = pq.Peek()
	if res := pq.Pop(); res != "test5" && res == peek {
		t.Error("Unexpected pop result:", res)
		return
	}

	if pq.CurrentPriority() != 0 {
		t.Error("Unexpected priority:", pq.CurrentPriority())
		return
	}

}
