/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package datautil

import (
	"fmt"
	"testing"
)

func TestRingBuffer(t *testing.T) {

	rb := NewRingBuffer(3)

	if !rb.IsEmpty() {
		t.Error("Initial buffer should be empty")
		return
	}

	if rb.Poll() != nil {
		t.Error("Initial buffer should be empty")
		return
	}

	if rb.Size() != 0 {
		t.Error("Unexpected size:", rb.Size())
		return
	}

	rb.Add("AAA")

	if rb.Size() != 1 {
		t.Error("Unexpected size:", rb.Size())
		return
	}

	rb.Add("BBB")
	rb.Add("CCC")

	if rb.Size() != 3 {
		t.Error("Unexpected size:", rb.Size())
		return
	}

	if rb.String() != `
AAA
BBB
CCC`[1:] {
		t.Error("Unexpected result:", rb.String())
		return
	}

	rb.Log("DDD\nEEE")
	if rb.Size() != 3 {
		t.Error("Unexpected size:", rb.Size())
		return
	}

	if rb.String() != `
CCC
DDD
EEE`[1:] {
		t.Error("Unexpected result:", rb.String())
		return
	}

	if p := rb.Poll(); p != "CCC" {
		t.Error("Unexpected result:", p)
		return
	}

	if rb.Size() != 2 {
		t.Error("Unexpected size:", rb.Size())
		return
	}

	if p := rb.Get(rb.Size() - 1); p != "EEE" {
		t.Error("Unexpected result:", p)
		return
	}

	rb = NewRingBuffer(100)

	rb.Add("AAA")

	if s := rb.String(); s != "AAA" {
		t.Error("Unexpected result:", s)
		return
	}

	rb.Add("BBB")

	if s := rb.String(); s != "AAA\nBBB" {
		t.Error("Unexpected result:", s)
		return
	}

	if s := rb.Slice(); fmt.Sprint(s) != "[AAA BBB]" {
		t.Error("Unexpected result:", s)
		return
	}

	rb.Reset()

	if !rb.IsEmpty() {
		t.Error("Buffer shoudl be empty after a reset")
		return
	}
}
