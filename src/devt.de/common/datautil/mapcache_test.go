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
	"testing"
	"time"
)

func TestMapCache(t *testing.T) {

	// Create a map cache which can hold a maximum of 3 items for no longer than
	// 5 seconds

	mc := NewMapCache(3, 5)

	mc.Put("k1", "aaa")
	mc.Put("k2", "bbb")
	mc.Put("k3", "ccc")

	if s := mc.Size(); s != 3 {
		t.Error("Unexpected size:", s)
		return
	}

	mc.Clear()

	if s := mc.Size(); s != 0 {
		t.Error("Unexpected size:", s)
		return
	}

	mc.Put("k1", "aaa")
	mc.Put("k2", "bbb")
	mc.Put("k3", "ccc")

	if s := mc.Size(); s != 3 {
		t.Error("Unexpected size:", s)
		return
	}

	// Test copy

	cp := mc.GetAll()

	if len(cp) != 3 {
		t.Error("Unexpected copy result:", cp)
		return
	}

	// Simulate different timings

	mc.ts["k1"] = time.Now().Unix() - 6 // Expired
	mc.ts["k2"] = time.Now().Unix() - 3 // Oldest entry

	if mc.String() != `
k1:aaa
k2:bbb
k3:ccc
`[1:] {
		t.Error("Unexpected cache content:", mc)
		return
	}

	// Do a read operation on an expired entry

	if e, ok := mc.Get("k1"); e != nil || ok {
		t.Error("Expired entry should not be returned", ok, e)
		return
	}

	if mc.String() != `
k2:bbb
k3:ccc
`[1:] {
		t.Error("Unexpected cache content:", mc)
		return
	}

	// Do a read operation on a live entry

	if e, ok := mc.Get("k2"); e != "bbb" || !ok {
		t.Error("Live entry should be returned", ok, e)
		return
	}

	if mc.String() != `
k2:bbb
k3:ccc
`[1:] {
		t.Error("Unexpected cache content:", mc)
		return
	}

	// Add 1 entry and update another

	mc.Put("k3", "updateccc")
	mc.Put("k4", "ddd")

	if mc.String() != `
k2:bbb
k3:updateccc
k4:ddd
`[1:] {
		t.Error("Unexpected cache content:", mc)
		return
	}

	// Add another entry which should push out the oldest

	mc.Put("k5", "eee")

	if mc.String() != `
k3:updateccc
k4:ddd
k5:eee
`[1:] {
		t.Error("Unexpected cache content:", mc)
		return
	}

	// Remove items

	if !mc.Remove("k3") {
		t.Error("Live item should be deleted")
		return
	}

	if mc.String() != `
k4:ddd
k5:eee
`[1:] {
		t.Error("Unexpected cache content:", mc)
		return
	}

	if mc.Remove("k0") {
		t.Error("Removal of non-existing item should not return success")
		return
	}
}
