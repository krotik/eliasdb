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
	"reflect"
	"testing"
)

func TestInt64s(t *testing.T) {
	testSlice := []int64{5, 2, 3, 0xFFFFFFFF, 1}

	Int64s(testSlice)

	if !reflect.DeepEqual(testSlice, []int64{1, 2, 3, 5, 0xFFFFFFFF}) {
		t.Error("Unexpected sorted order:", testSlice)
		return
	}
}

func TestUInt64s(t *testing.T) {
	testSlice := []uint64{5, 2, 3, 0xFFFFFFFF, 1}

	UInt64s(testSlice)

	if !reflect.DeepEqual(testSlice, []uint64{1, 2, 3, 5, 0xFFFFFFFF}) {
		t.Error("Unexpected sorted order:", testSlice)
		return
	}
}


func TestAbstractSlice(t *testing.T) {
	testSlice := []interface{}{5, 2, "bla", 0xFFFFFFFF, 1}

	InterfaceStrings(testSlice)

	if !reflect.DeepEqual(testSlice, []interface{}{1, 2, 0xFFFFFFFF, 5, "bla"}) {
		t.Error("Unexpected sorted order:", testSlice)
		return
	}
}
