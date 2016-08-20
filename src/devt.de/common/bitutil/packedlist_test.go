/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package bitutil

import (
	"fmt"
	"math"
	"testing"
)

func TestListPacking(t *testing.T) {
	mylist := make([]uint64, 7)
	mylist[0] = 3
	mylist[1] = 7
	mylist[2] = 63
	mylist[3] = math.MaxUint8
	mylist[4] = math.MaxUint16
	mylist[5] = math.MaxUint32
	mylist[6] = math.MaxUint64

	res := UnpackList(PackList(mylist, 3))
	if res[0] != 3 {
		t.Error("Unexpected result:", res)
		return
	}

	res = UnpackList(PackList(mylist, 7))
	if fmt.Sprint(res[:2]) != "[3 7]" {
		t.Error("Unexpected result:", res[:2])
		return
	}

	res = UnpackList(PackList(mylist, 63))
	if fmt.Sprint(res[:3]) != "[3 7 63]" {
		t.Error("Unexpected result:", res[:3])
		return
	}

	res = UnpackList(PackList(mylist, math.MaxUint8))
	if fmt.Sprint(res[:4]) != "[3 7 63 255]" {
		t.Error("Unexpected result:", res[:4])
		return
	}

	res = UnpackList(PackList(mylist, math.MaxUint16))
	if fmt.Sprint(res[:5]) != "[3 7 63 255 65535]" {
		t.Error("Unexpected result:", res[:5])
		return
	}

	res = UnpackList(PackList(mylist, math.MaxUint32))
	if fmt.Sprint(res[:6]) != "[3 7 63 255 65535 4294967295]" {
		t.Error("Unexpected result:", res[:6])
		return
	}

	res = UnpackList(PackList(mylist, math.MaxUint64))
	if fmt.Sprint(res[:7]) != "[3 7 63 255 65535 4294967295 18446744073709551615]" {
		t.Error("Unexpected result:", res[:7])
		return
	}

	res = UnpackList(PackList([]uint64{10, 12, 80}, 80))
	if fmt.Sprint(res) != "[10 12 80]" {
		t.Error("Unexpected result:", res)
		return
	}
}

func TestListPacking8(t *testing.T) {
	list1 := PackList3Bit([]byte{1, 2, 3, 4, 5, 6, 7})
	list2 := PackList16Bit([]uint16{1, 2, 3, 4})

	if len(list1) != 4 || len(list2) != 9 {
		t.Error("Unexpected lengths:", len(list1), len(list2))
		return
	}

	res1 := UnpackList(list1)
	res2 := UnpackList(list2)

	if fmt.Sprint(res1) != "[1 2 3 4 5 6 7]" {
		t.Error("Unexpected result:", res1)
		return
	}
	if fmt.Sprint(res2) != "[1 2 3 4]" {
		t.Error("Unexpected result:", res2)
		return
	}

	if UnpackList("") != nil {
		t.Error("Unexpected result")
		return
	}
}

func TestVarBitListPacking8(t *testing.T) {
	scale := 3

	testlist := make([]uint8, scale)

	for i := 0; i < scale; i++ {
		testlist[i] = math.MaxUint8
	}

	res := PackList8Bit(testlist)

	if len(res) != scale+1 {
		t.Error("Unexpected length:", len(res))
		return
	}

	res2 := UnpackBigList(res)

	for i := 0; i < scale; i++ {
		if testlist[i] != uint8(res2[i]) {
			t.Error("Unexpected result at:", i)
		}
	}
}

func TestVarBitListPacking16(t *testing.T) {
	scale := 3

	testlist := make([]uint16, scale)

	for i := 0; i < scale; i++ {
		testlist[i] = math.MaxUint16
	}

	res := PackList16Bit(testlist)

	if len(res) != scale*2+1 {
		t.Error("Unexpected length:", len(res))
		return
	}

	res2 := UnpackBigList(res)

	for i := 0; i < scale; i++ {
		if testlist[i] != uint16(res2[i]) {
			t.Error("Unexpected result at:", i)
		}
	}
}

func TestVarBitListPacking32(t *testing.T) {
	scale := 3

	testlist := make([]uint32, scale)

	for i := 0; i < scale; i++ {
		testlist[i] = math.MaxUint32
	}

	res := PackList32Bit(testlist)

	if len(res) != scale*4+1 {
		t.Error("Unexpected length:", len(res))
		return
	}

	res2 := UnpackBigList(res)

	for i := 0; i < scale; i++ {
		if testlist[i] != uint32(res2[i]) {
			t.Error("Unexpected result at:", i)
		}
	}
}

func TestVarBitListPacking64(t *testing.T) {
	scale := 3

	testlist := make([]uint64, scale)

	for i := 0; i < scale; i++ {
		testlist[i] = math.MaxUint64
	}

	res := PackList64Bit(testlist)

	if len(res) != scale*8+1 {
		t.Error("Unexpected length:", len(res))
		return
	}

	res2 := UnpackBigList(res)

	for i := 0; i < scale; i++ {
		if testlist[i] != uint64(res2[i]) {
			t.Error("Unexpected result at:", i)
		}
	}
}

func TestSmallListPacking(t *testing.T) {

	// Test simple cases

	if PackList2Bit([]byte{}) != "" {
		t.Error("Unexpected result")
		return
	}

	if PackList3Bit([]byte{}) != "" {
		t.Error("Unexpected result")
		return
	}

	if PackList6Bit([]byte{}) != "" {
		t.Error("Unexpected result")
		return
	}

	if string(UnpackSmallList("")) != "" {
		t.Error("Unexpected result")
		return
	}

	// Simulates a gob encoded string

	if string(UnpackSmallList(string([]byte{0x00}))) != string(0x00) {
		t.Error("Unexpected result")
		return
	}

	// Test normal cases

	checkListAndPresentation2bit(t, []byte{1, 2, 3, 1, 2, 3}, []byte{0x5b, 0x6c}, 2)
	checkListAndPresentation2bit(t, []byte{1}, []byte{0x50}, 1)
	checkListAndPresentation2bit(t, []byte{1, 2}, []byte{0x58}, 1)
	checkListAndPresentation2bit(t, []byte{1, 2, 3}, []byte{0x5B}, 1)
	checkListAndPresentation2bit(t, []byte{1, 2, 3, 3}, []byte{0x5B, 0xC0}, 2)
	checkListAndPresentation2bit(t, []byte{1, 2, 3, 3, 2}, []byte{0x5B, 0xE0}, 2)
	checkListAndPresentation2bit(t, []byte{1, 2, 3, 3, 2, 1, 3}, []byte{0x5B, 0xE7}, 2)

	checkListAndPresentation3bit(t, []byte{1, 2, 3, 1, 2, 3}, []byte{0x8A, 0x19, 0x13}, 3)
	checkListAndPresentation3bit(t, []byte{1}, []byte{0x88}, 1)
	checkListAndPresentation3bit(t, []byte{1, 2}, []byte{0x8A}, 1)
	checkListAndPresentation3bit(t, []byte{1, 2, 3}, []byte{0x8A, 0x18}, 2)
	checkListAndPresentation3bit(t, []byte{1, 2, 3, 3}, []byte{0x8A, 0x1B}, 2)
	checkListAndPresentation3bit(t, []byte{1, 2, 3, 4, 5, 6, 7}, []byte{0x8A, 0x1C, 0x2E, 0x38}, 4)

	checkListAndPresentation6bit(t, []byte{1, 2, 3, 1, 2, 3})
	checkListAndPresentation6bit(t, []byte{1})
	checkListAndPresentation6bit(t, []byte{1, 2})
	checkListAndPresentation6bit(t, []byte{1, 2, 3})
	checkListAndPresentation6bit(t, []byte{1, 2, 3, 3})
	checkListAndPresentation6bit(t, []byte{1, 2, 3, 4, 35, 45, 63})
}

func checkListAndPresentation2bit(t *testing.T, list []byte, packedlist []byte, packedLen int) {
	res := PackList2Bit(list)
	if res != string(packedlist) {
		t.Errorf("Unexpected result: %X", []byte(res))
		return
	}
	if len(res) != packedLen {
		t.Error("Unexpected size", len(res))
		return
	}
	if dres := UnpackSmallList(res); string(dres) != string(list) {
		t.Errorf("Unexpected result: %X", []byte(dres))
		return
	}
}

func checkListAndPresentation3bit(t *testing.T, list []byte, packedlist []byte, packedLen int) {
	res := PackList3Bit(list)
	if res != string(packedlist) {
		t.Errorf("Unexpected result: %X", []byte(res))
		return
	}
	if len(res) != packedLen {
		t.Error("Unexpected size", len(res))
		return
	}
	if dres := UnpackSmallList(res); string(dres) != string(list) {
		t.Errorf("Unexpected result: %X", []byte(dres))
		return
	}
}

func checkListAndPresentation6bit(t *testing.T, list []byte) {
	res := PackList6Bit(list)

	packedlist := make([]byte, len(list))
	copy(packedlist, list)
	packedlist[0] = packedlist[0] | 0xC0

	if res != string(packedlist) {
		t.Errorf("Unexpected result: %X vs %X", []byte(res), packedlist)
		return
	}
	if len(res) != len(list) {
		t.Error("Unexpected size", len(res))
		return
	}
	if dres := UnpackSmallList(res); string(dres) != string(list) {
		t.Errorf("Unexpected result: %X", []byte(dres))
		return
	}
}

func TestList2byte2bit(t *testing.T) {
	if res := list2byte2bit(0x01, 0x2, 0x03, 0x01); res != 0x6D {
		t.Errorf("Unexpected result: %X", res)
		return
	}
	if res := list2byte3bitAndHeader(0x00, 0x07, 0x03); res != 0x3B {
		t.Errorf("Unexpected result: %X", res)
		return
	}
}

func TestByte2list2bit(t *testing.T) {
	if a, b, c, d := byte2list2bit(0x30); a != 00 || b != 03 || c != 00 || d != 00 {
		t.Error("Unexpected result:", a, b, c, d)
		return
	}
	if a, b, c, d := byte2list2bit(0x80); a != 02 || b != 00 || c != 00 || d != 00 {
		t.Error("Unexpected result:", a, b, c, d)
		return
	}
	if a, b, c, d := byte2list2bit(0x01); a != 00 || b != 00 || c != 00 || d != 01 {
		t.Error("Unexpected result:", a, b, c, d)
		return
	}
	if a, b, c, d := byte2list2bit(0x31); a != 00 || b != 03 || c != 00 || d != 01 {
		t.Error("Unexpected result:", a, b, c, d)
		return
	}
	if a, b, c, d := byte2list2bit(0x05); a != 00 || b != 00 || c != 01 || d != 01 {
		t.Error("Unexpected result:", a, b, c, d)
		return
	}
}

func TestByte2list3bit(t *testing.T) {
	if a, b := byte2list3bit(0x01); a != 00 || b != 01 {
		t.Error("Unexpected result:", a, b)
		return
	}
	if a, b := byte2list3bit(0x31); a != 06 || b != 01 {
		t.Error("Unexpected result:", a, b)
		return
	}
	if a, b := byte2list3bit(0x05); a != 00 || b != 05 {
		t.Error("Unexpected result:", a, b)
		return
	}
}
