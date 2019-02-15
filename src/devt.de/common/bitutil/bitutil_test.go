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
	"testing"
)

func TestCompareByteArray(t *testing.T) {
	testdata1 := []byte("Test")
	testdata2 := make([]byte, 4, 5)
	testdata3 := make([]byte, 3, 3)

	if CompareByteArray(testdata1, testdata2) {
		t.Error("Byte arrays should not be considered equal before copying data.")
	}

	if CompareByteArray(testdata1, testdata3) {
		t.Error("Byte arrays should not be considered equal if the length is different.")
	}

	copy(testdata2, testdata1)

	if cap(testdata1) == cap(testdata2) {
		t.Error("Capacity of testdata sclices should be different.")
	}

	if !CompareByteArray(testdata1, testdata2) {
		t.Error("Byte arrays should be considered equal.")
	}
}

func TestByteSizeString(t *testing.T) {
	// Test byte sizes
	testdata := []int64{10000, 1024, 500, 1233456, 44166037, 84166037, 5000000000}

	// non-ISU values
	expected1 := []string{"9.8 KiB", "1.0 KiB", "500 B", "1.2 MiB", "42.1 MiB", "80.3 MiB", "4.7 GiB"}

	// ISU values
	expected2 := []string{"10.0 kB", "1.0 kB", "500 B", "1.2 MB", "44.2 MB", "84.2 MB", "5.0 GB"}

	for i, test := range testdata {
		res := ByteSizeString(test, false)
		if res != expected1[i] {
			t.Error("Unexpected value for non-isu value:", test,
				"got:", res, "expected:", expected1[i])
			return
		}

		res = ByteSizeString(test, true)
		if res != expected2[i] {
			t.Error("Unexpected value for isu value:", test,
				"got:", res, "expected:", expected2[i])
			return
		}
	}
}

func TestHexDump(t *testing.T) {
	testdata := []byte("This is a test text. This is a test text.")

	res := HexDump(testdata)
	if res != "====\n"+
		"000000  54 68 69 73 20 69 73 20 61 20  This is a \n"+
		"00000a  74 65 73 74 20 74 65 78 74 2E  test text.\n"+
		"000014  20 54 68 69 73 20 69 73 20 61   This is a\n"+
		"00001e  20 74 65 73 74 20 74 65 78 74   test text\n"+
		"000028  2E                             .\n"+
		"====\n" {

		t.Error("Invalid boundaries should cause an error")
	}
}
