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

var testData = []byte("Now is the time for all good men to come to the aid of their country")

var resultArray1 = []uint32{
	0x249cb285, 0xcae32c45, 0x49cc6fdd, 0x3c89b814, 0xdc9778bb, 0x6db6607a,
	0x736df8ad, 0xd367e257, 0x59b32232, 0x2496a9b4, 0x01d69f33, 0x08454378,
	0x4ad4f630, 0x0ae1ca05, 0x042bdb5b, 0xbf3592e8, 0x0ed8b048, 0xb86958db,
	0xa74ca5b6, 0xb7982271, 0x10a77c40, 0x8caba8ef, 0xe5085ab6, 0x8ee964b8,
	0x170f0222, 0x42dec76d, 0xc4ebe4e5, 0x3d246566, 0x64f1133e, 0x8a0597dd,
	0x5b13cdb8, 0x1c723636, 0xc8b60a2f, 0xb572fe46, 0xb801f177, 0x71d44c64,
	0x755aeff1, 0x66ba2eeb, 0x5cfec249, 0x5b9d603f, 0x4e916049, 0x07622306,
	0x57d4271f, 0x3fa8e56a, 0x4b4fe703, 0x995e958d, 0xdaf48fbb, 0xbe381e68,
	0xd4af5452, 0x6b8e4cdc, 0x3c7bbc57, 0xd834a3e0, 0x78665c77, 0x5ab0d747,
	0x4b34afb7, 0xbce90104, 0x25a31264, 0xa348c314, 0xab9fb213, 0x48f40ea9,
	0xa232f18e, 0xda12f11a, 0x7dcdfcfb, 0x24381ba8, 0x1a15737d, 0x32b1ea01,
	0x7ed7f6c6, 0xd16ab3ed}

func TestMurMurHashData(t *testing.T) {

	data := []byte{0xf6, 0x02, 0x03, 0x04}

	// Test invalid data boundaries

	_, err := MurMurHashData(data, 1, -3, 6)

	if err == nil {
		t.Error("Invalid boundaries should cause an error")
	} else if err.Error() != "Invalid data boundaries; offset: 1; size: -3" {
		t.Errorf("Unexpected error: %v", err)
	}

	_, err = MurMurHashData(data, 1, 5, 6)

	if err == nil {
		t.Error("Invalid boundaries should cause an error")
	} else if err.Error() != "Data out of bounds; set boundary: 4; data length: 4" {
		t.Errorf("Unexpected error: %v", err)
	}

	// Test against data

	// Go source code is always UTF-8, so the string literal is UTF-8 text.
	data = []byte("Now is the time for all good men to come to the aid of their country")

	doTest := func(offset, size int) uint32 {
		res, err := MurMurHashData(data, offset, size, 4)

		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		return res
	}

	for i := 0; i < len(resultArray1); i++ {
		res := doTest(0, i)
		if res != resultArray1[i] {
			t.Errorf("Unexpected result; Expected: 0x%x; Got: 0x%x", resultArray1[i], res)
		}
	}
}
