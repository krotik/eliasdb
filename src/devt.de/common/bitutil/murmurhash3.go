/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package bitutil

import "fmt"

const (
	c1 uint32 = 0xcc9e2d51
	c2 uint32 = 0x1b873593
)

/*
MurMurHashData hashes a given array of bytes. This is an implementation
of Austin Appleby's MurmurHash3 (32bit) function.

Reference implementation: http://code.google.com/p/smhasher/wiki/MurmurHash3
*/
func MurMurHashData(data []byte, offset int, size int, seed int) (uint32, error) {

	// Check parameters

	if offset < 0 || size < 0 {
		return 0, fmt.Errorf("Invalid data boundaries; offset: %v; size: %v",
			offset, size)
	}

	h1 := uint32(seed)
	end := offset + size
	end -= end % 4

	// Check length of available data

	if len(data) <= end {
		return 0, fmt.Errorf("Data out of bounds; set boundary: %v; data length: %v",
			end, len(data))
	}

	for i := offset; i < end; i += 4 {

		var k1 = uint32(data[i])
		k1 |= uint32(data[i+1]) << 8
		k1 |= uint32(data[i+2]) << 16
		k1 |= uint32(data[i+3]) << 24

		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17) // ROTL32(k1,15);
		k1 *= c2

		h1 ^= k1
		h1 = (h1 << 13) | (h1 >> 19) // ROTL32(h1,13);
		h1 = h1*5 + 0xe6546b64
	}

	// Tail

	var k1 uint32

	switch size & 3 {
	case 3:
		k1 = uint32(data[end+2]) << 16
		fallthrough
	case 2:
		k1 |= uint32(data[end+1]) << 8
		fallthrough
	case 1:
		k1 |= uint32(data[end])
		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17) // ROTL32(k1,15);
		k1 *= c2
		h1 ^= k1
	}

	h1 ^= uint32(size)

	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16

	return h1, nil
}
