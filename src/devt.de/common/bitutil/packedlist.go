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
	"bytes"
	"encoding/binary"
	"math"
)

/*
Different types of list packing
*/
const (
	packListType2Bit = 0x1
	packListType3Bit = 0x2
	packListType6Bit = 0x3
	packListTypeVar  = 0x0
)

/*
PackList packs a given list to a string. Depending on the given highest number the
list is packed in the most efficient way.
*/
func PackList(unpackedlist []uint64, highest uint64) string {

	// Depending on the highest number convert to given list

	switch {
	case highest <= 3:
		list := make([]byte, len(unpackedlist))
		for i, num := range unpackedlist {
			list[i] = byte(num)
		}
		return PackList2Bit(list)

	case highest <= 7:
		list := make([]byte, len(unpackedlist))
		for i, num := range unpackedlist {
			list[i] = byte(num)
		}
		return PackList3Bit(list)

	case highest <= 63:
		list := make([]byte, len(unpackedlist))
		for i, num := range unpackedlist {
			list[i] = byte(num)
		}
		return PackList6Bit(list)

	case highest <= math.MaxUint8:
		list := make([]byte, len(unpackedlist))
		for i, num := range unpackedlist {
			list[i] = byte(num)
		}
		return PackList8Bit(list)

	case highest <= math.MaxUint16:
		list := make([]uint16, len(unpackedlist))
		for i, num := range unpackedlist {
			list[i] = uint16(num)
		}
		return PackList16Bit(list)

	case highest <= math.MaxUint32:
		list := make([]uint32, len(unpackedlist))
		for i, num := range unpackedlist {
			list[i] = uint32(num)
		}
		return PackList32Bit(list)
	}

	return PackList64Bit(unpackedlist)
}

/*
UnpackList unpacks a list from a packed string.
*/
func UnpackList(packedlist string) []uint64 {
	plist := []byte(packedlist)

	if len(plist) == 0 {
		return nil
	}

	if plist[0]&0xC0 == packListTypeVar {
		return UnpackBigList(packedlist)
	}

	res := UnpackSmallList(packedlist)
	ret := make([]uint64, len(res))

	for i, item := range res {
		ret[i] = uint64(item)
	}

	return ret
}

/*
PackList8Bit packs a list of 8 bit numbers.
*/
func PackList8Bit(list []uint8) string {
	var bb bytes.Buffer

	bb.WriteByte(0x00)

	for i := 0; i < len(list); i++ {
		binary.Write(&bb, binary.LittleEndian, list[i])
	}

	return bb.String()
}

/*
PackList16Bit packs a list of 16 bit numbers.
*/
func PackList16Bit(list []uint16) string {
	var bb bytes.Buffer

	bb.WriteByte(0x01)

	for i := 0; i < len(list); i++ {
		binary.Write(&bb, binary.LittleEndian, list[i])
	}

	return bb.String()
}

/*
PackList32Bit packs a list of 32 bit numbers.
*/
func PackList32Bit(list []uint32) string {
	var bb bytes.Buffer

	bb.WriteByte(0x02)

	for i := 0; i < len(list); i++ {
		binary.Write(&bb, binary.LittleEndian, list[i])
	}

	return bb.String()
}

/*
PackList64Bit packs a list of 64 bit numbers.
*/
func PackList64Bit(list []uint64) string {
	var bb bytes.Buffer

	bb.WriteByte(0x03)

	for i := 0; i < len(list); i++ {
		binary.Write(&bb, binary.LittleEndian, list[i])
	}

	return bb.String()
}

/*
UnpackBigList unpacks a list which has large values.
*/
func UnpackBigList(packedlist string) []uint64 {
	var ret []uint64
	plist := []byte(packedlist)

	numlist := plist[1:]
	reader := bytes.NewReader(numlist)

	if plist[0] == 0x00 {
		var item uint8
		size := len(numlist)
		ret = make([]uint64, size)
		for i := 0; i < size; i++ {
			binary.Read(reader, binary.LittleEndian, &item)
			ret[i] = uint64(item)
		}
	} else if plist[0] == 0x01 {
		var item uint16
		size := len(numlist) / 2
		ret = make([]uint64, size)
		for i := 0; i < size; i++ {
			binary.Read(reader, binary.LittleEndian, &item)
			ret[i] = uint64(item)
		}
	} else if plist[0] == 0x02 {
		var item uint32
		size := len(numlist) / 4
		ret = make([]uint64, size)
		for i := 0; i < size; i++ {
			binary.Read(reader, binary.LittleEndian, &item)
			ret[i] = uint64(item)
		}
	} else if plist[0] == 0x03 {
		size := len(numlist) / 8
		ret = make([]uint64, size)
		binary.Read(reader, binary.LittleEndian, ret)
	}

	return ret
}

/*
PackList2Bit packs a list of bytes into a string using 2 bits for each item.
(Items must be between 1 and 3)
*/
func PackList2Bit(list []byte) string {
	if len(list) == 0 {
		return ""
	}

	// Packing the list with 2 bit items reduces the size by a factor of 4

	ret := make([]byte, int(math.Ceil(float64(1)/3+float64(len(list)-1)/4)))

	if len(list) == 1 {
		ret[0] = list2byte2bit(packListType2Bit, list[0], 0, 0)
	} else if len(list) == 2 {
		ret[0] = list2byte2bit(packListType2Bit, list[0], list[1], 0)
	} else {
		ret[0] = list2byte2bit(packListType2Bit, list[0], list[1], list[2])

		j := 1
		for i := 3; i < len(list); i += 4 {
			if len(list[i:]) == 1 {
				ret[j] = list2byte2bit(list[i], 0, 0, 0)
			} else if len(list[i:]) == 2 {
				ret[j] = list2byte2bit(list[i], list[i+1], 0, 0)
			} else if len(list[i:]) == 3 {
				ret[j] = list2byte2bit(list[i], list[i+1], list[i+2], 0)
			} else {
				ret[j] = list2byte2bit(list[i], list[i+1], list[i+2], list[i+3])
			}
			j++
		}
	}

	return string(ret)
}

/*
PackList3Bit packs a list of bytes into a string using 3 bits for each item.
(Items must be between 1 and 7)
*/
func PackList3Bit(list []byte) string {
	if len(list) == 0 {
		return ""
	}

	// Packing the list with 2 bit items reduces the size by a factor of 2

	ret := make([]byte, int(math.Ceil(float64(len(list))/2)))

	if len(list) == 1 {
		ret[0] = list2byte3bitAndHeader(packListType3Bit, list[0], 0)
	} else {
		ret[0] = list2byte3bitAndHeader(packListType3Bit, list[0], list[1])

		j := 1
		for i := 2; i < len(list); i += 2 {
			if len(list[i:]) == 1 {
				ret[j] = list2byte3bitAndHeader(0, list[i], 0)
			} else {
				ret[j] = list2byte3bitAndHeader(0, list[i], list[i+1])
			}
			j++
		}
	}

	return string(ret)
}

/*
PackList6Bit packs a list of bytes into a string using 6 bits for each item.
(Items must be between 1 and 63)
*/
func PackList6Bit(list []byte) string {
	if len(list) == 0 {
		return ""
	}

	// Packing the list with 6 bit items does not reduce the factor

	ret := make([]byte, len(list))

	if len(list) == 1 {
		ret[0] = list2byte6bitAndHeader(packListType6Bit, list[0])
	} else {
		ret[0] = list2byte6bitAndHeader(packListType6Bit, list[0])

		for i := 1; i < len(list); i++ {
			ret[i] = list2byte6bitAndHeader(0, list[i])
		}
	}

	return string(ret)
}

/*
UnpackSmallList unpacks a string into a list of bytes. Returns the list of bytes
or a list of a single 0x00 byte if the numbers in the list are too big.
*/
func UnpackSmallList(packedlist string) []byte {
	plist := []byte(packedlist)

	if len(plist) == 0 {
		return []byte{}
	}

	ltype := plist[0] & 0xC0 >> 6

	if ltype == packListType2Bit {
		return unpacklist2bit(plist)
	} else if ltype == packListType3Bit {
		return unpacklist3bit(plist)
	} else if ltype == packListType6Bit {
		return unpacklist6bit(plist)
	}

	// Must be gob encoded

	return []byte{00}
}

func unpacklist2bit(packedlist []byte) []byte {
	ret := make([]byte, 0, len(packedlist)*3)

	for i := 0; i < len(packedlist); i++ {
		b1, b2, b3, b4 := byte2list2bit(packedlist[i])
		if i > 0 && b1 != 0 {
			ret = append(ret, b1)
		}
		if b2 != 0 {
			ret = append(ret, b2)
		}
		if b3 != 0 {
			ret = append(ret, b3)
		}
		if b4 != 0 {
			ret = append(ret, b4)
		}
	}

	return ret
}

func unpacklist3bit(packedlist []byte) []byte {
	ret := make([]byte, 0, len(packedlist)*2)

	for i := 0; i < len(packedlist); i++ {
		b1, b2 := byte2list3bit(packedlist[i])
		if b1 != 0 {
			ret = append(ret, b1)
		}
		if b2 != 0 {
			ret = append(ret, b2)
		}
	}

	return ret
}

func unpacklist6bit(packedlist []byte) []byte {
	ret := make([]byte, 0, len(packedlist))

	for i := 0; i < len(packedlist); i++ {
		ret = append(ret, byte2list6bit(packedlist[i]))
	}

	return ret
}

func byte2list2bit(b byte) (b1 byte, b2 byte, b3 byte, b4 byte) {
	b1 = b & 0xC0 >> 6
	b2 = b & 0x30 >> 4
	b3 = b & 0x0C >> 2
	b4 = b & 0x03

	return b1, b2, b3, b4
}

func list2byte2bit(b1 byte, b2 byte, b3 byte, b4 byte) byte {
	return (b1 & 0x03 << 6) |
		(b2 & 0x03 << 4) |
		(b3 & 0x03 << 2) |
		(b4 & 0x03)
}

func list2byte3bitAndHeader(b1 byte, b2 byte, b3 byte) byte {
	return (b1 & 0x03 << 6) |
		(b2 & 0x07 << 3) |
		(b3 & 0x07)
}

func byte2list3bit(b byte) (b2 byte, b3 byte) {
	b2 = b & 0x38 >> 3
	b3 = b & 0x07

	return b2, b3
}

func list2byte6bitAndHeader(b1 byte, b2 byte) byte {
	return (b1 & 0x03 << 6) |
		(b2 & 0x3F)
}

func byte2list6bit(b byte) byte {
	return b & 0x3F
}
