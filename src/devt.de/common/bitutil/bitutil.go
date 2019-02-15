/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package bitutil contains common function for bit-level operations.

Pack and Unpack functions are used to pack and unpack a list of non-zero numbers
very efficiently.
*/
package bitutil

import (
	"bytes"
	"fmt"
	"math"
)

/*
CompareByteArray compares the contents of two byte array slices. Returns true
if both slices are equivalent in terms of size and content. The capacity may
be different.
*/
func CompareByteArray(arr1 []byte, arr2 []byte) bool {
	if len(arr1) != len(arr2) {
		return false
	}
	for i, v := range arr1 {
		if v != arr2[i] {
			return false
		}
	}
	return true
}

/*
ByteSizeString takes a numeric byte size and returns it in human readable form.
The useISU parameter determines which units to use. False uses the more common
binary form. The units kibibyte, mebibyte, etc were established by the
International Electrotechnical Commission (IEC) in 1998.

useISU = True -> Decimal (as formally defined in the International System of Units)
Bytes / Metric
1000^1 kB kilobyte
1000^2 MB megabyte
1000^3 GB gigabyte
1000^4 TB terabyte
1000^5 PB petabyte
1000^6 EB exabyte

useISU = False -> Binary (as defined by the International Electrotechnical Commission)
Bytes / Metric
1024^1 KiB kibibyte
1024^2 MiB mebibyte
1024^3 GiB gibibyte
1024^4 TiB tebibyte
1024^5 PiB pebibyte
1024^6 EiB exbibyte
*/
func ByteSizeString(size int64, useISU bool) string {
	var byteSize, unit float64 = float64(size), 1024
	var pre string

	if useISU {
		unit = 1000
	}

	if byteSize < unit {
		return fmt.Sprintf("%d B", int(byteSize))
	}

	exp := math.Floor(math.Log(byteSize) / math.Log(unit))

	if useISU {
		pre = string("kMGTPE"[int(exp-1)])
	} else {
		pre = fmt.Sprintf("%vi", string("KMGTPE"[int(exp-1)]))
	}

	res := byteSize / math.Pow(unit, exp)

	return fmt.Sprintf("%.1f %sB", res, pre)
}

/*
HexDump produces a more-or-less human readable hex dump from a given byte array
slice.
*/
func HexDump(data []byte) string {
	buf := new(bytes.Buffer)
	line := new(bytes.Buffer)

	buf.WriteString("====\n000000  ")

	for i, b := range data {

		if i != 0 && i%10 == 0 {
			buf.WriteString(fmt.Sprintf(" %s\n%06x  ", line.String(), i))
			line = new(bytes.Buffer)
		}

		buf.WriteString(fmt.Sprintf("%02X ", b))
		line.WriteString(fmt.Sprintf("%c", b))
	}

	rest := len(data) % 10
	if rest != 0 {
		for i := rest; i < 10; i++ {
			buf.WriteString("   ")
		}
	}

	buf.WriteString(fmt.Sprintf(" %s\n====\n", line.String()))

	return buf.String()
}
