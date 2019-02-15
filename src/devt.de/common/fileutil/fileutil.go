/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package fileutil

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"os"

	"devt.de/common/bitutil"
	"devt.de/common/pools"
)

/*
PathExists returns whether the given file or directory exists.
*/
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

/*
IsDir returns whether the given path is a directory.
*/
func IsDir(path string) (bool, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return false, err
	}

	return stat.IsDir(), nil
}

/*
CheckSumFile calculates a sha256 checksum of a given file. This function
will read in the whole file.
*/
func CheckSumFile(path string) (string, error) {
	var checksum = ""

	f, err := os.Open(path)

	if err == nil {
		defer f.Close()

		hashFactory := sha256.New()

		if _, err = io.Copy(hashFactory, f); err == nil {
			checksum = fmt.Sprintf("%x", hashFactory.Sum(nil))
		}
	}

	return checksum, err
}

/*
fastSumSampleSize is the sample size for fast checksum
*/
const fastSumSampleSize = 16 * 1024

/*
bufferPool holds buffers which are used for fast checksums.
*/
var fastChecksumBigBufferPool = pools.NewByteBufferPool()
var fastChecksumSmallBufferPool = pools.NewByteSlicePool(fastSumSampleSize * 3)

/*
CheckSumFileFast calculates a 32bit MurmurHash3 checksum from a portion
of the given file.
*/
func CheckSumFileFast(path string) (string, error) {
	var fi os.FileInfo
	var checksum = ""

	f, err := os.Open(path)

	if err == nil {
		defer f.Close()

		if fi, err = f.Stat(); err == nil {
			var res uint32

			if fi.Size() < int64(fastSumSampleSize*8) {
				buf := fastChecksumBigBufferPool.Get().(*bytes.Buffer)

				// Read in the whole file

				if _, err = io.Copy(buf, f); err == nil {

					if res, err = bitutil.MurMurHashData(buf.Bytes(), 0, buf.Len(), 42); err == nil {
						checksum = fmt.Sprintf("%x", res)
					}
				}

				buf.Reset()
				fastChecksumBigBufferPool.Put(buf)

			} else {

				sr := io.NewSectionReader(f, 0, fi.Size())
				buf := fastChecksumSmallBufferPool.Get().([]byte)

				sr.Read(buf[:fastSumSampleSize])
				sr.Seek(sr.Size()/2, 0)
				sr.Read(buf[fastSumSampleSize : fastSumSampleSize*2])
				sr.Seek(int64(-fastSumSampleSize), 2)
				sr.Read(buf[fastSumSampleSize*2:])

				if res, err = bitutil.MurMurHashData(buf, 0, len(buf)-1, 42); err == nil {
					checksum = fmt.Sprintf("%x", res)
				}

				fastChecksumSmallBufferPool.Put(buf)
			}
		}
	}

	return checksum, err
}
