/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Common functions for copying data.
*/
package datautil

import (
	"bytes"
	"encoding/gob"

	"devt.de/common/pools"
)

var BufferPool = pools.NewByteBufferPool()

/*
CopyObject copies contents of a given object reference to another given object reference.
*/
func CopyObject(src interface{}, dest interface{}) error {
	bb := BufferPool.Get().(*bytes.Buffer)

	err := gob.NewEncoder(bb).Encode(src)

	if err != nil {
		return err
	}

	err = gob.NewDecoder(bb).Decode(dest)

	if err != nil {
		return err
	}

	bb.Reset()
	BufferPool.Put(bb)

	return nil
}
