/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package pools

import (
	"bytes"
	"testing"
)

func TestByteBufferPool(t *testing.T) {

	pool := NewByteBufferPool()

	buf1 := pool.Get().(*bytes.Buffer)
	buf2 := pool.Get()
	buf3 := pool.Get()

	if buf1 == nil || buf2 == nil || buf3 == nil {
		t.Error("Initialisation didn't work")
		return
	}

	buf1.Write(make([]byte, 10, 10))

	buf1.Reset()

	pool.Put(buf1)
}

func TestByteSlicePool(t *testing.T) {

	pool := NewByteSlicePool(5)

	buf1 := pool.Get().([]byte)
	buf2 := pool.Get()
	buf3 := pool.Get()

	if buf1 == nil || buf2 == nil || buf3 == nil {
		t.Error("Initialisation didn't work")
		return
	}

	if s := len(buf1); s != 5 {
		t.Error("Unexpected size:", s)
		return
	}

	pool.Put(buf1)
}
