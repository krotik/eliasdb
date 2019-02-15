/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package pools contains object pooling utilities.
*/
package pools

import (
	"bytes"
	"sync"
)

/*
NewByteBufferPool creates a new pool of bytes.Buffer objects. The pool creates
new ones if it runs empty.
*/
func NewByteBufferPool() *sync.Pool {
	return &sync.Pool{New: func() interface{} { return &bytes.Buffer{} }}
}

/*
NewByteSlicePool creates a new pool of []byte objects of a certain size. The
pool creates new ones if it runs empty.
*/
func NewByteSlicePool(size int) *sync.Pool {
	return &sync.Pool{New: func() interface{} { return make([]byte, size) }}
}
