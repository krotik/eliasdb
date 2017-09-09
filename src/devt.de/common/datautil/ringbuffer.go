/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package datautil contains general data handling objects and helper methods.
*/
package datautil

import (
	"fmt"
	"strings"
	"sync"
)

/*
RingBuffer is a classic thread-safe ringbuffer implementation. It stores
abstract interface{} objects. It has specific methods so it can be used as
a print logger.
*/
type RingBuffer struct {
	data     []interface{} // Elements of this ring buffer
	size     int           // Size of the ring buffer
	first    int           // First item of the ring buffer
	last     int           // Last item of the ring buffer
	modCount int           // Check for modifications during iterations
	lock     *sync.RWMutex // Lock for RingBuffer
}

/*
NewRingBuffer creates a new ringbuffer with a given size.
*/
func NewRingBuffer(size int) *RingBuffer {
	return &RingBuffer{make([]interface{}, size), 0, 0, 0, 0, &sync.RWMutex{}}
}

/*
Reset removes all content from the ringbuffer.
*/
func (rb *RingBuffer) Reset() {
	rb.lock.Lock()
	defer rb.lock.Unlock()

	rb.data = make([]interface{}, cap(rb.data))
	rb.size = 0
	rb.first = 0
	rb.last = 0
	rb.modCount = 0
}

/*
IsEmpty returns if this ringbuffer is empty.
*/
func (rb *RingBuffer) IsEmpty() bool {
	rb.lock.RLock()
	defer rb.lock.RUnlock()

	return rb.size == 0
}

/*
Size returns the size of the ringbuffer.
*/
func (rb *RingBuffer) Size() int {
	rb.lock.RLock()
	defer rb.lock.RUnlock()

	return rb.size
}

/*
Get returns an element of the ringbuffer from a given position.
*/
func (rb *RingBuffer) Get(p int) interface{} {
	rb.lock.RLock()
	defer rb.lock.RUnlock()

	return rb.data[(rb.first+p)%len(rb.data)]
}

/*
Add adds an item to the ringbuffer.
*/
func (rb *RingBuffer) Add(e interface{}) {
	rb.lock.Lock()
	defer rb.lock.Unlock()

	ld := len(rb.data)

	rb.data[rb.last] = e
	rb.last = (rb.last + 1) % ld

	if rb.size == ld {
		rb.first = (rb.first + 1) % ld
	} else {
		rb.size++
	}

	rb.modCount++
}

/*
Poll removes and returns the head of the ringbuffer.
*/
func (rb *RingBuffer) Poll() interface{} {
	rb.lock.Lock()
	defer rb.lock.Unlock()

	if rb.size == 0 {
		return nil
	}

	i := rb.data[rb.first]
	rb.data[rb.first] = nil

	rb.size--
	rb.first = (rb.first + 1) % len(rb.data)
	rb.modCount++

	return i
}

/*
Log writes the given arguments as strings into the ring buffer. Each line is a
separate item.
*/
func (rb *RingBuffer) Log(v ...interface{}) {
	lines := strings.Split(fmt.Sprint(v...), "\n")

	for _, line := range lines {
		rb.Add(line)
	}
}

/*
Slice returns the contents of the buffer as a slice.
*/
func (rb *RingBuffer) Slice() []interface{} {
	rb.lock.RLock()
	defer rb.lock.RUnlock()

	ld := len(rb.data)
	ret := make([]interface{}, rb.size)

	for i := 0; i < rb.size; i++ {
		ret[i] = rb.data[(i+rb.first)%ld]
	}

	return ret
}

/*
StringSlice returns the contents of the buffer as a slice of strings.
Each item of the buffer is a separate string.
*/
func (rb *RingBuffer) StringSlice() []string {
	rb.lock.RLock()
	defer rb.lock.RUnlock()

	ld := len(rb.data)
	ret := make([]string, rb.size)

	for i := 0; i < rb.size; i++ {
		ret[i] = fmt.Sprint(rb.data[(i+rb.first)%ld])
	}

	return ret
}

/*
String retusn the contents of the buffer as a string. Each item of the buffer is
treated as a separate line.
*/
func (rb *RingBuffer) String() string {
	return strings.Join(rb.StringSlice(), "\n")
}
