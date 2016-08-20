/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package testutil contains common datastructures and functions for testing.
*/
package testutil

import (
	"errors"
	"fmt"
)

/*
GobTestObject testing object for gob errors.
*/
type GobTestObject struct {
	Name   string
	EncErr bool
	DecErr bool
}

/*
GobEncode returns a test encoded byte array or an error.
*/
func (t *GobTestObject) GobEncode() ([]byte, error) {
	if t.EncErr {
		return nil, errors.New("Encode error")
	}
	return []byte(t.Name), nil
}

/*
GobDecode decodes the given byte array are returns an error.
*/
func (t *GobTestObject) GobDecode(b []byte) error {
	if t.DecErr {
		return errors.New("Decode error")
	}
	t.Name = string(b)
	return nil
}

/*
ErrorTestingBuffer is a testing buffer to test error handling for writing operations.
*/
type ErrorTestingBuffer struct {
	RemainingSize int
	WrittenSize   int
}

/*
Write simulates writing to the test buffer. Returns error if it is full.
*/
func (etb *ErrorTestingBuffer) Write(p []byte) (n int, err error) {
	if len(p) > etb.RemainingSize {
		return 0, *etb
	}
	written := len(p)
	etb.WrittenSize += written
	etb.RemainingSize -= written
	return written, nil
}

/*
Error returns buffer errors. For simplicity the buffer itself implements the error interface.
*/
func (etb ErrorTestingBuffer) Error() string {
	return fmt.Sprintf("Buffer is full at: %v", etb.WrittenSize+etb.RemainingSize)
}

/*
ErrorTestingFile is a testing buffer which can be used as an io.File like object.
*/
type ErrorTestingFile struct {
	Buf *ErrorTestingBuffer
}

/*
NewTestingFile creates a new test file.
*/
func NewTestingFile(size int) *ErrorTestingFile {
	return &ErrorTestingFile{&ErrorTestingBuffer{size, 0}}
}

/*
Write writes to the file.
*/
func (etf ErrorTestingFile) Write(p []byte) (n int, err error) {
	return etf.Buf.Write(p)
}

/*
Close does nothing (there to satisfy interfaces)
*/
func (etf ErrorTestingFile) Close() error {
	return nil
}

/*
Sync does nothing (there to satisfy interfaces)
*/
func (etf ErrorTestingFile) Sync() error {
	return nil
}
