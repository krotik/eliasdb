/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain. 
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Common datastructures and functions for testing.
*/
package testutil

import (
	"errors"
	"fmt"
)

/*
Testing object for gob errors.
*/
type GobTestObject struct {
	Name   string
	EncErr bool
	DecErr bool
}

func (t *GobTestObject) GobEncode() ([]byte, error) {
	if t.EncErr {
		return nil, errors.New("Encode error")
	}
	return []byte(t.Name), nil
}

func (t *GobTestObject) GobDecode(b []byte) error {
	if t.DecErr {
		return errors.New("Decode error")
	}
	t.Name = string(b)
	return nil
}

/*
Testing buffer to test error handling for writing operations.
*/
type ErrorTestingBuffer struct {
	RemainingSize int
	WrittenSize   int
}

/*
Simulates writing to the test buffer. Returns error if it is full.
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
For simplicity the buffer itself implements the error interface.
*/
func (etb ErrorTestingBuffer) Error() string {
	return fmt.Sprintf("Buffer is full at: %v", etb.WrittenSize+etb.RemainingSize)
}

/*
Use testing buffer as an io.File like object.
*/
type ErrorTestingFile struct {
	Buf *ErrorTestingBuffer
}

/*
Create a new test file.
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
