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
	"bytes"
	"errors"
	"fmt"
	"net"
	"runtime"
	"time"
)

/*
GetCaller returns the calling function of the function which called it and the
code location which called the calling function. Can optionally go further back
depending on the level parameter.

For example testfile.go:

20: foo() {
21:	  x := GetCaller(0)
22: }
23:
24: bar() {
25:	  foo()
26: }

In the example x would be set to "bar" and location would be testfile.go 25.
*/
func GetCaller(level int) (string, string) {
	name := "n/a"
	loc := "n/a"

	// Get callers as uintptr

	fpcs := make([]uintptr, 1)

	// Skip 3 levels to get the caller's caller

	if n := runtime.Callers(3+level, fpcs); n != 0 {

		// Get the function info

		if fc := runtime.FuncForPC(fpcs[0] - 1); fc != nil {

			name = fc.Name()
			file, line := fc.FileLine(fpcs[0] - 1)
			loc = fmt.Sprintf("%v:%v", file, line)
		}
	}

	return name, loc
}

/*
ErrorTestingConnection testing object for connection errors.
*/
type ErrorTestingConnection struct {
	In       bytes.Buffer
	Out      bytes.Buffer
	rc       int
	InErr    int
	wc       int
	OutErr   int
	OutClose bool
}

/*
Read reads from the test connection in buffer.
*/
func (c *ErrorTestingConnection) Read(b []byte) (n int, err error) {
	c.rc += len(b)
	if c.InErr != 0 && c.rc > c.InErr {
		return 0, errors.New("Test reading error")
	}
	return c.In.Read(b)
}

/*
Write write to the test connections buffer.
*/
func (c *ErrorTestingConnection) Write(b []byte) (n int, err error) {
	c.wc += len(b)
	if c.OutErr != 0 && c.wc > c.OutErr {
		return 0, errors.New("Test writing error")
	}
	if c.OutClose {
		return 0, nil
	}
	return c.Out.Write(b)
}

/*
Close is a method stub to satisfy the net.Conn interface.
*/
func (c *ErrorTestingConnection) Close() error {
	return nil
}

/*
LocalAddr is a method stub to satisfy the net.Conn interface.
*/
func (c *ErrorTestingConnection) LocalAddr() net.Addr {
	return nil
}

/*
RemoteAddr is a method stub to satisfy the net.Conn interface.
*/
func (c *ErrorTestingConnection) RemoteAddr() net.Addr {
	return nil
}

/*
SetDeadline is a method stub to satisfy the net.Conn interface.
*/
func (c *ErrorTestingConnection) SetDeadline(t time.Time) error {
	return nil
}

/*
SetReadDeadline is a method stub to satisfy the net.Conn interface.
*/
func (c *ErrorTestingConnection) SetReadDeadline(t time.Time) error {
	return nil
}

/*
SetWriteDeadline is a method stub to satisfy the net.Conn interface.
*/
func (c *ErrorTestingConnection) SetWriteDeadline(t time.Time) error {
	return nil
}

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
