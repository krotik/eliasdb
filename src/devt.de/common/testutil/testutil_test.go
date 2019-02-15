/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package testutil

import (
	"bytes"
	"encoding/gob"
	"strings"
	"testing"
	"time"
)

func TestGetCaller(t *testing.T) {
	var name, loc string

	foo := func() {

		// Foo asks who called me?

		name, loc = GetCaller(0)
	}

	foo()

	// Answer should be TestGetCaller in file testutil_test.go line 30

	if !strings.Contains(name, "devt.de/common/testutil.TestGetCaller") {
		t.Error("Unexpected result:", name)
	}

	if !strings.Contains(loc, "testutil_test.go:30") {
		t.Error("Unexpected result:", loc)
	}
}

func TestErrorTestingConnection(t *testing.T) {
	c := &ErrorTestingConnection{}

	// Check methods which do nothing

	c.Close()
	c.LocalAddr()
	c.RemoteAddr()
	c.SetDeadline(time.Now())
	c.SetReadDeadline(time.Now())
	c.SetWriteDeadline(time.Now())

	c.In.WriteString("This is a test")
	c.InErr = 4

	tb := make([]byte, 4, 4)

	// First read of 4 bytes should be fine

	n, err := c.Read(tb)
	if err != nil || n != 4 {
		t.Error(n, err)
		return
	}

	// Then we should get an error

	n, err = c.Read(tb)
	if err.Error() != "Test reading error" || n != 0 {
		t.Error(n, err)
		return
	}

	c.OutErr = 4

	// First write of 4 bytes should be fine

	n, err = c.Write([]byte("test"))
	if err != nil || n != 4 {
		t.Error(n, err)
		return
	}

	// Then we should get an error

	n, err = c.Write([]byte("test"))
	if err.Error() != "Test writing error" || n != 0 {
		t.Error(n, err)
		return
	}

	c.OutErr = 0
	c.OutClose = true

	n, err = c.Write([]byte("test"))
	if err != nil || n != 0 {
		t.Error(n, err)
		return
	}
}

func TestErrorTestingBuffer(t *testing.T) {

	buf := &ErrorTestingBuffer{5, 0}

	n, err := buf.Write(make([]byte, 5))
	if n != 5 {
		t.Error("Unexpected number of bytes written:", n)
		return
	}
	if err != nil {
		t.Error(err)
		return
	}

	n, err = buf.Write(make([]byte, 6))
	if n != 0 {
		t.Error("Unexpected number of bytes written:", n)
		return
	}
	if err == nil {
		t.Error("Writing too much too the buffer didn't return an error")
		return
	}

	etb, ok := err.(ErrorTestingBuffer)
	if !ok {
		t.Error("Unexpected error type was returned")
		return
	}
	if etb.WrittenSize != 5 || etb.RemainingSize != 0 {
		t.Error("Unexpected error state")
		return
	}
	if etb.Error() != "Buffer is full at: 5" {
		t.Error("Unexpected error message:", err)
		return
	}
}

func TestErrorTestingFile(t *testing.T) {
	f := NewTestingFile(5)

	n, err := f.Write(make([]byte, 5))
	if n != 5 {
		t.Error("Unexpected number of bytes written:", n)
		return
	}
	if err != nil {
		t.Error(err)
		return
	}

	n, err = f.Write(make([]byte, 6))
	if n != 0 {
		t.Error("Unexpected number of bytes written:", n)
		return
	}
	if err == nil {
		t.Error("Writing too much too the buffer didn't return an error")
		return
	}

	// Methods do nothing
	f.Sync()
	f.Close()
}

func TestGobTestObject(t *testing.T) {

	bb := &bytes.Buffer{}

	var ret GobTestObject

	gobtest := &GobTestObject{"test", true, false}

	if err := gob.NewEncoder(bb).Encode(gobtest); err == nil || err.Error() != "Encode error" {
		t.Error("Unexpected result:", err)
		return
	}

	ret = GobTestObject{"test", false, true}

	bb = &bytes.Buffer{}

	if err := gob.NewEncoder(bb).Encode(&GobTestObject{"test", false, false}); err != nil {
		t.Error(err)
		return
	}

	if err := gob.NewDecoder(bb).Decode(&ret); err == nil || err.Error() != "Decode error" {
		t.Error("Unexpected result:", err)
		return
	}

	bb = &bytes.Buffer{}
	ret = GobTestObject{"", false, false}

	if err := gob.NewEncoder(bb).Encode(&GobTestObject{"test", false, false}); err != nil {
		t.Error(err)
		return
	}

	if err := gob.NewDecoder(bb).Decode(&ret); err != nil {
		t.Error(err)
		return
	}

	if ret.Name != "test" {
		t.Error("Unexpected decoding result:", ret)
		return
	}
}
