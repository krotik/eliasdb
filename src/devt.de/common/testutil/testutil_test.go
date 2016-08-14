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
	"testing"
)

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
