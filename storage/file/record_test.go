/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package file

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"

	"devt.de/krotik/common/bitutil"
	"devt.de/krotik/common/testutil"
)

func TestRecordInitialisation(t *testing.T) {
	r := new(Record)
	out := r.String()

	if out != "Record: 0 (dirty:false transCount:0 len:0 cap:0)\n"+
		"====\n"+
		"000000   \n"+
		"====\n" {
		t.Error("Unexpected output of empty record:", out)
	}

	rdata := []byte("This is a test")
	r = NewRecord(123, rdata)

	id := r.ID()
	if id != 123 {
		t.Error("Unexpected id:", id)
	}

	data := r.Data()
	if !bitutil.CompareByteArray(data, rdata) {
		t.Error("Unexpected initial data", data)
	}

	if r.Dirty() {
		t.Error("Record shouldn't be dirty right after it was created.")
	}

	// Test page view object storage

	dummyString := "TEST"

	r.SetPageView(dummyString)

	if r.PageView() != dummyString {
		t.Error("Unexpected page view object")
	}
}

func TestTransactionCounter(t *testing.T) {
	r := NewRecord(123, make([]byte, 20))

	if r.InTransaction() {
		t.Error("A fresh record should not be in a transaction.")
	}

	r.IncTransCount()

	if !r.InTransaction() {
		t.Error("Record should be in transaction after the transaction count was increased.")
	}

	if r.SetID(567) == nil {
		t.Error("It should not be possible to change the record id while in a transaction.")
	}

	r.DecTransCount()

	if r.SetID(789); r.ID() != 789 {
		t.Error("It should be possible to change the record id outside of a transaction.")
	}

	if r.InTransaction() {
		t.Error("Record should not be in transaction after the transaction count was decreased again.")
	}

	testTransactionCountPanic(t, r)
}

func testTransactionCountPanic(t *testing.T, r *Record) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Decreasing of transaction count did not cause a panic.")
		}
	}()

	r.DecTransCount()
}

func TestReadAndWrite(t *testing.T) {
	r := NewRecord(123, make([]byte, 20))

	r.WriteSingleByte(3, 0x42)

	if r.data[3] != 0x42 {
		t.Error("Unexpected value in read/write test", r.data[3], "expected: 0x42")
	}

	if !r.Dirty() {
		t.Error("Record should be marked as dirty after write operation.")
	}

	r.ClearDirty()

	if r.Dirty() {
		t.Error("Record should not be marked as dirty after clearing flag.")
	}

	r.WriteSingleByte(0, 0xff)
	showRWTestResult(t, r.ReadSingleByte(0) == byte(0xff), "a byte")
	r.WriteSingleByte(0, 0x01)
	showRWTestResult(t, r.ReadSingleByte(0) == byte(0x01), "a byte")

	r.WriteUInt16(0, 0x1234)
	showRWTestResult(t, r.ReadUInt16(0) == uint16(0x1234), "an uint16")
	r.WriteUInt16(0, 0xFFFF)
	showRWTestResult(t, r.ReadUInt16(0) == uint16(0xFFFF), "an uint16")

	r.WriteInt16(0, -0x1234)
	showRWTestResult(t, r.ReadInt16(0) == int16(-0x1234), "an int16")
	r.WriteInt16(0, -0x7FFF)
	showRWTestResult(t, r.ReadInt16(0) == int16(-0x7FFF), "an int16")
	r.WriteInt16(0, 0x7FFF)
	showRWTestResult(t, r.ReadInt16(0) == int16(0x7FFF), "an int16")

	r.WriteUInt32(0, 0x12345678)
	showRWTestResult(t, r.ReadUInt32(0) == uint32(0x12345678), "an uint32")
	r.WriteUInt32(0, 0xFFFFFFFF)
	showRWTestResult(t, r.ReadUInt32(0) == uint32(0xFFFFFFFF), "an uint32")

	r.WriteInt32(0, -0x12345678)
	showRWTestResult(t, r.ReadInt32(0) == int32(-0x12345678), "an int32")
	r.WriteInt32(0, -0x7FFFFFFF)
	showRWTestResult(t, r.ReadInt32(0) == int32(-0x7FFFFFFF), "an int32")
	r.WriteInt32(0, 0x7FFFFFFF)
	showRWTestResult(t, r.ReadInt32(0) == int32(0x7FFFFFFF), "an int32")

	r.WriteUInt64(0, 0x1234567891234567)
	showRWTestResult(t, r.ReadUInt64(0) == uint64(0x1234567891234567), "an uint64")
	r.WriteUInt64(0, 0xFFFFFFFFFFFFFFFF)
	showRWTestResult(t, r.ReadUInt64(0) == uint64(0xFFFFFFFFFFFFFFFF), "an uint64")

	r.ClearData()

	if r.ReadUInt64(0) != 0 || r.Dirty() {
		t.Error("Record should be clean and not marked as dirty after it was cleaned.")
	}
}

func showRWTestResult(t *testing.T, res bool, operation string) {
	if !res {
		t.Error("Unexpected result while reading/writing", operation)
	}
}

func TestMarshalBinary(t *testing.T) {
	r := NewRecord(123, make([]byte, 20))

	r.WriteSingleByte(0, 0x41)
	r.WriteSingleByte(3, 0x42)
	r.WriteSingleByte(19, 0x43)

	r.transCount = 19

	data, _ := r.MarshalBinary()

	r2 := NewRecord(0, make([]byte, 20))

	err := r2.UnmarshalBinary(data)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(r, r2) {
		t.Error("Unmarshaled record should be the same as the original record")
	}

	ior := new(bytes.Buffer)
	ior.Write(data)
	r3, err := ReadRecord(ior)
	if err != nil {
		t.Error(err)
		return
	}

	if !reflect.DeepEqual(r, r3) {
		t.Error("Unmarshaled record should be the same as the original record")
	}

	ior = new(bytes.Buffer)
	ior.Write(data[1:5])
	_, err = ReadRecord(ior)
	if err == nil {
		t.Error("ReadRecord should return an error when given invalid data")
		return
	}

	data2, _ := r2.MarshalBinary()
	if !reflect.DeepEqual(data, data2) {
		t.Error("Marshaled representation of records should be the same")
	}

	// Test errors of writing
	for i := 0; i < len(data); i++ {
		buf := &testutil.ErrorTestingBuffer{RemainingSize: i, WrittenSize: 0}
		err := r.WriteRecord(buf)
		if _, ok := err.(testutil.ErrorTestingBuffer); !ok {
			t.Error("Unexpected error return:", err)
		}
	}

	r.ClearDirty()
	data, _ = r.MarshalBinary()
	buf := &testutil.ErrorTestingBuffer{RemainingSize: 8, WrittenSize: 0}
	err = r.WriteRecord(buf)
	if _, ok := err.(testutil.ErrorTestingBuffer); !ok {
		t.Error("Unexpected error return:", err)
	}

	for i := 0; i < len(data); i++ {
		err := r.UnmarshalBinary(data[0:i])
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			fmt.Println("fail")
			t.Error("Unexpected error return:", err)
		}
	}
}
