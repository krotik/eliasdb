/* 
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. 
 */

/*
Wrapper data structure for a byte array which provides read and write methods for
several data types.
*/
package file

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"devt.de/common/bitutil"
	"devt.de/common/stringutil"
)

/*
Size constants
*/
const SIZE_BYTE = 1
const SIZE_UNSIGNED_SHORT = 2
const SIZE_SHORT = 2
const SIZE_THREE_BYTE_INT = 3
const SIZE_UNSIGNED_INT = 4
const SIZE_INT = 4
const SIZE_SIX_BYTE_LONG = 6
const SIZE_LONG = 8

/*
Record data structure
*/
type Record struct {
	id         uint64      // 64-bit record id
	data       []byte      // Slice of the whole data byte array
	dirty      bool        // Firty flag to indicate change
	transCount int         // Transaction counter
	pageView   interface{} // View on this record (this is not persisted)
}

/*
NewRecord creates a new Record and returns a pointer to it.
*/
func NewRecord(id uint64, data []byte) *Record {
	return &Record{id, data, false, 0, nil}
}

/*
Id return the id of the Record.
*/
func (r *Record) Id() uint64 {
	return r.id
}

/*
SetId changes the id of the Record.
*/
func (r *Record) SetId(id uint64) error {
	if r.InTransaction() {
		return errors.New(fmt.Sprintf("Record id cannot be changed. Record "+
			"is used in %d transaction%s.", r.transCount,
			stringutil.Plural(r.transCount)))
	}
	r.id = id
	return nil
}

/*
Data returns the raw data of the Record.
*/
func (r *Record) Data() []byte {
	return r.data
}

/*
Dirty returns the dirty flag of the Record.
*/
func (r *Record) Dirty() bool {
	return r.dirty
}

/*
SetDirty sets the dirty flag of the Record.
*/
func (r *Record) SetDirty() {
	r.dirty = true
}

/*
SetClear clears the dirty flag of the Record.
*/
func (r *Record) ClearDirty() {
	r.dirty = false
}

/*
ClearData removes all stored data from the Record.
*/
func (r *Record) ClearData() {
	var ccap, clen int

	if r.data != nil {
		ccap = cap(r.data)
		clen = len(r.data)
	} else {
		clen = DEFAULT_RECORD_SIZE
		ccap = DEFAULT_RECORD_SIZE
	}
	r.data = make([]byte, clen, ccap)
	r.ClearDirty()
}

/*
InTransaction returns if the Record is used in a transaction.
*/
func (r *Record) InTransaction() bool {
	return r.transCount != 0
}

/*
IncTransCount increments the transaction count which means the record is in the
log but not yet in the data file.
*/
func (r *Record) IncTransCount() {
	r.transCount++
}

/*
DecTransCount decrements the transaction count which means the record has been
written to disk.
*/
func (r *Record) DecTransCount() {
	r.transCount--
	if r.transCount < 0 {
		panic(fmt.Sprintf("Transaction count for record %s is below zero: %s",
			r.id, r.transCount))
	}
}

/*
PageView returns the view on this record. The view determines how the record
is being used.
*/
func (r *Record) PageView() interface{} {
	return r.pageView
}

/*
SetPageView sets the view on this record.
*/
func (r *Record) SetPageView(view interface{}) {
	r.pageView = view
}

/*
String prints a string representation the Record.
*/
func (r *Record) String() string {
	return fmt.Sprintf("Record: %v (dirty:%v transCount:%v len:%v cap:%v)\n%s",
		r.id, r.dirty, r.transCount, len(r.data), cap(r.data), bitutil.HexDump(r.data))
}

// Read and Write functions
// ========================

/*
ReadByte reads a byte from the Record.
*/
func (r *Record) ReadByte(pos int) byte {
	return r.data[pos]
}

/*
WriteByte writes a byte to the Record.
*/
func (r *Record) WriteByte(pos int, value byte) {
	r.data[pos] = value
	r.SetDirty()
}

/*
ReadUInt16 reads a 16-bit unsigned integer from the Record.
*/
func (r *Record) ReadUInt16(pos int) uint16 {
	return (uint16(r.data[pos+0]) << 8) |
		(uint16(r.data[pos+1]) << 0)
}

/*
WriteUInt16 writes a 16-bit unsigned integer to the Record.
*/
func (r *Record) WriteUInt16(pos int, value uint16) {
	r.data[pos+0] = byte(value >> 8)
	r.data[pos+1] = byte(value >> 0)
	r.SetDirty()
}

/*
ReadInt16 reads a 16-bit signed integer from the Record.
*/
func (r *Record) ReadInt16(pos int) int16 {
	return (int16(r.data[pos+0]) << 8) |
		(int16(r.data[pos+1]) << 0)
}

/*
WriteInt16 writes a 16-bit signed integer to the Record.
*/
func (r *Record) WriteInt16(pos int, value int16) {
	r.data[pos+0] = byte(value >> 8)
	r.data[pos+1] = byte(value >> 0)
	r.SetDirty()
}

/*
ReadUInt32 reads a 32-bit unsigned integer from the Record.
*/
func (r *Record) ReadUInt32(pos int) uint32 {
	return (uint32(r.data[pos+0]) << 24) |
		(uint32(r.data[pos+1]) << 16) |
		(uint32(r.data[pos+2]) << 8) |
		(uint32(r.data[pos+3]) << 0)
}

/*
WriteUInt32 writes a 32-bit unsigned integer to the Record.
*/
func (r *Record) WriteUInt32(pos int, value uint32) {
	r.data[pos+0] = byte(value >> 24)
	r.data[pos+1] = byte(value >> 16)
	r.data[pos+2] = byte(value >> 8)
	r.data[pos+3] = byte(value >> 0)
	r.SetDirty()
}

/*
ReadInt32 reads a 32-bit signed integer from the Record.
*/
func (r *Record) ReadInt32(pos int) int32 {
	return (int32(r.data[pos+0]) << 24) |
		(int32(r.data[pos+1]) << 16) |
		(int32(r.data[pos+2]) << 8) |
		(int32(r.data[pos+3]) << 0)
}

/*
WriteInt32 writes a 32-bit signed integer to the Record.
*/
func (r *Record) WriteInt32(pos int, value int32) {
	r.data[pos+0] = byte(value >> 24)
	r.data[pos+1] = byte(value >> 16)
	r.data[pos+2] = byte(value >> 8)
	r.data[pos+3] = byte(value >> 0)
	r.SetDirty()
}

/*
ReadUInt64 reads a 32-bit unsigned integer from the Record.
*/
func (r *Record) ReadUInt64(pos int) uint64 {
	return (uint64(r.data[pos+0]) << 56) |
		(uint64(r.data[pos+1]) << 48) |
		(uint64(r.data[pos+2]) << 40) |
		(uint64(r.data[pos+3]) << 32) |
		(uint64(r.data[pos+4]) << 24) |
		(uint64(r.data[pos+5]) << 16) |
		(uint64(r.data[pos+6]) << 8) |
		(uint64(r.data[pos+7]) << 0)
}

/*
WriteUInt32 writes a 32-bit unsigned integer to the Record.
*/
func (r *Record) WriteUInt64(pos int, value uint64) {
	r.data[pos+0] = byte(value >> 56)
	r.data[pos+1] = byte(value >> 48)
	r.data[pos+2] = byte(value >> 40)
	r.data[pos+3] = byte(value >> 32)
	r.data[pos+4] = byte(value >> 24)
	r.data[pos+5] = byte(value >> 16)
	r.data[pos+6] = byte(value >> 8)
	r.data[pos+7] = byte(value >> 0)
	r.SetDirty()
}

/*
Return this record as a binary representation.
*/
func (r *Record) MarshalBinary() (data []byte, err error) {
	buf := new(bytes.Buffer)

	// Using a normal memory buffer this should always succeed
	r.WriteRecord(buf)

	return buf.Bytes(), nil
}

/*
Write this record to an io.Writer.
*/
func (r *Record) WriteRecord(iow io.Writer) error {
	if err := binary.Write(iow, binary.LittleEndian, r.id); err != nil {
		return err
	}

	if r.dirty {
		if err := binary.Write(iow, binary.LittleEndian, int8(1)); err != nil {
			return err
		}
	} else {
		if err := binary.Write(iow, binary.LittleEndian, int8(0)); err != nil {
			return err
		}
	}

	if err := binary.Write(iow, binary.LittleEndian, int64(r.transCount)); err != nil {
		return err
	}

	if err := binary.Write(iow, binary.LittleEndian, int64(len(r.data))); err != nil {
		return err
	}
	if _, err := iow.Write(r.data); err != nil {
		return err
	}

	// PageView is not persisted since it is derived from the record data

	return nil
}

/*
Decode a record from a binary blob.
*/
func (r *Record) UnmarshalBinary(data []byte) error {
	buf := new(bytes.Buffer)
	buf.Write(data)

	return r.ReadRecord(buf)
}

/*
Decode a record by reading from a io.Reader.
*/
func (r *Record) ReadRecord(ior io.Reader) error {
	if err := binary.Read(ior, binary.LittleEndian, &r.id); err != nil {
		return err
	}

	r.pageView = nil

	var d int8
	if err := binary.Read(ior, binary.LittleEndian, &d); err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	r.dirty = d == 1

	var t int64
	if err := binary.Read(ior, binary.LittleEndian, &t); err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	r.transCount = int(t)

	if err := binary.Read(ior, binary.LittleEndian, &t); err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return err
	}

	r.data = make([]byte, t)

	i, err := io.ReadFull(ior, r.data)

	if int64(i) != t {
		return io.ErrUnexpectedEOF
	}
	return err
}

/*
Decode a record by reading from a io.Reader.
*/
func ReadRecord(ior io.Reader) (*Record, error) {
	r := NewRecord(0, nil)
	if err := r.ReadRecord(ior); err != nil {
		return nil, err
	}
	return r, nil
}
