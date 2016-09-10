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
	"os"

	"devt.de/common/sortutil"
)

/*
Common storage file related errors. Having these global definitions
makes the error comparison easier but has potential race-conditions.
If two StorageFile objects throw an error at the same time both errors
will appear to come from the same instance.
*/
var (
	ErrAlreadyInUse  = newStorageFileError("Record is already in-use")
	ErrNotInUse      = newStorageFileError("Record was not in-use")
	ErrInUse         = newStorageFileError("Records are still in-use")
	ErrTransDisabled = newStorageFileError("Transactions are disabled")
	ErrInTrans       = newStorageFileError("Records are still in a transaction")
	ErrNilData       = newStorageFileError("Record has nil data")
)

/*
DefaultRecordSize is the default size of a record in bytes
*/
const DefaultRecordSize = 4096

/*
DefaultFileSize is the default size of a physical file (10GB)
*/
const DefaultFileSize = 0x2540BE401 // 10000000001 Bytes

/*
StorageFile data structure
*/
type StorageFile struct {
	name          string // Name of the storage file
	transDisabled bool   // Flag if transactions are disabled
	recordSize    uint32 // Size of a record
	maxFileSize   uint64 // Max size of a storage file on disk

	free    map[uint64]*Record // Map of records which are stored in memory
	inUse   map[uint64]*Record // Locked records which are currently being modified
	inTrans map[uint64]*Record // Records which are in the transaction log but not yet written to disk
	dirty   map[uint64]*Record // Dirty little records waiting to be written

	files []*os.File // List of storage files

	tm *TransactionManager // Manager object for transactions
}

/*
NewDefaultStorageFile creates a new storage file with default record size and
returns a reference to it.
*/
func NewDefaultStorageFile(name string, transDisabled bool) (*StorageFile, error) {
	return NewStorageFile(name, DefaultRecordSize, transDisabled)
}

/*
NewStorageFile creates a new storage file and returns a reference to it.
*/
func NewStorageFile(name string, recordSize uint32, transDisabled bool) (*StorageFile, error) {
	maxFileSize := DefaultFileSize - DefaultFileSize%uint64(recordSize)

	ret := &StorageFile{name, transDisabled, recordSize, maxFileSize,
		make(map[uint64]*Record), make(map[uint64]*Record), make(map[uint64]*Record),
		make(map[uint64]*Record), make([]*os.File, 0), nil}

	if !transDisabled {
		tm, err := NewTransactionManager(ret, true)
		if err != nil {
			return nil, err
		}
		ret.tm = tm
	}

	_, err := ret.getFile(0)

	if err != nil {
		return nil, err
	}

	return ret, nil
}

/*
Name returns the name of this storage file.
*/
func (s *StorageFile) Name() string {
	return s.name
}

/*
RecordSize returns the size of records which can be storerd or retrieved.
*/
func (s *StorageFile) RecordSize() uint32 {
	return s.recordSize
}

/*
Get returns a record from the file. Other components can write to this record.
Any write operation should set the dirty flag on the record. Dirty records will
be written back to disk when the file is flushed after which the dirty flag is
cleared. Get panics if a record is requested which is still in-use.
*/
func (s *StorageFile) Get(id uint64) (*Record, error) {
	var record *Record

	// Check if the record is in one of the caches

	if record, ok := s.inTrans[id]; ok {
		delete(s.inTrans, id)
		s.inUse[id] = record
		return record, nil
	}

	if record, ok := s.dirty[id]; ok {
		delete(s.dirty, id)
		s.inUse[id] = record
		return record, nil
	}

	if record, ok := s.free[id]; ok {
		delete(s.free, id)
		s.inUse[id] = record
		return record, nil
	}

	// Error if a record which is in-use is requested again before it is released.
	if _, ok := s.inUse[id]; ok {
		return nil, ErrAlreadyInUse.fireError(s, fmt.Sprintf("Record %v", id))
	}

	// Read the record in from file

	record = s.createRecord(id)
	err := s.readRecord(record)

	if err != nil {
		return nil, err
	}

	s.inUse[id] = record

	return record, nil
}

/*
getFile gets a physical file for a specific offset.
*/
func (s *StorageFile) getFile(offset uint64) (*os.File, error) {

	filenumber := int(offset / s.maxFileSize)

	// Make sure the index exists which we want to use.
	// Fill all previous positions up with nil pointers if they don't exist.

	for i := len(s.files); i <= filenumber; i++ {
		s.files = append(s.files, nil)
	}

	var ret *os.File

	if len(s.files) > filenumber {
		ret = s.files[filenumber]
	}

	if ret == nil {

		// Important not to have os.O_APPEND since we really want
		// to have random access to the file.

		filename := fmt.Sprintf("%s.%d", s.name, filenumber)

		file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0660)
		if err != nil {
			return nil, err
		}

		s.files[filenumber] = file
		ret = file
	}

	return ret, nil
}

/*
createRecord creates a new record - (either from the free cache or newly created).
*/
func (s *StorageFile) createRecord(id uint64) *Record {
	var record *Record

	if len(s.free) != 0 {
		var rkey uint64

		for rkey, record = range s.free {
			break
		}
		delete(s.free, rkey)

		// NOTE At this point the free record contains
		// still old data. It is expected that the following
		// readRecord operation will overwrite the data.
	}
	if record == nil {
		record = NewRecord(id, make([]byte, s.recordSize, s.recordSize))
	}

	record.SetID(id)
	record.SetPageView(nil)
	record.ClearDirty()

	return record
}

/*
writeRecord writes a record to disk.
*/
func (s *StorageFile) writeRecord(record *Record) error {
	data := record.Data()

	if data != nil {

		offset := record.ID() * uint64(s.recordSize)

		file, err := s.getFile(offset)
		if err != nil {
			return err
		}

		file.WriteAt(data, int64(offset%s.maxFileSize))

		return nil
	}

	return ErrNilData.fireError(s, fmt.Sprintf("Record %v", record.ID()))
}

/*
readRecord fills a given record object with data.
*/
func (s *StorageFile) readRecord(record *Record) error {

	if record.Data() == nil {
		return ErrNilData.fireError(s, fmt.Sprintf("Record %v", record.ID()))
	}

	offset := record.ID() * uint64(s.recordSize)

	file, err := s.getFile(offset)
	if err != nil {
		return err
	}

	n, err := file.ReadAt(record.Data(), int64(offset%s.maxFileSize))

	if n > 0 && uint32(n) != s.recordSize {
		panic(fmt.Sprintf("File on disk returned unexpected length of data: %v "+
			"expected length was: %v", n, s.recordSize))
	} else if n == 0 {
		// We just allocate a new array here which seems to be the
		// quickest way to get an empty array.
		record.ClearData()
	}

	if err == io.EOF {
		return nil
	}

	return err
}

/*
Discard a given record.
*/
func (s *StorageFile) Discard(record *Record) {
	if record == nil {
		return
	}

	delete(s.inUse, record.ID())
}

/*
releaseInTrans releases a record which was in a transaction. The client code
may indicate if the record should be recycled.
*/
func (s *StorageFile) releaseInTrans(record *Record, recycle bool) {
	if record == nil {
		return
	}

	_, ok := s.inTrans[record.ID()]

	if ok {
		delete(s.inTrans, record.ID())

		if recycle {
			s.free[record.ID()] = record
		}
	}
}

/*
ReleaseInUseID releases a record given by its id from the in-use map. The
client code may indicate if the record is not dirty.
*/
func (s *StorageFile) ReleaseInUseID(id uint64, dirty bool) error {
	record, ok := s.inUse[id]

	if !ok {
		return ErrNotInUse.fireError(s, fmt.Sprintf("Record %v", id))
	}

	if !record.Dirty() && dirty {
		record.SetDirty()
	}

	s.ReleaseInUse(record)

	return nil
}

/*
ReleaseInUse releases a record from the in-use map. ReleaseInUse panics if
the record was not in use.
*/
func (s *StorageFile) ReleaseInUse(record *Record) {
	if record == nil {
		return
	}

	id := record.ID()

	// Panic if a record which is release was not in-use.
	if _, ok := s.inUse[id]; !ok {
		panic(fmt.Sprintf("Released record %d was not in-use", id))
	}
	delete(s.inUse, id)

	if record.Dirty() {
		s.dirty[id] = record
	} else {
		if !s.transDisabled && record.InTransaction() {
			s.inTrans[id] = record
		} else {
			s.free[id] = record
		}
	}
}

/*
Flush commits the current transaction by flushing all dirty records to the
transaction log on disk. If transactions are disabled it simply
writes all dirty records to disk.
*/
func (s *StorageFile) Flush() error {
	if len(s.inUse) > 0 {
		return ErrInUse.fireError(s, fmt.Sprintf("Records %v", len(s.inUse)))
	}

	if len(s.dirty) == 0 {
		return nil
	}

	if !s.transDisabled {
		s.tm.start()
	}

	for id, record := range s.dirty {

		if s.transDisabled {
			err := s.writeRecord(record)
			if err != nil {
				return err
			}
			record.ClearDirty()
			delete(s.dirty, id)
			s.free[id] = record
		} else {
			s.tm.add(record)
			delete(s.dirty, id)
			s.inTrans[id] = record
		}
	}

	if !s.transDisabled {
		return s.tm.commit()
	}
	return nil
}

/*
Rollback cancels the current transaction by discarding all dirty records.
*/
func (s *StorageFile) Rollback() error {

	if s.transDisabled {
		return ErrTransDisabled.fireError(s, "")
	}

	if len(s.inUse) > 0 {
		return ErrInUse.fireError(s, fmt.Sprintf("Records %v", len(s.inUse)))
	}

	s.dirty = make(map[uint64]*Record)

	if err := s.tm.syncLogFromDisk(); err != nil {
		return err
	}

	if len(s.inTrans) > 0 {
		return ErrInTrans.fireError(s, fmt.Sprintf("Records %v", len(s.inTrans)))
	}

	return nil
}

/*
Sync syncs all physical files.
*/
func (s *StorageFile) Sync() {

	for _, file := range s.files {
		if file != nil {
			file.Sync()
		}
	}
}

/*
Close commits all data and closes all physical files.
*/
func (s *StorageFile) Close() error {

	if len(s.dirty) > 0 {
		if err := s.Flush(); err != nil {
			return err
		}
	}

	if !s.transDisabled {

		// If something fails here we will know about it
		// when checking if there are records in inTrans

		s.tm.syncLogFromMemory()
		s.tm.close()
	}

	if len(s.inTrans) > 0 {
		return ErrInTrans.fireError(s, fmt.Sprintf("Records %v", len(s.inTrans)))
	} else if len(s.inUse) > 0 {
		return ErrInUse.fireError(s, fmt.Sprintf("Records %v", len(s.inUse)))
	}

	for _, file := range s.files {
		if file != nil {
			file.Close()
		}
	}

	s.free = make(map[uint64]*Record)
	s.files = make([]*os.File, 0)

	// If transactions are enabled then a StorageFile cannot be
	// reused after it was closed.

	s.tm = nil

	return nil
}

/*
String returns a string representation of a StorageFile.
*/
func (s *StorageFile) String() string {
	buf := new(bytes.Buffer)

	buf.WriteString(fmt.Sprintf("Storage File: %v (transDisabled:%v recordSize:%v "+
		"maxFileSize:%v)\n", s.name, s.transDisabled, s.recordSize, s.maxFileSize))

	buf.WriteString("====\n")

	printRecordIDMap(buf, &s.free, "Free")
	buf.WriteString("\n")
	printRecordIDMap(buf, &s.inUse, "InUse")
	buf.WriteString("\n")
	printRecordIDMap(buf, &s.inTrans, "InTrans")
	buf.WriteString("\n")
	printRecordIDMap(buf, &s.dirty, "Dirty")
	buf.WriteString("\n")

	buf.WriteString("Open files: ")
	l := len(s.files)
	for i, file := range s.files {
		if file != nil {
			buf.WriteString(file.Name())
			buf.WriteString(fmt.Sprintf(" (%v)", i))
			if i < l-1 {
				buf.WriteString(", ")
			}
		}
	}
	buf.WriteString("\n")

	buf.WriteString("====\n")

	if s.tm != nil {
		buf.WriteString(s.tm.String())
	}

	return buf.String()
}

/*
printRecordIDMap appends the ids of a record map to a given buffer.
*/
func printRecordIDMap(buf *bytes.Buffer, recordMap *map[uint64]*Record, name string) {
	buf.WriteString(name)
	buf.WriteString(" Records: ")

	var keys []uint64
	for k := range *recordMap {
		keys = append(keys, k)
	}
	sortutil.UInt64s(keys)

	l := len(*recordMap)

	for _, id := range keys {
		buf.WriteString(fmt.Sprintf("%v", id))
		if l--; l > 0 {
			buf.WriteString(", ")
		}
	}
}

/*
newStorageFileError returns a new StorageFile specific error.
*/
func newStorageFileError(text string) *storagefileError {
	return &storagefileError{text, "?", ""}
}

/*
StorageFile specific error datastructure
*/
type storagefileError struct {
	msg      string
	filename string
	info     string
}

/*
fireError returns the error instance from a specific StorageFile instance.
*/
func (e *storagefileError) fireError(s *StorageFile, info string) error {
	e.filename = s.name
	e.info = info
	return e
}

/*
Error returns a string representation of the error.
*/
func (e *storagefileError) Error() string {
	return fmt.Sprintf("%s (%s - %s)", e.msg, e.filename, e.info)
}
