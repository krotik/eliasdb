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
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

/*
Common TransactionManager related errors
*/
var (
	ErrBadMagic = fmt.Errorf("Bad magic for transaction log")
)

/*
LogFileSuffix is the file suffix for transaction log files
*/
const LogFileSuffix = "tlg"

/*
DefaultTransInLog is the default number of transactions which should be kept in memory
(affects how often we sync the log from memory)
*/
const DefaultTransInLog = 10

/*
DefaultTransSize is the default number of records in a single transaction
(affects how many record pointers are allocated at first
per transaction)
*/
const DefaultTransSize = 10

/*
TransactionLogHeader is the magic number to identify transaction log files
*/
var TransactionLogHeader = []byte{0x66, 0x42}

/*
LogFile is the abstract interface for an transaction log file.
*/
type LogFile interface {
	io.Writer
	io.Closer
	Sync() error
}

/*
TransactionManager data structure
*/
type TransactionManager struct {
	name      string       // Name of this transaction manager
	logFile   LogFile      // Log file for transactions
	curTrans  int          // Current transaction pointer
	transList [][]*Record  // List of storage files
	maxTrans  int          // Maximal number of transaction before log is written
	owner     *StorageFile // Owner of this manager
}

/*
String returns a string representation of a TransactionManager.
*/
func (t *TransactionManager) String() string {
	buf := new(bytes.Buffer)

	hasLog := t.logFile != nil

	buf.WriteString(fmt.Sprintf("Transaction Manager: %v (logFile:%v curTrans:%v "+
		"maxTrans:%v)\n", t.name, hasLog, t.curTrans, t.maxTrans))

	buf.WriteString("====\n")

	buf.WriteString("transList:\n")

	for i := 0; i < len(t.transList); i++ {
		buf.WriteString(fmt.Sprint(i, ": "))
		for _, record := range t.transList[i] {
			buf.WriteString(fmt.Sprint(record.ID(), " "))
		}
		buf.WriteString("\n")
	}

	buf.WriteString("====\n")

	return buf.String()
}

/*
NewTransactionManager creates a new transaction manager and returns a reference to it.
*/
func NewTransactionManager(owner *StorageFile, doRecover bool) (*TransactionManager, error) {
	name := fmt.Sprintf("%s.%s", owner.Name(), LogFileSuffix)

	ret := &TransactionManager{name, nil, -1, make([][]*Record, DefaultTransInLog),
		DefaultTransInLog, owner}

	if doRecover {
		if err := ret.recover(); err != nil {
			if sfe, ok := err.(*StorageFileError); !ok || sfe.Type != ErrBadMagic {
				return nil, err
			}
		}

		// If we have a bad magic just overwrite the transaction file
	}
	if err := ret.open(); err != nil {
		return nil, err
	}

	return ret, nil
}

/*
recover tries to recover pending transactions from the physical transaction log.
*/
func (t *TransactionManager) recover() error {
	file, err := os.OpenFile(t.name, os.O_RDONLY, 0660)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	// Read and verify magic

	magic := make([]byte, 2)
	i, _ := file.Read(magic)

	if i != 2 || magic[0] != TransactionLogHeader[0] ||
		magic[1] != TransactionLogHeader[1] {
		return NewStorageFileError(ErrBadMagic, "", t.owner.name)
	}

	for true {
		var numRecords int64
		if err := binary.Read(file, binary.LittleEndian, &numRecords); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		recMap := make(map[uint64]*Record)

		for i := int64(0); i < numRecords; i++ {
			record, err := ReadRecord(file)
			if err != nil {
				return err
			}

			// Any duplicated records will only be synced once
			// using the latest version

			recMap[record.ID()] = record
		}

		// If something goes wrong here ignore and try to do the rest

		t.syncRecords(recMap, false)
	}

	return nil
}

/*
Open opens the transaction log for writing.
*/
func (t *TransactionManager) open() error {

	// Always create a new empty transaction log file

	file, err := os.OpenFile(t.name, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0660)
	if err != nil {
		return err
	}
	t.logFile = file

	t.logFile.Write(TransactionLogHeader)
	t.logFile.Sync()
	t.curTrans = -1

	return nil
}

/*
Start starts a new transaction.
*/
func (t *TransactionManager) start() {
	t.curTrans++
	if t.curTrans >= t.maxTrans {
		t.syncLogFromMemory()
		t.curTrans = 0
	}
	t.transList[t.curTrans] = make([]*Record, 0, DefaultTransSize)
}

/*
Add adds a record to the current transaction.
*/
func (t *TransactionManager) add(record *Record) {
	record.IncTransCount()
	t.transList[t.curTrans] = append(t.transList[t.curTrans], record)
}

/*
Commit commits the memory transaction log to the physical transaction log.
*/
func (t *TransactionManager) commit() error {

	// Write how many records will be stored

	if err := binary.Write(t.logFile, binary.LittleEndian,
		int64(len(t.transList[t.curTrans]))); err != nil {

		return err
	}

	// Write records to log file

	for _, record := range t.transList[t.curTrans] {
		if err := record.WriteRecord(t.logFile); err != nil {
			return err
		}
	}

	t.syncFile()

	// Clear all dirty flags

	for _, record := range t.transList[t.curTrans] {
		record.ClearDirty()
	}

	return nil
}

/*
syncFile syncs the transaction log file with the disk.
*/
func (t *TransactionManager) syncFile() {
	t.logFile.Sync()
}

/*
close closes the trasaction log file.
*/
func (t *TransactionManager) close() {
	t.syncFile()

	// If something went wrong with closing the handle
	// we don't care as we release the reference

	t.logFile.Close()
	t.logFile = nil
}

/*
syncLogFromMemory syncs the transaction log from memory to disk.
*/
func (t *TransactionManager) syncLogFromMemory() error {
	t.close()

	recMap := make(map[uint64]*Record)

	for i, transList := range t.transList {
		if transList == nil {
			continue
		}

		// Add each record to the record map, decreasing the transaction count
		// if the same record is listed twice.

		for _, record := range transList {
			_, ok := recMap[record.ID()]
			if ok {
				record.DecTransCount()
			} else {
				recMap[record.ID()] = record
			}
		}

		t.transList[i] = nil
	}

	// Write the records from the record list to disk

	if err := t.syncRecords(recMap, true); err != nil {
		return err
	}

	t.owner.Sync()

	return t.open()
}

/*
syncLogFromDisk syncs the log from disk and clears the memory transaction log.
This is used for the rollback operation.
*/
func (t *TransactionManager) syncLogFromDisk() error {
	t.close()

	for i, transList := range t.transList {
		if transList == nil {
			continue
		}

		// Discard all records which are held in memory

		for _, record := range transList {
			record.DecTransCount()
			if !record.InTransaction() {
				t.owner.releaseInTrans(record, false)
			}
		}

		t.transList[i] = nil
	}

	if err := t.recover(); err != nil {
		return err
	}

	return t.open()
}

/*
syncRecords writes a list of records to the pysical disk file.
*/
func (t *TransactionManager) syncRecords(records map[uint64]*Record, clearMemTransLog bool) error {
	for _, record := range records {
		if err := t.owner.writeRecord(record); err != nil {
			return err
		}
		if clearMemTransLog {
			record.DecTransCount()
			if !record.InTransaction() {
				t.owner.releaseInTrans(record, true)
			}
		}
	}
	return nil
}
