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
	"io"
	"os"
	"reflect"
	"testing"

	"devt.de/common/fileutil"
	"devt.de/common/testutil"
)

/*
TestMain() which controls creation and deletion of DBDIR is defined in
storagefile_test.go
*/

func TestTransactionManagerInitialisation(t *testing.T) {

	if _, err := NewDefaultStorageFile(InvalidFileName, false); err == nil {
		t.Error("Invalid name for transaction log should cause an error")
		return
	}

	sf, err := NewDefaultStorageFile(DBDir+"/trans_test1", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	oldname := sf.name
	sf.name = InvalidFileName
	if _, err = NewTransactionManager(sf, true); err == nil {
		t.Error("Invalid name for transaction log should cause an error")
		return
	}
	if _, err = NewTransactionManager(sf, false); err == nil {
		t.Error("Invalid name for transaction log should cause an error")
		return
	}
	sf.name = oldname

	if sf.Name() != DBDir+"/trans_test1" {
		t.Error("Unexpected name of StorageFile:", sf.Name())
		return
	}

	if sf.RecordSize() != DefaultRecordSize {
		t.Error("Unexpected record size:", sf.RecordSize())
		return
	}

	tmName := sf.tm.name
	if err = sf.Close(); err != nil {
		t.Error(err)
		return
	}

	res, err := fileutil.PathExists(DBDir + "/trans_test1.0")
	if err != nil {
		t.Error(err)
		return
	}
	if !res {
		t.Error("Expected db file test1.0 does not exist")
		return
	}

	res, err = fileutil.PathExists(DBDir + "/trans_test1." + LogFileSuffix)
	if err != nil {
		t.Error(err)
		return
	}
	if !res {
		t.Error("Expected db file test1.0 does not exist")
		return
	}

	// Test Magic

	file, err := os.OpenFile(tmName, os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		t.Error(err)
	}

	file.Write([]byte{0x01, 0x02})
	file.Close()

	tm, err := NewTransactionManager(sf, true)
	if err != nil {
		t.Error(err)
		return
	}
	tm.close()

	file, err = os.OpenFile(tmName, os.O_RDONLY, 0660)
	if err != nil {
		t.Error(err)
		return
	}

	buf := make([]byte, 2)
	if _, err = file.Read(buf); err != nil {
		t.Error(err)
		return
	}

	if !reflect.DeepEqual(buf, TransactionLogHeader) {
		t.Error("Magic should have been restored in the transaction file")
	}

	if _, err = file.Read(buf); err != io.EOF {
		t.Error("File should only contain magic")
	}

	file.Close()

	// Next time we should still be able to open the file without problems

	tm, err = NewTransactionManager(sf, true)

	if err != nil {
		t.Error(err)
		return
	}
	tm.close()

	// Test corrupted transaction log

	file, err = os.OpenFile(tmName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0660)
	if err != nil {
		t.Error(err)
	}

	file.Write(TransactionLogHeader)
	file.WriteString("*")
	file.Close()

	if _, err = NewTransactionManager(sf, true); err != io.ErrUnexpectedEOF {
		t.Error("Corrupted transaction logs should get an unexpected EOF", err)
		return
	}

	file, err = os.OpenFile(tmName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0660)
	if err != nil {
		t.Error(err)
	}

	file.Write(TransactionLogHeader)
	file.Write([]byte{0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	file.WriteString("HalloTEST")
	file.Close()
	if _, err = NewTransactionManager(sf, true); err != io.ErrUnexpectedEOF {
		t.Error("Corrupted transaction logs should get an unexpected EOF", err)
		return
	}
}

func TestTMSimpleHighLevelGetRelease(t *testing.T) {

	sf, err := NewDefaultStorageFile(DBDir+"/trans_test2", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	record, err := sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}
	record.WriteSingleByte(5, 0x42)
	sf.ReleaseInUse(record)

	if err = sf.Close(); err != nil {
		t.Error(err)
		return
	}

	// Check that all files are closed now on the sf
	l := len(sf.free) | len(sf.inUse) | len(sf.inTrans) | len(sf.dirty) | len(sf.files)
	if l != 0 {
		t.Error("Left over data in StorageFile:", sf)
	}

	// StorageFiles with transaction management cannot be reused after they
	// were closed.

	sf, err = NewDefaultStorageFile(DBDir+"/trans_test2", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	record, err = sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	if record.ReadSingleByte(5) != 0x42 {
		t.Error("Unexpected value in record")
	}

	sf.ReleaseInUse(record)

	sf.Close()

	l = len(sf.free) | len(sf.inUse) | len(sf.inTrans) |
		len(sf.dirty) | len(sf.files)
	if l != 0 {
		t.Error("Left over data in StorageFile:", sf)
	}
}

func TestTMComplexHighLevelGetRelease(t *testing.T) {

	// Test the auto commit of many transactions

	sf, err := NewDefaultStorageFile(DBDir+"/trans_test3", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	// Releasing a nil pointer should have no effect
	sf.releaseInTrans(nil, true)

	for i := 0; i < DefaultTransInLog+1; i++ {
		record, err := sf.Get(1 + uint64(i))
		if err != nil {
			t.Error(err)
			return
		}
		record.WriteSingleByte(5+i, 0x42)
		sf.ReleaseInUse(record)

		if i == DefaultTransInLog {
			if len(sf.inTrans) != DefaultTransInLog {
				t.Error("Expected", DefaultTransInLog, "records in transaction")
			}
		}

		sf.Flush()

		if i == DefaultTransInLog {
			if len(sf.inTrans) != 1 && len(sf.free) == 10 {
				t.Error("Expected", DefaultTransInLog, "records to be free")
			}
			out := sf.String()

			if out != "Storage File: storagefiletest/trans_test3 (transDisabled:false recordSize:4096 maxFileSize:9999998976)\n"+
				"====\n"+
				"Free Records: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10\n"+
				"InUse Records: \n"+
				"InTrans Records: 11\n"+
				"Dirty Records: \n"+
				"Open files: storagefiletest/trans_test3.0 (0)\n"+
				"====\n"+
				"Transaction Manager: storagefiletest/trans_test3.tlg (logFile:true curTrans:0 maxTrans:10)\n"+
				"====\n"+
				"transList:\n"+
				"0: 11 \n"+
				"1: \n"+
				"2: \n"+
				"3: \n"+
				"4: \n"+
				"5: \n"+
				"6: \n"+
				"7: \n"+
				"8: \n"+
				"9: \n"+
				"====\n" {
				t.Error("Unexpected output of storage file:", out)
				return
			}

			// Do one more and check that a free record is being reused
			record, err := sf.Get(1 + uint64(i+1))
			if err != nil {
				t.Error(err)
				return
			}

			if len(sf.free) != DefaultTransInLog-1 {
				t.Error("Expected that a free record would be reused")
			}
			for _, b := range record.data {
				if b != 0x00 {
					t.Error("Reused record was not cleaned properly:", record)
					return
				}
			}

			record.WriteSingleByte(5+i+1, 0x42)
			sf.ReleaseInUse(record)
			sf.Flush()
		}
	}

	sf.Close()

	tm, err := NewTransactionManager(sf, true)
	if err != nil {
		t.Error(err)
		return
	}
	defer tm.logFile.Close()
	sf.tm = tm

	sf.name = InvalidFileName

	record := NewRecord(5, make([]byte, sf.recordSize, sf.recordSize))

	tm.transList[0] = append(tm.transList[0], record)
	tm.transList[1] = append(tm.transList[1], record)
	record.transCount = 2

	if err := tm.syncLogFromMemory(); err == nil {
		t.Error("Writing records to an invalid storage file should fail")
	}

	if record.transCount != 1 {
		t.Error("Transaction count in record should have been decreased")
	}

	// tm.logFile is now nil since there was an error

	tm.logFile = testutil.NewTestingFile(10)

	tm.curTrans = 5
	tm.start()
	tm.add(record)

	if err := tm.commit(); err == nil {
		t.Error("Failed write operations should be reported")
	}

	tm.logFile = testutil.NewTestingFile(3)

	if err := tm.commit(); err == nil {
		t.Error("Failed write operations should be reported")
	}
}

func TestRecover(t *testing.T) {

	sf, err := NewDefaultStorageFile(DBDir+"/trans_test4", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	record, err := sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}
	record.WriteSingleByte(5, 0x42)
	sf.ReleaseInUse(record)
	sf.Flush()

	// Getting the record for a read operation and then releasing it
	// should have no effect on its membership in the transaction
	record, err = sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}
	sf.ReleaseInUse(record)
	if sf.inTrans[record.ID()] == nil {
		t.Error("Record should still be part of the transaction")
		return
	}

	record, err = sf.Get(2)
	if err != nil {
		t.Error(err)
		return
	}
	record.WriteSingleByte(6, 0x42)
	sf.ReleaseInUse(record)

	// Not let an error happen which makes the transaction log file unavailable
	sf.tm.logFile.Close()

	if sf.Flush() == nil {
		t.Error("Flush should fail when the transaction log cannot be accessed")
	}

	record, err = sf.Get(2)
	if err != nil {
		t.Error(err)
		return
	}
	record.WriteSingleByte(7, 0x42)
	sf.ReleaseInUse(record)

	// The record should still be in a transaction and should have now both changes
	if record.ReadSingleByte(5) != 0 || record.ReadSingleByte(6) != 0x42 ||
		record.ReadSingleByte(7) != 0x42 {

		t.Error("Unexpected data in record:", record)
		return
	}

	if err = sf.Close(); err == nil {
		t.Error(err)
		return
	}

	// Now lets get out of the mess

	sf.transDisabled = true

	if err = sf.Close(); err != ErrInTrans {
		t.Error(err)
		return
	}

	sf.inTrans = make(map[uint64]*Record)

	if err = sf.Close(); err != nil {
		t.Error(err)
		return
	}

	// Check that all files are closed now on the sf
	l := len(sf.free) | len(sf.inUse) | len(sf.inTrans) | len(sf.dirty) | len(sf.files)
	if l != 0 {
		t.Error("Left over data in StorageFile:", sf)
	}

	// Open the StorageFile again and hope that recover() does the right thing

	sf, err = NewDefaultStorageFile(DBDir+"/trans_test4", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	record, err = sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}
	record2, err := sf.Get(2)
	if err != nil {
		t.Error(err)
		return
	}

	// Check that expected values are there / not there
	if record.ReadSingleByte(5) != 0x42 || record.ReadSingleByte(6) != 0 ||
		record.ReadSingleByte(7) != 0 {

		t.Error("Unexpected data in record1:", record)
		return
	}

	// All transactions on record2 should have failed
	if record2.ReadSingleByte(5) != 0 || record2.ReadSingleByte(6) != 0 ||
		record2.ReadSingleByte(7) != 0 {

		t.Error("Unexpected data in record2:", record)
		return
	}

	sf.ReleaseInUse(record)
	sf.ReleaseInUse(record2)

	sf.Close()

	// Check that all files are closed now on the sf
	l = len(sf.free) | len(sf.inUse) | len(sf.inTrans) | len(sf.dirty) | len(sf.files)
	if l != 0 {
		t.Error("Left over data in StorageFile:", sf)
	}
}

func TestRollback(t *testing.T) {

	sf, err := NewDefaultStorageFile(DBDir+"/trans_test5", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	record, err := sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}
	record.WriteSingleByte(5, 0x42)
	sf.ReleaseInUse(record)

	sf.Flush()

	record, err = sf.Get(2)
	if err != nil {
		t.Error(err)
		return
	}
	record.WriteSingleByte(6, 0x42)
	sf.ReleaseInUse(record)

	if err := sf.Rollback(); err != nil {
		t.Error(err)
		return
	}

	if err = sf.Close(); err != nil {
		t.Error(err)
		return
	}

	sf, err = NewDefaultStorageFile(DBDir+"/trans_test2", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	record, err = sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}
	record2, err := sf.Get(2)
	if err != nil {
		t.Error(err)
		return
	}

	// Check that expected values are there / not there
	if record.ReadSingleByte(5) != 0x42 || record.ReadSingleByte(6) != 0 {

		t.Error("Unexpected data in record1:", record)
		return
	}

	// All transactions on record2 should have failed
	if record2.ReadSingleByte(5) != 0 || record2.ReadSingleByte(6) != 0 {

		t.Error("Unexpected data in record2:", record)
		return
	}

	sf.ReleaseInUse(record)
	sf.ReleaseInUse(record2)

	sf.Close()
}

func TestRollbackFail(t *testing.T) {
	sf, err := NewDefaultStorageFile(DBDir+"/trans_test6", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	record, err := sf.Get(1)
	if err != nil {
		t.Error(err)
		return
	}
	record.WriteSingleByte(5, 0x42)

	if err = sf.Rollback(); err != ErrInUse {
		t.Error("It should not be possible to rollback while records are still in use")
	}

	sf.ReleaseInUse(record)

	sf.tm.logFile.Close()
	sf.tm.name = DBDir + "/" + InvalidFileName + "." + LogFileSuffix
	if err = sf.Rollback(); err == nil {
		t.Error("Rollback should fail when using invalid filename for transaction log")
		return
	}

	// Cleanup
	sf.transDisabled = true
	sf.Close()

	sf, err = NewDefaultStorageFile(DBDir+"/trans_test6", false)
	if err != nil {
		t.Error(err.Error())
		return
	}

	sf.inTrans[record.ID()] = record
	if err = sf.Rollback(); err != ErrInTrans {
		t.Error("It should not be possible to rollback while records are still in transaction")
		return
	}

	delete(sf.inTrans, record.ID())

	sf.Close()
}
