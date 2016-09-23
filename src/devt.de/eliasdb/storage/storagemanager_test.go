/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package storage

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"devt.de/common/fileutil"
)

const DBDIR = "storagemanagertest"

// Main function for all tests in this package

func TestMain(m *testing.M) {
	flag.Parse()

	// Setup
	if res, _ := fileutil.PathExists(DBDIR); res {
		os.RemoveAll(DBDIR)
	}

	err := os.Mkdir(DBDIR, 0770)
	if err != nil {
		fmt.Print("Could not create test directory:", err.Error())
		os.Exit(1)
	}

	// Run the tests
	res := m.Run()

	// Teardown
	err = os.RemoveAll(DBDIR)
	if err != nil {
		fmt.Print("Could not remove test directory:", err.Error())
	}

	os.Exit(res)
}

var enableConcurrencyTest = false

func TestStorageManagerConcurrency(t *testing.T) {

	// Disabled for normal testing

	if !enableConcurrencyTest {
		return
	}

	var retChans []chan error

	threads := 50
	ops := 1000

	dsm := NewDiskStorageManager(DBDIR+"/ctest_dsm", false, false, true, false)
	cdsm := NewCachedDiskStorageManager(dsm, 50000)
	sm := cdsm

	start := time.Now()

	for i := 1; i < threads+1; i++ {
		retChan := make(chan error)
		retChans = append(retChans, retChan)

		// Kick off thread

		fmt.Println("Id:", strconv.Itoa(i), " start")
		go runConcurrencyTest(strconv.Itoa(i), sm, ops, retChan)
	}

	// Wait for threads to complete

	for i := 0; i < threads; i++ {
		retChan := retChans[i]

		err := <-retChan
		if err != nil {
			fmt.Println("Id:", strconv.Itoa(i), " Error:", err)
		} else {
			fmt.Println("Id:", strconv.Itoa(i), " ok")
		}
	}

	elapsed := time.Since(start)
	fmt.Println("Total time:", elapsed)

	sm.Close()
}

var enablePerformanceTest = false

func TestStorageManagerPerformance(t *testing.T) {

	// Disabled for normal testing

	if !enablePerformanceTest {
		return
	}

	// Test multiple read/write operations in concurrent threads

	start := time.Now()

	// Last iteration here shows the cache running out of available entries -
	// Since we ask for the same elements in the same order we completely
	// loose the benefit of the cache (i.e. oldest elements are removed first)

	for i := 1000; i < 51001; i += 5000 {
		dsm := NewDiskStorageManager(DBDIR+"/ptest_dsm", false, false, true, false)
		cdsm := NewCachedDiskStorageManager(dsm, 50000)
		runPerformanceTest("1", cdsm, i)
	}

	elapsed := time.Since(start)
	fmt.Println("Total time:", elapsed)
}

func runConcurrencyTest(id string, sm Manager, ops int, retChan chan error) {

	errorChan := make(chan error)

	tc := &testclient{make([]uint64, 0)}

	// Insert, Fetch, Update, Fetch some data

	start := time.Now()

	go tc.clientInsert(id, sm, ops, errorChan)
	res := <-errorChan
	if res != nil {
		retChan <- res
	}

	go tc.clientFetch(id, "test", sm, ops, errorChan)
	res = <-errorChan
	if res != nil {
		retChan <- res
	}

	go tc.clientUpdate(id, "t35ter", sm, ops, errorChan)
	res = <-errorChan
	if res != nil {
		retChan <- res
	}

	go tc.clientFetch(id, "t35ter", sm, ops, errorChan)
	res = <-errorChan
	if res != nil {
		retChan <- res
	}

	elapsed := time.Since(start).Nanoseconds() / (1000 * 1000)

	fmt.Println("Id:", id, " Strings:", ops, " Time:", elapsed)

	retChan <- nil
}

func runPerformanceTest(id string, sm Manager, ops int) {

	var elapsed1, elapsed2, elapsed3, elapsed4, elapsed5, elapsed6, elapsed7 int64

	errorChan := make(chan error)

	tc := &testclient{make([]uint64, 0)}

	// Insert some data

	start := time.Now()

	go tc.clientInsert(id, sm, ops, errorChan)

	res := <-errorChan
	if res != nil {
		fmt.Println("tc.clientInsert:", res)
	}

	elapsed1 = time.Since(start).Nanoseconds() / (1000 * 1000)

	// Read data back

	start = time.Now()

	go tc.clientFetch(id, "test", sm, ops, errorChan)

	res = <-errorChan
	if res != nil {
		fmt.Println("tc.clientFetch:", res)
	}

	elapsed2 = time.Since(start).Nanoseconds() / (1000 * 1000)

	// Read data back a 2nd time

	start = time.Now()

	go tc.clientFetch(id, "test", sm, ops, errorChan)

	res = <-errorChan
	if res != nil {
		fmt.Println("tc.clientFetch:", res)
	}

	elapsed3 = time.Since(start).Nanoseconds() / (1000 * 1000)

	// Update the data without reallocation

	start = time.Now()

	go tc.clientUpdate(id, "t35t", sm, ops, errorChan)

	res = <-errorChan
	if res != nil {
		fmt.Println("tc.clientUpdate:", res)
	}

	elapsed4 = time.Since(start).Nanoseconds() / (1000 * 1000)

	// Read data back a 3nd time

	start = time.Now()

	go tc.clientFetch(id, "t35t", sm, ops, errorChan)

	res = <-errorChan
	if res != nil {
		fmt.Println("tc.clientFetch:", res)
	}

	elapsed5 = time.Since(start).Nanoseconds() / (1000 * 1000)

	// Update the data with reallocation

	start = time.Now()

	go tc.clientUpdate(id, "teststring", sm, ops, errorChan)

	res = <-errorChan
	if res != nil {
		fmt.Println("tc.clientUpdate:", res)
	}

	elapsed6 = time.Since(start).Nanoseconds() / (1000 * 1000)

	// Read data back a 4th time

	start = time.Now()

	go tc.clientFetch(id, "teststring", sm, ops, errorChan)

	<-errorChan

	elapsed7 = time.Since(start).Nanoseconds() / (1000 * 1000)

	fmt.Println("Strings,", ops, ",Insert,", elapsed1, ",Fetch1,", elapsed2, ",Fetch2,",
		elapsed3, ",Update,", elapsed4, ",Fetch3,", elapsed5, ",Update Realloc,",
		elapsed6, ",Fetch,", elapsed7)

	sm.Close()
}

type testclient struct {
	locs []uint64
}

/*
clientInsert inserts some test data.
*/
func (tc *testclient) clientInsert(id string, sm Manager, ops int, errorChan chan error) {

	// Write stull

	for i := 0; i < ops; i++ {

		loc, err := sm.Insert(fmt.Sprint("test-", id, i))

		if err != nil {
			errorChan <- errors.New(fmt.Sprint("Error during insert thread:", id, " iteration:", i, " error:", err.Error()))
			return
		}

		tc.locs = append(tc.locs, loc)
	}

	// Flush changes to disk

	if err := sm.Flush(); err != nil {
		errorChan <- errors.New(fmt.Sprint("Error during flush thread:", id, " error:", err.Error()))
		return
	}

	errorChan <- nil
}

/*
clientFetch reads back test data.
*/
func (tc *testclient) clientFetch(id string, teststring string, sm Manager, ops int, errorChan chan error) {
	var obj interface{}
	var res string
	var err error

	for i := 0; i < ops; i++ {

		obj, _ = sm.FetchCached(tc.locs[i])

		if obj == nil {
			err = sm.Fetch(tc.locs[i], &res)
		} else {
			res = obj.(string)
		}

		if err != nil {
			errorChan <- errors.New(fmt.Sprint("Error during fetch thread:", id, " iteration:", i, " error:", err.Error()))
			return
		}

		if res != fmt.Sprint(teststring, "-", id, i) {
			errorChan <- errors.New(fmt.Sprint("Unexpected fetch result thread:", id, " iteration:", i, " result:", res))
			return
		}
	}

	errorChan <- nil
}

/*
clientUpdate updates test data without requiring relocation.
*/
func (tc *testclient) clientUpdate(id string, teststring string, sm Manager, ops int, errorChan chan error) {

	// Write stull

	for i := 0; i < ops; i++ {

		err := sm.Update(tc.locs[i], fmt.Sprint(teststring, "-", id, i))

		if err != nil {
			errorChan <- errors.New(fmt.Sprint("Error during update thread:", id, " iteration:", i, " error:", err.Error()))
			return
		}
	}

	// Flush changes to disk

	if err := sm.Flush(); err != nil {
		errorChan <- errors.New(fmt.Sprint("Error during flush thread:", id, " error:", err.Error()))
		return
	}

	errorChan <- nil
}
