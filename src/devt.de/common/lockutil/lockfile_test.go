/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package lockutil

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"devt.de/common/fileutil"
)

const lfdir = "lockfiletest"

const invalidFileName = "**" + string(0x0)

func TestMain(m *testing.M) {
	flag.Parse()

	// Setup
	if res, _ := fileutil.PathExists(lfdir); res {
		os.RemoveAll(lfdir)
	}

	err := os.Mkdir(lfdir, 0770)
	if err != nil {
		fmt.Print("Could not create test directory:", err.Error())
		os.Exit(1)
	}

	// Run the tests
	res := m.Run()

	// Teardown
	err = os.RemoveAll(lfdir)
	if err != nil {
		fmt.Print("Could not remove test directory:", err.Error())
	}

	os.Exit(res)
}

func TestLockFile(t *testing.T) {

	duration := time.Duration(3) * time.Millisecond

	// Straight case

	lf := NewLockFile(lfdir+"/test1.lck", duration)

	if err := lf.Start(); err != nil {
		t.Error(err)
		return
	}

	if err := lf.Finish(); err != nil {
		t.Error(err)
		return
	}

	// Simulate 2 process opening the same lockfile

	lf1 := &LockFile{lfdir + "/test2.lck", 1, duration, nil, false}
	if err := lf1.Start(); err != nil {
		t.Error(err)
		return
	}

	lf2 := &LockFile{lfdir + "/test2.lck", 2, duration, nil, false}
	if err := lf2.Start(); err == nil {
		t.Error("Unexpected result while starting lockfile watch:", err)
		return
	}

	if err := lf1.Finish(); err != nil {
		t.Error(err)
		return
	}

	// Test error cases

	lf3 := &LockFile{lfdir + "/" + invalidFileName, 1, duration, nil, false}
	if err := lf3.Start(); err == nil {
		t.Error("Unexpected result while starting lockfile watch:", err)
		return
	}

	lf = &LockFile{lfdir + "/test3.lck", 1, duration, nil, false}
	if err := lf.Start(); err != nil {
		t.Error(err)
		return
	}

	// Calling start twice should have no effect

	if err := lf.Start(); err != nil {
		t.Error(err)
		return
	}

	lf.filename = lfdir + "/" + invalidFileName

	for lf.WatcherRunning() {
		time.Sleep(lf.interval * 2)
	}

	if lf.WatcherRunning() {
		t.Error("Watcher is still running")
		return
	}

	if err := lf.Finish(); err == nil {
		t.Error("Unexpected finish result")
		return
	}

	file, err := os.OpenFile(lfdir+"/test4.lck", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0660)
	if err != nil {
		t.Error(err)
		return
	}
	file.Write(make([]byte, 3))
	file.Close()

	lf = &LockFile{lfdir + "/test4.lck", 1, duration, nil, false}
	if _, err := lf.checkLockfile(); err == nil || err.Error() != "Unexpected timestamp value found in lockfile:[0 0 0 0 0 0 0 0]" {
		t.Error("Unexpected checkLockfile result:", err)
		return
	}
}
