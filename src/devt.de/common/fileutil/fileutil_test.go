/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package fileutil

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

const TESTPATH = "fileutiltestpath"

func TestDirectoryExists(t *testing.T) {
	os.Remove(TESTPATH)

	res, err := PathExists(TESTPATH)
	if err != nil {
		t.Error(err.Error())
		return
	}
	if res {
		t.Error("Path test should not exist")
	}

	os.Mkdir(TESTPATH, 0770)
	defer func() {
		os.RemoveAll(TESTPATH)
	}()

	res, err = PathExists(TESTPATH)
	if err != nil {
		t.Error(err.Error())
		return
	}
	if !res {
		t.Error("Path test should exist after it was created")
		return
	}

	_, err = PathExists("**" + string(0x0))
	if err == nil {
		t.Error("Incorrect paths should throw an error")
		return
	}
}

func TestIsDir(t *testing.T) {
	os.Remove(TESTPATH)

	res, err := IsDir(TESTPATH)
	if err != nil && !os.IsNotExist(err) {
		t.Error(err.Error())
		return
	}
	if res {
		t.Error("Path test should not exist")
	}

	os.Mkdir(TESTPATH, 0770)
	defer func() {
		os.RemoveAll(TESTPATH)
	}()

	res, err = IsDir(TESTPATH)
	if err != nil {
		t.Error(err.Error())
		return
	}
	if !res {
		t.Error("Dir test should exist after it was created")
		return
	}

	_, err = IsDir("**" + string(0x0))
	if err == nil {
		t.Error("Incorrect paths should throw an error")
		return
	}
}

func TestCheckSumFiles(t *testing.T) {
	os.Remove(TESTPATH)

	res, err := IsDir(TESTPATH)
	if err != nil && !os.IsNotExist(err) {
		t.Error(err.Error())
		return
	}
	if res {
		t.Error("Path test should not exist")
	}

	os.Mkdir(TESTPATH, 0770)
	defer func() {
		os.RemoveAll(TESTPATH)
	}()

	testfile := filepath.Join(TESTPATH, "testfile.txt")

	ioutil.WriteFile(testfile, []byte("Omnium enim rerum\nprincipia parva sunt"), 0660)

	if res, err := CheckSumFile(testfile); res != "90a258b01ceab4058906318bf0b34a31f2ff7ac2268c7bf3df9168f1f6ca5bc6" || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}

	// Test fast checksum

	if res, err := CheckSumFileFast(testfile); res != "6f05b934" || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}

	testfile = filepath.Join(TESTPATH, "testfile2.txt")

	buf := make([]byte, fastSumSampleSize*8)
	for i := 0; i < fastSumSampleSize*8; i++ {
		buf[i] = byte(i % 10)
	}

	ioutil.WriteFile(testfile, buf, 0660)

	if res, err := CheckSumFileFast(testfile); res != "14294b07" || err != nil {
		t.Error("Unexpected result:", res, err)
		return
	}
}
