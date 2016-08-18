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
	"os"
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

	os.Mkdir(TESTPATH, 660)

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

	os.Remove(TESTPATH)
}

func TestIsDir(t *testing.T) {
	os.Remove(TESTPATH)

	res, err := IsDir(TESTPATH)
	if err == nil {
		t.Error(err.Error())
		return
	}
	if res {
		t.Error("Path test should not exist")
	}

	os.Mkdir(TESTPATH, 660)

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

	os.Remove(TESTPATH)
}
