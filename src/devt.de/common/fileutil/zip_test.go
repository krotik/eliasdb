package fileutil

import (
	"encoding/base64"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
)

var testZipFile = "UEsDBBQAAAAAAAlhM0sAAAAAAAAAAAAAAAALAAAAdGVzdGZvbGRlci" +
	"9QSwMECgAAAAAA/WAzS9JjSIgDAAAAAwAAABQAAAB0ZXN0Zm9sZGVyL3Rlc3QxLnR4dDEyM1" +
	"BLAwQKAAAAAAAMYTNLccOosQMAAAADAAAAFAAAAHRlc3Rmb2xkZXIvdGVzdDIudHh0NDU2UE" +
	"sBAj8AFAAAAAAACWEzSwAAAAAAAAAAAAAAAAsAJAAAAAAAAAAQAAAAAAAAAHRlc3Rmb2xkZX" +
	"IvCgAgAAAAAAABABgAynC8mDcx0wG6nMOYNzHTAcpwvJg3MdMBUEsBAj8ACgAAAAAA/WAzS9" +
	"JjSIgDAAAAAwAAABQAJAAAAAAAAAAgAAAAKQAAAHRlc3Rmb2xkZXIvdGVzdDEudHh0CgAgAA" +
	"AAAAABABgAAgkxjDcx0wFqBhKVNzHTAQIJMYw3MdMBUEsBAj8ACgAAAAAADGEzS3HDqLEDAA" +
	"AAAwAAABQAJAAAAAAAAAAgAAAAXgAAAHRlc3Rmb2xkZXIvdGVzdDIudHh0CgAgAAAAAAABAB" +
	"gArtRMnDcx0wE68M6gNzHTAXrDTJw3MdMBUEsFBgAAAAADAAMAKQEAAJMAAAAAAA=="

func TestUnzipFile(t *testing.T) {

	data, _ := base64.StdEncoding.DecodeString(testZipFile)

	ioutil.WriteFile("ziptest.zip", data, 0660)
	ioutil.WriteFile("ziptest2.zip", data[:5], 0660)

	defer func() {
		os.Remove("ziptest.zip")
		os.Remove("ziptest2.zip")
		os.RemoveAll("foo")
	}()

	if err := UnzipFile("ziptest.zip", "foo", false); err != nil {
		t.Error(err)
		return
	}

	if err := UnzipFile("ziptest.zip", "foo", false); !strings.Contains(err.Error(), "Path already exists:") {
		t.Error(err)
		return
	}

	if err := UnzipFile("ziptest2.zip", "foo", false); err.Error() != "zip: not a valid zip file" {
		t.Error(err)
		return
	}

	if e, err := PathExists("foo"); !e {
		t.Error("Unexpected result:", e, err)
		return
	}

	if e, err := PathExists(path.Join("foo", "testfolder")); !e {
		t.Error("Unexpected result:", e, err)
		return
	}

	if e, err := PathExists(path.Join("foo", "testfolder", "test1.txt")); !e {
		t.Error("Unexpected result:", e, err)
		return
	}

	if e, err := PathExists(path.Join("foo", "testfolder", "test2.txt")); !e {
		t.Error("Unexpected result:", e, err)
		return
	}

	if c, err := ioutil.ReadFile(path.Join("foo", "testfolder", "test1.txt")); string(c) != "123" {
		t.Error("Unexpected result:", string(c), err)
		return
	}

	if c, err := ioutil.ReadFile(path.Join("foo", "testfolder", "test2.txt")); string(c) != "456" {
		t.Error("Unexpected result:", string(c), err)
		return
	}
}
