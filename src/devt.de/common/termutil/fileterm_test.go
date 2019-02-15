/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package termutil

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"devt.de/common/termutil/getch"
)

func TestFileReadingTerminal(t *testing.T) {
	var ct ConsoleLineTerminal
	var err error
	var out bytes.Buffer

	// Take over the low-level input interface

	stdin = bytes.NewBufferString("test line\n")

	// First test the fallback mode

	getchStart = func() error { return fmt.Errorf("Something is wrong") }
	getchGetch = func() (*getch.KeyEvent, error) { return nil, fmt.Errorf("baa") }

	// Write a line file

	ioutil.WriteFile("test.cmd", []byte(`Test1
Test2
TestEnd
`), 0666)
	defer os.Remove("test.cmd")

	// Create a basic terminal

	ct, err = NewConsoleLineTerminal(&out)
	if err != nil {
		t.Error("Console terminal should go into fallback without error:", err)
		return
	}

	// Wrap the terminal in a file reading terminal

	file, _ := os.Open("test.cmd")
	defer file.Close()

	ct, err = AddFileReadingWrapper(ct, file, false)
	if err != nil {
		t.Error(err)
		return
	}

	ct.StartTerm()
	defer ct.StopTerm()

	if l, err := ct.NextLine(); err != nil || l != "Test1" {
		t.Error("Unexpected result:", l, err)
		return
	}

	if l, err := ct.NextLine(); err != nil || l != "Test2" {
		t.Error("Unexpected result:", l, err)
		return
	}

	if l, err := ct.NextLine(); err != nil || l != "TestEnd" {
		t.Error("Unexpected result:", l, err)
		return
	}

	if l, err := ct.NextLine(); err != nil || l != "test line" {
		t.Error("Unexpected result:", l, err)
		return
	}

	if l, err := ct.NextLine(); err != nil || l != "" {
		t.Error("Unexpected result:", l, err)
		return
	}

	ct.(*filereadingTerminalMixin).termOnEOF = true

	if l, err := ct.NextLine(); err != nil || l != "\x04" {
		t.Error("Unexpected result:", l, err)
		return
	}
}
