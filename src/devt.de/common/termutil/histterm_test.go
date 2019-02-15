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
	"io/ioutil"
	"os"
	"testing"

	"devt.de/common/termutil/getch"
)

func TestHistoryConsoleLineTerminal(t *testing.T) {
	var out bytes.Buffer

	DefaultHistoryBufferSize = 3 // Set very small default history size

	// Setup mock getch

	getchStart = func() error { return nil }

	var getchbuffer []*getch.KeyEvent
	addTestKeyEvent := func(kc getch.KeyCode, r rune) {
		getchbuffer = append(getchbuffer, &getch.KeyEvent{
			Code: kc,
			Rune: r,
		})
	}

	getchGetch = func() (*getch.KeyEvent, error) {
		e := getchbuffer[0]
		getchbuffer = getchbuffer[1:]
		return e, nil
	}

	ct, err := NewConsoleLineTerminal(&out)

	if err != nil {
		t.Error(err)
		return
	}

	// Wrap the console terminal in a History Mixin

	ct, err = AddHistoryMixin(ct, "testhistory", func(l string) bool {
		return l == "exit"
	})

	if err != nil {
		t.Error(err)
		return
	}

	defer os.Remove("testhistory")

	addTestKeyEvent(getch.KeyT, 't')
	addTestKeyEvent(getch.KeyE, 'e')
	addTestKeyEvent(getch.KeyS, 's')
	addTestKeyEvent(getch.KeyT, 't')
	addTestKeyEvent(getch.KeyEnter, 0x00)

	if line, err := ct.NextLine(); err != nil || line != "test" {
		t.Error("Unexpected result:", "#"+line+"#", err)
		return
	}

	// Test repeat previous line

	addTestKeyEvent(getch.KeyArrowUp, 0x00)
	addTestKeyEvent(getch.KeyEnter, 0x00)

	if line, err := ct.NextLine(); err != nil || line != "test" {
		t.Error("Unexpected result:", "#"+line+"#", err)
		return
	}

	addTestKeyEvent(getch.Key1, '1')
	addTestKeyEvent(getch.KeyEnter, 0x00)
	ct.NextLine()

	addTestKeyEvent(getch.Key1, '2')
	addTestKeyEvent(getch.KeyEnter, 0x00)
	ct.NextLine()

	// Next line should be ignored

	addTestKeyEvent(getch.KeyT, 'e')
	addTestKeyEvent(getch.KeyE, 'x')
	addTestKeyEvent(getch.KeyS, 'i')
	addTestKeyEvent(getch.KeyT, 't')
	addTestKeyEvent(getch.KeyEnter, 0x00)

	if line, err := ct.NextLine(); err != nil || line != "exit" {
		t.Error("Unexpected result:", "#"+line+"#", err)
		return
	}

	addTestKeyEvent(getch.Key1, '3')
	addTestKeyEvent(getch.KeyEnter, 0x00)
	ct.NextLine()

	// Check that the first lines have fallen off

	if res, _ := ioutil.ReadFile("testhistory"); string(res) != `1
2
3` {
		t.Error("Unexpected history:", string(res))
		return
	}

	ct.StopTerm()

	// Recreate the terminal

	ct, err = NewConsoleLineTerminal(&out)

	if err != nil {
		t.Error(err)
		return
	}

	// Wrap the console terminal in a History Mixin

	ct, err = AddHistoryMixin(ct, "testhistory", func(l string) bool {
		return l == "exit"
	})

	if err != nil {
		t.Error(err)
		return
	}

	// Check we have history entries

	addTestKeyEvent(getch.KeyArrowUp, 0x00)
	addTestKeyEvent(getch.KeyArrowUp, 0x00)
	addTestKeyEvent(getch.KeyArrowUp, 0x00)
	addTestKeyEvent(getch.KeyArrowDown, 0x00)
	addTestKeyEvent(getch.KeyEnter, 0x00)

	if line, err := ct.NextLine(); err != nil || line != "2" {
		t.Error("Unexpected result:", "#"+line+"#", err)
		return
	}

	addTestKeyEvent(getch.KeyArrowUp, 0x00)
	addTestKeyEvent(getch.KeyArrowUp, 0x00)
	addTestKeyEvent(getch.KeyEnter, 0x00)

	if line, err := ct.NextLine(); err != nil || line != "3" {
		t.Error("Unexpected result:", "#"+line+"#", err)
		return
	}

	addTestKeyEvent(getch.KeyT, 't')
	addTestKeyEvent(getch.KeyE, 'e')

	addTestKeyEvent(getch.KeyArrowUp, 0x00)
	addTestKeyEvent(getch.KeyArrowUp, 0x00)
	addTestKeyEvent(getch.KeyArrowDown, 0x00)
	addTestKeyEvent(getch.KeyArrowDown, 0x00)
	addTestKeyEvent(getch.KeyEnter, 0x00)

	if line, err := ct.NextLine(); err != nil || line != "te" {
		t.Error("Unexpected result:", "#"+line+"#", err)
		return
	}

	ct.StopTerm()
}
