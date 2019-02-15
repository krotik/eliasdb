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
	"testing"

	"devt.de/common/termutil/getch"
)

func TestConsoleLineTerminal(t *testing.T) {
	var out bytes.Buffer

	// Take over the low-level input interface

	stdin = bytes.NewBufferString("test line\n")

	// First test the fallback mode

	getchStart = func() error { return fmt.Errorf("Something is wrong") }
	getchGetch = func() (*getch.KeyEvent, error) { return nil, fmt.Errorf("baa") }

	ct, err := NewConsoleLineTerminal(&out)

	if err != nil {
		t.Error("Console terminal should go into fallback without error:", err)
		return
	}

	ct.StartTerm()

	if !ct.(*consoleLineTerminal).fallback {
		t.Error("Console terminal should be in fallback mode")
		return
	}

	l, err := ct.NextLine()

	if err != nil || l != "test line" {
		t.Error("Unexpected result:", l, err)
		return
	}

	l, err = ct.NextLinePrompt("", '*')

	if err == nil || err.Error() != "Cannot mask input characters" {
		t.Error("Unexpected result:", l, err)
		return
	}

	// Now do the proper getch supported backend

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
		if e.Code == getch.KeyQuote { // Special case for tests
			return nil, &getch.ErrUnknownEscapeSequence{}
		}
		return e, nil
	}

	if ct, err = NewConsoleLineTerminal(&out); err != nil {
		t.Error(err)
		return
	}

	if ct.(*consoleLineTerminal).fallback {
		t.Error("Console terminal should NOT be in fallback mode")
		return
	}

	// Now do a normal line parsing

	addTestKeyEvent(getch.KeyT, 't')
	addTestKeyEvent(getch.KeyE, 'e')
	addTestKeyEvent(getch.KeyS, 's')
	addTestKeyEvent(getch.KeyT, 't')
	addTestKeyEvent(getch.KeyEnter, 0x00)

	if line, err := ct.NextLine(); err != nil || line != "test" {
		t.Error("Unexpected result:", "#"+line+"#", err)
		return
	}

	if res := string(out.Bytes()[60:67]); res != `>>>test` {
		t.Error("Unexpected result:", "#"+res+"#")
		return
	}

	out.Reset()

	// Now do a line parsing with masked line

	addTestKeyEvent(getch.KeyT, 't')
	addTestKeyEvent(getch.KeyE, 'e')
	addTestKeyEvent(getch.KeyS, 's')
	addTestKeyEvent(getch.KeyT, 't')
	addTestKeyEvent(getch.KeyEnter, 0x00)

	if line, err := ct.NextLinePrompt(">", '*'); err != nil || line != "test" {
		t.Error("Unexpected result:", "#"+line+"#", err)
		return
	}

	if res := string(out.Bytes()[39:44]); res != `>****` {
		t.Error("Unexpected result:", "#"+res+"#")
		return
	}

	// Now do a line parsing with pasted text

	addTestKeyEvent(getch.KeyT, 't')
	addTestKeyEvent(getch.KeyE, 'e')
	addTestKeyEvent(getch.KeyS, 's')
	addTestKeyEvent(getch.KeyT, 't')
	addTestKeyEvent("", 0x0)
	addTestKeyEvent(getch.KeyEnter, 0x00)

	getchbuffer[2].RawBuf = []byte("s1s")

	if line, err := ct.NextLine(); err != nil || line != "tes1st " {
		t.Error("Unexpected result:", "#"+line+"#", err)
		return
	}

	// Test backspace

	addTestKeyEvent(getch.KeyT, 't')
	addTestKeyEvent(getch.KeyE, 'e')
	addTestKeyEvent(getch.KeyS, 's')
	addTestKeyEvent(getch.KeyT, 't')
	addTestKeyEvent(getch.KeyBackspace, 0x0)
	addTestKeyEvent(getch.KeyBackspace, 0x0)
	addTestKeyEvent(getch.KeyEnter, 0x00)

	if line, err := ct.NextLine(); err != nil || line != "te" {
		t.Error("Unexpected result:", "#"+line+"#", err)
		return
	}

	// Test cursor movement and delete

	addTestKeyEvent(getch.KeyT, 't')
	addTestKeyEvent(getch.KeyQuote, 0x00) // Generates an unknown escape sequence error
	addTestKeyEvent(getch.KeyE, 'e')
	addTestKeyEvent(getch.KeyS, 's')
	addTestKeyEvent(getch.KeyT, 't')
	addTestKeyEvent(getch.KeyArrowLeft, 0x0)
	addTestKeyEvent(getch.KeyArrowLeft, 0x0)
	addTestKeyEvent(getch.KeyDelete, 0x0)
	addTestKeyEvent(getch.KeyT, 'x')
	addTestKeyEvent(getch.KeyArrowRight, 0x0)
	addTestKeyEvent(getch.KeyArrowRight, 0x0)
	addTestKeyEvent(getch.KeyT, 'e')
	addTestKeyEvent(getch.KeyT, 'r')
	addTestKeyEvent(getch.KeyHome, 0x0)
	addTestKeyEvent(getch.KeyT, '-')
	addTestKeyEvent(getch.KeyEnd, 0x0)
	addTestKeyEvent(getch.KeyT, '-')

	addTestKeyEvent(getch.KeyEnter, 0x00)

	// The unknown escape sequence error should have been ignored

	if line, err := ct.NextLine(); err != nil || line != "-texter-" {
		t.Error("Unexpected result:", "#"+line+"#", err)
		return
	}

	// Calling the actual stop function in getch should always be ok

	ct.StopTerm()
}
