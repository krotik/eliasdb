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

func TestAutoCompleteConsoleLineTerminal(t *testing.T) {
	var out bytes.Buffer

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

	rootDict := NewWordListDict([]string{"ll", "dir", "get", "put", "test", "test1", "test2"})

	chooser := func(lineWords []string, dictCache map[string]Dict) (Dict, error) {

		if len(lineWords) == 1 {
			return rootDict, nil
		}

		return NewWordListDict([]string{fmt.Sprintf("file4-%v", len(lineWords)), "file2", "file3", "file1", "test"}), nil
	}

	dict := NewMultiWordDict(chooser, nil)

	// Wrap the console terminal in a Auto Complete Mixin

	ct, err = AddAutoCompleteMixin(ct, dict)

	if err != nil {
		t.Error(err)
		return
	}

	// Test normal auto complete

	addTestKeyEvent(getch.KeyT, 'd')
	addTestKeyEvent(getch.KeyTab, 0x00)
	addTestKeyEvent(getch.KeyEnter, 0x00)

	if line, err := ct.NextLine(); err != nil || line != "dir " {
		t.Error("Unexpected result:", "#"+line+"#", err)
		return
	}

	addTestKeyEvent(getch.KeyT, 'd')
	addTestKeyEvent(getch.KeyTab, 0x00)
	addTestKeyEvent(getch.KeyT, 't')
	addTestKeyEvent(getch.KeyTab, 0x00)
	addTestKeyEvent(getch.KeyEnter, 0x00)

	if line, err := ct.NextLine(); err != nil || line != "dir test " {
		t.Error("Unexpected result:", "#"+line+"#", err)
		return
	}

	// Test auto complete with multiple suggestion and picking one by pressing tab

	addTestKeyEvent(getch.KeyT, 't')
	addTestKeyEvent(getch.KeyE, 'e')
	addTestKeyEvent(getch.KeyTab, 0x00) // Auto complete to test
	addTestKeyEvent(getch.KeyTab, 0x00) // See suggestions ("test", "test1", "test2"s)
	addTestKeyEvent(getch.KeyTab, 0x00) // Produce final space - "test" was accepted
	addTestKeyEvent(getch.KeyEnter, 0x00)

	if line, err := ct.NextLine(); err != nil || line != "test " {
		t.Error("Unexpected result:", "#"+line+"#", err)
		return
	}

	// Check second level suggestion

	addTestKeyEvent(getch.KeyT, 't')
	addTestKeyEvent(getch.KeyE, 'e')
	addTestKeyEvent(getch.KeyTab, 0x00)
	addTestKeyEvent(getch.KeyE, ' ')
	addTestKeyEvent(getch.KeyT, 'f')
	addTestKeyEvent(getch.KeyTab, 0x00) // No effect since there is no "file"
	addTestKeyEvent(getch.KeyTab, 0x00)
	addTestKeyEvent(getch.KeyTab, 0x00)
	addTestKeyEvent(getch.KeyT, '1')
	addTestKeyEvent(getch.KeyTab, 0x00)
	addTestKeyEvent(getch.KeyEnter, 0x00)

	if line, err := ct.NextLine(); err != nil || line != "test file1 " {
		t.Error("Unexpected result:", "#"+line+"#", err)
		return
	}

}

func TestWordListDict(t *testing.T) {

	wlist := []string{"bar", "foo", "test", "test1", "test2", "test3", "zanas"}
	wld := NewWordListDict(wlist)

	if res, _ := wld.Suggest("zanas"); fmt.Sprint(res) != "[zanas]" {
		t.Error("Unexpected result:", res)
		return
	}

	if res, _ := wld.Suggest("zan"); fmt.Sprint(res) != "[zanas]" {
		t.Error("Unexpected result:", res)
		return
	}

	if res, _ := wld.Suggest("zap"); fmt.Sprint(res) != "[]" {
		t.Error("Unexpected result:", res)
		return
	}

	if res, _ := wld.Suggest("t"); fmt.Sprint(res) != "[test test1 test2 test3]" {
		t.Error("Unexpected result:", res)
		return
	}

	if res, _ := wld.Suggest("test"); fmt.Sprint(res) != "[test test1 test2 test3]" {
		t.Error("Unexpected result:", res)
		return
	}

	if res, _ := wld.Suggest("b"); fmt.Sprint(res) != "[bar]" {
		t.Error("Unexpected result:", res)
		return
	}

	// Special case of empty dictionary

	wld = NewWordListDict([]string{})

	if res, _ := wld.Suggest("b"); fmt.Sprint(res) != "[]" {
		t.Error("Unexpected result:", res)
		return
	}
}

func TestMultiWordDict(t *testing.T) {

	rootDict := NewWordListDict([]string{"bar", "foo"})

	md := NewMultiWordDict(func(p []string, c map[string]Dict) (Dict, error) {
		var dict Dict
		var ok bool

		if p[0] == "" {
			return nil, nil
		}

		if p[0] == "foo" {
			return nil, fmt.Errorf("Testerror")
		}

		if dict, ok = c[p[0]]; !ok {
			dict = rootDict
		}

		return dict, nil
	}, nil)

	md.dicts["bar"] = NewWordListDict([]string{"bar", "foo", "test", "test1", "test2", "test3", "zanas"})

	if res, err := md.Suggest(""); err != nil || fmt.Sprint(res) != "[]" {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, err := md.Suggest("f"); err != nil || fmt.Sprint(res) != "[foo]" {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, err := md.Suggest("foo"); err == nil || err.Error() != "Testerror" {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, err := md.Suggest("b"); err != nil || fmt.Sprint(res) != "[bar]" {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, err := md.Suggest("bar"); err != nil || fmt.Sprint(res) != "[bar]" {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, err := md.Suggest("bar "); err != nil || fmt.Sprint(res) != "[bar foo test test1 test2 test3 zanas]" {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, err := md.Suggest("bar b"); err != nil || fmt.Sprint(res) != "[bar]" {
		t.Error("Unexpected result:", res, err)
		return
	}

	if res, err := md.Suggest("bar test"); err != nil || fmt.Sprint(res) != "[test test1 test2 test3]" {
		t.Error("Unexpected result:", res, err)
		return
	}
}
