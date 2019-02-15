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
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

const BUFTESTPATH = "filebuftestpath"

func TestMultiFileBufferErrors(t *testing.T) {

	buf, err := NewMultiFileBuffer("**"+string(0x0),
		ConsecutiveNumberIterator(5), EmptyRolloverCondition())

	if buf != nil || err == nil {
		t.Error("Unexpected result:", buf, err)
		return
	}
}

func TestMultiFileBufferDateDailyDate(t *testing.T) {
	os.RemoveAll(BUFTESTPATH)
	os.Mkdir(BUFTESTPATH, 0770)
	defer func() {
		if res, _ := PathExists(BUFTESTPATH); res {
			os.RemoveAll(BUFTESTPATH)
		}
	}()

	filename := path.Join(BUFTESTPATH, "testdate.log")

	it := DailyDateIterator(-1, 2) // No limit on files

	// Fix the today day

	it.(*dailyDateIterator).tsFunc = func() string {
		return "512800001234" // 1986-04-02
	}

	buf, err := NewMultiFileBuffer(filename,
		it, SizeBasedRolloverCondition(3))

	if err != nil {
		t.Error(err)
		return
	}

	buf.Write([]byte("a"))
	buf.Write([]byte("b"))
	buf.Write([]byte("c"))

	if err = checkDirLayout(BUFTESTPATH, map[string]string{
		"testdate.log.1986-04-02": "abc",
	}); err != nil {
		t.Error(err)
		return
	}

	buf.Close()

	// Create a new buffer

	buf, err = NewMultiFileBuffer(filename,
		it, SizeBasedRolloverCondition(3))

	if err != nil {
		t.Error(err)
		return
	}

	buf.Write([]byte("d"))
	buf.Write([]byte("e"))
	buf.Write([]byte("fg"))
	buf.Write([]byte("h"))

	if err = checkDirLayout(BUFTESTPATH, map[string]string{
		"testdate.log.1986-04-02":   "h",
		"testdate.log.1986-04-02.1": "defg",
		"testdate.log.1986-04-02.2": "abc",
	}); err != nil {
		t.Error(err)
		return
	}

	buf.Close()

	// A new day

	it.(*dailyDateIterator).tsFunc = func() string {
		return "512900001234" // 1986-04-03
	}

	buf.Write([]byte("123"))
	buf.Write([]byte("4"))

	if err = checkDirLayout(BUFTESTPATH, map[string]string{
		"testdate.log.1986-04-03":   "4",
		"testdate.log.1986-04-03.1": "123",
		"testdate.log.1986-04-02":   "h",
		"testdate.log.1986-04-02.1": "defg",
		"testdate.log.1986-04-02.2": "abc",
	}); err != nil {
		t.Error(err)
		return
	}

	buf.Close()

	// Test cleanup - Move months into the future

	it.(*dailyDateIterator).tsFunc = func() string {
		return "522800001234" // 1986-07-26
	}

	buf.Write([]byte("x"))

	if err = checkDirLayout(BUFTESTPATH, map[string]string{
		"testdate.log.1986-07-26":   "x",
		"testdate.log.1986-04-03":   "4",
		"testdate.log.1986-04-03.1": "123",
	}); err != nil {
		t.Error(err)
		return
	}

	buf.Close()

	// Last test writer without restriction

	buf, err = NewMultiFileBuffer(filename,
		it, EmptyRolloverCondition())

	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < 10; i++ {
		buf.Write([]byte("x"))
	}

	if err = checkDirLayout(BUFTESTPATH, map[string]string{
		"testdate.log.1986-07-26":   "xxxxxxxxxxx",
		"testdate.log.1986-04-03":   "4",
		"testdate.log.1986-04-03.1": "123",
	}); err != nil {
		t.Error(err)
		return
	}

	buf.Close()

	// Write into a closed file

	for i := 0; i < 10; i++ {
		buf.Write([]byte("x"))
	}

	if err = checkDirLayout(BUFTESTPATH, map[string]string{
		"testdate.log.1986-07-26":   "xxxxxxxxxxxxxxxxxxxxx",
		"testdate.log.1986-04-03":   "4",
		"testdate.log.1986-04-03.1": "123",
	}); err != nil {
		t.Error(err)
		return
	}

	buf.Close()
}

func TestMultiFileBufferSimpleNumbering(t *testing.T) {
	os.RemoveAll(BUFTESTPATH)
	os.Mkdir(BUFTESTPATH, 0770)
	defer func() {
		if res, _ := PathExists(BUFTESTPATH); res {
			os.RemoveAll(BUFTESTPATH)
		}
	}()

	filename := path.Join(BUFTESTPATH, "test1.log")

	buf, err := NewMultiFileBuffer(filename,
		ConsecutiveNumberIterator(3), SizeBasedRolloverCondition(4))

	if err != nil {
		t.Error(err)
		return
	}

	buf.Write([]byte("a"))
	buf.Write([]byte("b"))

	if err = checkDirLayout(BUFTESTPATH, map[string]string{
		"test1.log": "ab",
	}); err != nil {
		t.Error(err)
		return
	}

	buf.Close()

	// Create a new buffer

	buf, err = NewMultiFileBuffer(filename,
		ConsecutiveNumberIterator(3), SizeBasedRolloverCondition(4))

	if err != nil {
		t.Error(err)
		return
	}

	buf.Write([]byte("c"))
	buf.Write([]byte("d"))

	if err = checkDirLayout(BUFTESTPATH, map[string]string{
		"test1.log": "abcd",
	}); err != nil {
		t.Error(err)
		return
	}

	// Now fill up the files

	for i := 0; i < 10; i++ {
		if _, err := buf.Write([]byte(fmt.Sprint(i))); err != nil {
			t.Error(err)
			return
		}
	}

	if err = checkDirLayout(BUFTESTPATH, map[string]string{
		"test1.log":   "89",
		"test1.log.1": "4567",
		"test1.log.2": "0123",
		"test1.log.3": "abcd",
	}); err != nil {
		t.Error(err)
		return
	}

	// Fill up some more and see that the oldest entries disappear

	buf.Write([]byte("xxx"))

	for i := 0; i < 7; i++ {
		if _, err := buf.Write([]byte(fmt.Sprint(i))); err != nil {
			t.Error(err)
			return
		}
	}

	if err = checkDirLayout(BUFTESTPATH, map[string]string{
		"test1.log":   "456",
		"test1.log.1": "0123",
		"test1.log.2": "89xxx",
		"test1.log.3": "4567",
	}); err != nil {
		t.Error(err)
		return
	}

	buf.Close()

	// Create a new buffer

	buf, err = NewMultiFileBuffer(filename,
		ConsecutiveNumberIterator(3), SizeBasedRolloverCondition(4))

	for i := 0; i < 4; i++ {
		if _, err := buf.Write([]byte(fmt.Sprint(i))); err != nil {
			t.Error(err)
			return
		}
	}

	if err = checkDirLayout(BUFTESTPATH, map[string]string{
		"test1.log":   "123",
		"test1.log.1": "4560",
		"test1.log.2": "0123",
		"test1.log.3": "89xxx",
	}); err != nil {
		t.Error(err)
		return
	}

	buf.Close()
}

func checkDirLayout(dir string, expected map[string]string) error {

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	if len(files) != len(expected) {

		foundFiles := make([]string, 0, len(files))
		for _, f := range files {
			foundFiles = append(foundFiles, f.Name())
		}

		return fmt.Errorf("Unexpected layout found files: %v", foundFiles)
	}

	for _, f := range files {
		content, err := ioutil.ReadFile(path.Join(dir, f.Name()))
		if err != nil {
			return err
		}
		expectedContent, ok := expected[f.Name()]
		if !ok {
			return fmt.Errorf("File %v not in list of expected files", f.Name())
		}

		if expectedContent != string(content) {
			return fmt.Errorf("Content of file %v is not as expected: %v",
				f.Name(), string(content))
		}
	}

	return nil
}

func TestConsecutiveNumberIterator(t *testing.T) {

	it := ConsecutiveNumberIterator(5)

	if res := it.NextName("foo"); res != "foo.1" {
		t.Error("Unexpected result: ", res)
		return
	}

	if res := it.NextName("foo.1"); res != "foo.2" {
		t.Error("Unexpected result: ", res)
		return
	}

	if res := it.NextName("foo.4"); res != "foo.5" {
		t.Error("Unexpected result: ", res)
		return
	}

	if res := it.NextName("foo.5"); res != "" {
		t.Error("Unexpected result: ", res)
		return
	}
}

func TestDailyDateIterator(t *testing.T) {

	it := DailyDateIterator(-1, -1)
	it.(*dailyDateIterator).tsFunc = func() string {
		return "512800001234" // 1986-04-02
	}

	filename := "foo"

	basename := it.Basename(filename)

	if res := it.NextName(basename); res != "foo.1986-04-02.1" {
		t.Error("Unexpected result: ", res)
		return
	}

	it.(*dailyDateIterator).tsFunc = func() string {
		return "522800001234" // 1986-07-26
	}

	basename = it.Basename(filename)

	if res := it.NextName(basename + ".51"); res != "foo.1986-07-26.52" {
		t.Error("Unexpected result: ", res)
		return
	}
}
