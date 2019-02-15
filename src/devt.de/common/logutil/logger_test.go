/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package logutil

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

type brokenSink struct {
}

func (bs *brokenSink) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("testerror")
}

func TestLogging(t *testing.T) {

	if StringToLoglevel("iNfO") != Info {
		t.Error("Unexpected result")
		return
	}

	ClearLogSinks()

	sf := SimpleFormatter()

	sf.(*simpleFormatter).tsFunc = func() string {
		return "0000000000000" // Timestamp for testing is always 0
	}

	// Test straight forward case doing root logging

	rootBuf := &bytes.Buffer{}
	logger := GetLogger("")

	logger.AddLogSink(Debug, sf, rootBuf)

	logger.Info("foo")
	logger.Warning("bar")

	if rootBuf.String() != `
0000000000000 Info foo
0000000000000 Warning bar
`[1:] {
		t.Error("Unexpected output:", rootBuf.String())
		return
	}

	logger.LogStackTrace(Error, "test123")
	logger.Warning("next")

	if !strings.Contains(rootBuf.String(), "logger_test.go") {
		t.Error("Unexpected output:", rootBuf.String())
		return
	}

	rootBuf.Reset()

	logger.Info("foo")
	logger.Warning("bar")

	// Add a sub package logger

	subBuf := &bytes.Buffer{}
	logger = GetLogger("foo")

	logger.AddLogSink(Info, sf, subBuf)

	logger.Debug("debugmsg")
	logger.Info("foo")
	logger.Warning("bar")

	// Debug message was handled in root logger

	if rootBuf.String() != `
0000000000000 Info foo
0000000000000 Warning bar
0000000000000 Debug foo debugmsg
`[1:] {
		t.Error("Unexpected output:", rootBuf.String())
		return
	}

	// Info and warning where handled in the sub logger

	if subBuf.String() != `
0000000000000 Info foo foo
0000000000000 Warning foo bar
`[1:] {
		t.Error("Unexpected output:", subBuf.String())
		return
	}

	// Add a sub sub package logger

	subsubBuf := &bytes.Buffer{}
	logger = GetLogger("foo.bar")

	//  Add the logger twice

	logger.AddLogSink(Error, sf, subsubBuf)
	logger.AddLogSink(Error, sf, subsubBuf)

	logger = GetLogger("foo.bar.bla")

	logger.Error("test1")
	logger.Info("test2")
	logger.Debug("test3")

	// Check that the messages were distributed correctly

	if rootBuf.String() != `
0000000000000 Info foo
0000000000000 Warning bar
0000000000000 Debug foo debugmsg
0000000000000 Debug foo.bar.bla test3
`[1:] {
		t.Error("Unexpected output:", rootBuf.String())
		return
	}

	if subBuf.String() != `
0000000000000 Info foo foo
0000000000000 Warning foo bar
0000000000000 Info foo.bar.bla test2
`[1:] {
		t.Error("Unexpected output:", subBuf.String())
		return
	}

	// Log message is duplicated as we have the same sink twice

	if subsubBuf.String() != `
0000000000000 Error foo.bar.bla test1
0000000000000 Error foo.bar.bla test1
`[1:] {
		t.Error("Unexpected output:", subsubBuf.String())
		return
	}

	// Remove all log sinks and test error cases

	ClearLogSinks()

	fallbackBuf := &bytes.Buffer{}
	fallbackLogger = func(v ...interface{}) {
		fallbackBuf.WriteString(fmt.Sprint(v...))
	}

	logger = GetLogger("foo.bar.bla")

	logger.Error("test1")

	if !strings.Contains(fallbackBuf.String(), "Error foo.bar.bla test1") {
		t.Error("Unexpected output:", fallbackBuf.String())
		return
	}

	logger = GetLogger("foo.bar")

	logger.AddLogSink(Info, sf, &brokenSink{})

	logger.Info("test")

	if !strings.Contains(fallbackBuf.String(), "testerror") {
		t.Error("Unexpected output:", fallbackBuf.String())
		return
	}
}
