package logutil

import (
	"bytes"
	"strings"
	"testing"
)

func TestFormatting(t *testing.T) {
	ClearLogSinks()

	sf := TemplateFormatter("%t [%l] %s %m")

	sf.(*templateFormatter).tsFunc = func() string {
		return "0000000000000" // Timestamp for testing is always 0
	}

	rootBuf := &bytes.Buffer{}
	logger := GetLogger("")

	logger.AddLogSink(Debug, sf, rootBuf)

	logger.Info("foo")
	logger.Warning("bar")

	if rootBuf.String() != `
0000000000000 [Info]  foo
0000000000000 [Warning]  bar
`[1:] {
		t.Error("Unexpected output:", rootBuf.String())
		return
	}

	ClearLogSinks()

	sf = TemplateFormatter("%c - %m")

	sf.(*templateFormatter).tsFunc = func() string {
		return "0000000000000" // Timestamp for testing is always 0
	}

	rootBuf = &bytes.Buffer{}
	logger = GetLogger("")

	logger.AddLogSink(Debug, sf, rootBuf)

	logger.Info("foo")
	logger.Warning("bar")

	if !strings.Contains(rootBuf.String(), "formatter_test.go:47") {
		t.Error("Unexpected output:", rootBuf.String())
		return
	}

	ClearLogSinks()

	rootBuf = &bytes.Buffer{}
	logger = GetLogger("")

	logger.AddLogSink(Debug, ConsoleFormatter(), rootBuf)

	logger.Info("foo")
	logger.Warning("bar")

	if rootBuf.String() != `
Info: foo
Warning: bar
`[1:] {
		t.Error("Unexpected output:", rootBuf.String())
		return
	}
}
