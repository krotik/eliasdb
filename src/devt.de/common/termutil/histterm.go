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
	"bufio"
	"io/ioutil"
	"os"
	"strings"

	"devt.de/common/datautil"
	"devt.de/common/fileutil"
	"devt.de/common/stringutil"
	"devt.de/common/termutil/getch"
)

/*
DefaultHistoryBufferSize is the default history buffer size in lines
*/
var DefaultHistoryBufferSize = 100

/*
historyLineTerminalMixin adds history functionality to a given ConsoleLineTerminal
*/
type historyLineTerminalMixin struct {
	ConsoleLineTerminal                      // Terminal which is being extended
	histFile            string               // File containing the history
	history             *datautil.RingBuffer // Buffer containing the history
	historyPointer      int                  // Pointer into history buffer
	lastEntry           string               // Temporary storage for last entry
	ignoreLine          func(string) bool    // Ignore line function
}

/*
AddHistoryMixin adds history support for a given ConsoleLineTerminal. History
is collected with every line and persisted in a file. The user can scroll
through the history using the cursor keys up and down. The client can optionally
define a ignoreLine function which causes a line to be ignored if it returns true.
*/
func AddHistoryMixin(term ConsoleLineTerminal, histFile string,
	ignoreLine func(string) bool) (ConsoleLineTerminal, error) {

	var err error

	histterm := &historyLineTerminalMixin{term, histFile,
		datautil.NewRingBuffer(DefaultHistoryBufferSize), 0, "", ignoreLine}

	// Add key handler

	histterm.AddKeyHandler(histterm.handleKeyInput)

	if histFile != "" {
		if ok, err := fileutil.PathExists(histFile); err == nil && ok {
			var file *os.File

			// Read old history

			if file, err = os.Open(histFile); err == nil {
				defer file.Close()

				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					histterm.history.Add(scanner.Text())
				}

				histterm.historyPointer = histterm.history.Size()
			}
		}
	}

	return histterm, err
}

/*
handleKeyInput handles the key input for the history mixin.
*/
func (ht *historyLineTerminalMixin) handleKeyInput(e *getch.KeyEvent, buf []rune) (bool, []rune, error) {
	var ret []rune

	if e.Code == getch.KeyArrowUp && ht.historyPointer > 0 {

		// Go up in history

		if ht.historyPointer == ht.history.Size() {

			// Save the current entered text

			ht.lastEntry = stringutil.RuneSliceToString(buf)
		}

		ht.historyPointer--
		histLine := ht.history.Get(ht.historyPointer).(string)

		ret = stringutil.StringToRuneSlice(histLine)

	} else if e.Code == getch.KeyArrowDown && ht.historyPointer < ht.history.Size()-1 {

		// Go down in history

		ht.historyPointer++
		histLine := ht.history.Get(ht.historyPointer).(string)

		ret = stringutil.StringToRuneSlice(histLine)

	} else if e.Code == getch.KeyArrowDown && ht.historyPointer == ht.history.Size()-1 {

		// Restore the last entry from where we started

		ret = stringutil.StringToRuneSlice(ht.lastEntry)
		ht.historyPointer++
	}

	return ret != nil, ret, nil
}

/*
NextLine lets the user produce the next line in the terminal. All entered
characters are echoed. The line is finished if the user presses return or
pastes in a newline character. The final newline is echoed. If single
character input via getch is not available then the code falls back to a
simple line input from stdin. If single character input is available then
the entered lines are safed in a history buffer which can be accessed via the
up and down arrow keys.
*/
func (ht *historyLineTerminalMixin) NextLine() (string, error) {
	line, err := ht.ConsoleLineTerminal.NextLine()

	if strings.TrimSpace(line) != "" && (ht.ignoreLine == nil || !ht.ignoreLine(line)) {

		// Safe entered line

		ht.history.Add(line)
		ht.lastEntry = ""
		ht.historyPointer = ht.history.Size()
	}

	if ht.histFile != "" {
		ioutil.WriteFile(ht.histFile, []byte(ht.history.String()), 0600)
	}

	return line, err
}
