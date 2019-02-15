/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package termutil contains common function for terminal operations.
*/
package termutil

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"unicode/utf8"

	"devt.de/common/stringutil"
	"devt.de/common/termutil/getch"
)

/*
KeyHandler handles specific key events. KeyHandlers are used to extend the
functionality of the normal ConsoleLineTerminal. Returns if the event was
consumed (no further handling possible), a new input buffer and any errors
that might have occured. The new input buffer is ignored if it is nil.
*/
type KeyHandler func(*getch.KeyEvent, []rune) (bool, []rune, error)

/*
ConsoleLineTerminal is the most common console terminal implementation. The
user types input and a choosen backend records the input by key. It has a
graceful fallback to a standard line reader for all other platforms. The
functionality can be extended by adding key handlers.

Example code:

	clt, err := termutil.NewConsoleLineTerminal(os.Stdout)

	if err == nil {

		// Add history functionality

		clt, err = termutil.AddHistoryMixin(clt, "", func(s string) bool {
			return s == "q"
		})

		if err == nil {

			rootDict := termutil.NewWordListDict([]string{"ll", "dir", "test",
				"test1", "test2"})

			chooser := func(lineWords []string,
				dictCache map[string]termutil.Dict) (termutil.Dict, error) {

				if len(lineWords) == 1 {
					return rootDict, nil
				}

				return termutil.NewWordListDict([]string{
					fmt.Sprintf("file4-%v", len(lineWords)), "file2",
					"file1", "directory"}), nil
			}

			dict := termutil.NewMultiWordDict(chooser, nil)

			clt, err = termutil.AddAutoCompleteMixin(clt, dict)

			if err == nil {
				if err = clt.StartTerm(); err == nil {
					var line string

					defer clt.StopTerm()

					line, err = clt.NextLine()
					for err == nil && line != "q" {
						fmt.Println("###", line)
						line, err = clt.NextLine()
					}
				}
			}
		}
	}

	if err != nil {
		fmt.Println(err)
	}
*/
type ConsoleLineTerminal interface {

	/*
		StartTerm prepares a new terminal session. This call initialises the tty
		on Linux or retrieves an event object on Windows.
	*/
	StartTerm() error

	/*
	   AddKeyHandler adds a new KeyHandler to this ConsoleLineTerminal.
	*/
	AddKeyHandler(handler KeyHandler)

	/*
		NextLine lets the user produce the next line in the terminal. All entered
		characters are echoed. The line is finished if the user presses return or
		pastes in a newline character. The final newline is echoed. If single
		character input via getch is not available then the code falls back to a
		simple line input from stdin.
	*/
	NextLine() (string, error)

	/*
	   NextLinePrompt lets the user produce the next line in the terminal with a
	   special prompt. All entered characters are echoed if echo is 0x0 otherwise
	   the echo character is written. The line is finished if the user presses
	   return or pastes in a newline character. The final newline is echoed. If
	   single character input via getch is not available then the code falls back
	   to a simple line input from stdin.
	*/
	NextLinePrompt(prompt string, echo rune) (string, error)

	/*
	   WriteString write a string on this terminal.
	*/
	WriteString(s string)

	/*
	   Write writes len(p) bytes from p to the terminal.
	*/
	Write(p []byte) (n int, err error)

	/*
		StopTerm finishes the current terminal session. This call returns the tty
		on Linux to its original state and closes all open handles on all platforms.
	*/
	StopTerm()
}

/*
consoleLineTerminal is the main ConsoleLineTerminal implementation.
*/
type consoleLineTerminal struct {
	console  io.Writer    // Console to write to
	prompt   string       // Terminal prompt to display
	fallback bool         // Flag if we can use getch or should do fallback
	handlers []KeyHandler // List of KeyHandlers which provide extra functionality
}

/*
NewConsoleLineTerminal creates a new basic ConsoleLineTerminal.
*/
func NewConsoleLineTerminal(console io.Writer) (ConsoleLineTerminal, error) {
	ret := &consoleLineTerminal{console, ">>>", false, []KeyHandler{}}
	return ret, nil
}

/*
AddKeyHandler adds a new KeyHandler to this ConsoleLineTerminal.
*/
func (clr *consoleLineTerminal) AddKeyHandler(handler KeyHandler) {
	clr.handlers = append(clr.handlers, handler)
}

/*
StartTerm prepares a new terminal session. This call initialises the tty on
Linux or retrieves an event object on Windows.
*/
func (clr *consoleLineTerminal) StartTerm() error {

	// Initialise getch

	err := getchStart()

	if err != nil {

		// Activate fallback

		clr.fallback = true
		err = nil
	}

	return err
}

/*
WriteString write a string on this terminal.
*/
func (clr *consoleLineTerminal) WriteString(s string) {
	clr.Write([]byte(s))
}

/*
Write writes len(p) bytes from p to the terminal.
*/
func (clr *consoleLineTerminal) Write(p []byte) (n int, err error) {
	return fmt.Fprint(clr.console, string(p))
}

/*
StopTerm finishes the current terminal session. This call returns the tty on
Linux to its original state and closes all open handles on all platforms.
*/
func (clr *consoleLineTerminal) StopTerm() {
	getchStop()
}

/*
NextLine lets the user produce the next line in the terminal. All entered characters
are echoed. The line is finished if the user presses return or pastes in a newline
character. The final newline is echoed. If single character input via getch is not
available then the code falls back to a simple line input from stdin.
*/
func (clr *consoleLineTerminal) NextLine() (string, error) {
	return clr.NextLinePrompt(clr.prompt, 0x0)
}

/*
NextLinePrompt lets the user produce the next line in the terminal with a
special prompt. All entered characters are echoed if echo is 0x0 otherwise
the echo character is written. The line is finished if the user presses
return or pastes in a newline character. The final newline is echoed. If
single character input via getch is not available then the code falls back
to a simple line input from stdin.
*/
func (clr *consoleLineTerminal) NextLinePrompt(prompt string, echo rune) (string, error) {
	var err error
	var e *getch.KeyEvent

	// Write out prompt

	fmt.Fprint(clr.console, prompt)

	if clr.fallback {

		if echo != 0x0 {

			// Input characters cannot be masked in fallback mode

			return "", fmt.Errorf("Cannot mask input characters")
		}

		// Use the fallback solution

		scanner := bufio.NewScanner(stdin)
		scanner.Scan()
		return scanner.Text(), nil
	}

	var buf []rune
	var lastWrite int
	cursorPos := 0

	addToBuf := func(t rune) {
		buf = append(buf[:cursorPos], append([]rune{t}, buf[cursorPos:]...)...)
		cursorPos++
	}

	delLeftFromCursor := func() {
		buf = append(buf[:cursorPos-1], buf[cursorPos:]...)
		cursorPos--
	}

	delRightFromCursor := func() {
		buf = append(buf[:cursorPos], buf[cursorPos+1:]...)
	}

MainGetchLoop:

	// Main loop exits on error, Enter key or EOT (End of transmission) (CTRL+d)

	for (e == nil || (e.Code != getch.KeyEnter && e.Rune != 0x4)) && err == nil {
		e, err = getchGetch()

		if _, ok := err.(*getch.ErrUnknownEscapeSequence); ok {

			// Ignore unknown escape sequences

			err = nil
			continue
		}

		if err == nil {

			// Check KeyHandlers

			for _, h := range clr.handlers {
				var consumed bool
				var newBuf []rune

				consumed, newBuf, err = h(e, buf)

				if newBuf != nil {
					buf = newBuf
					cursorPos = len(newBuf)
				}

				if consumed {
					lastWrite = clr.output(prompt, buf, cursorPos, lastWrite)
					continue MainGetchLoop
				}
			}
		}

		if err == nil {

			if e.Rune != 0x0 {

				// Normal case a printable character was typed

				if len(e.RawBuf) == 0 {

					addToBuf(e.Rune)

				} else {

					// Handle copy & paste and quick typing

					for _, r := range string(e.RawBuf) {
						addToBuf(r)
					}
				}

			} else if e.Code == getch.KeyArrowLeft && cursorPos > 0 {

				cursorPos--

			} else if e.Code == getch.KeyArrowRight && cursorPos < len(buf) {

				cursorPos++

			} else if e.Code == getch.KeyEnd {

				cursorPos = len(buf)

			} else if e.Code == getch.KeyHome {

				cursorPos = 0

			} else if e.Code == getch.KeyDelete && cursorPos < len(buf) {

				// Delete next character

				delRightFromCursor()

			} else if e.Code == getch.KeyBackspace && cursorPos > 0 {

				// Delete last character

				delLeftFromCursor()

			} else if !e.Alt && !e.Shift && !e.Ctrl &&
				e.Rune == 0x0 && e.Code == "" {

				// Just append a space

				addToBuf(' ')
			}

			if e.Rune != 0x4 { // Do not echo EOT
				var outBuf []rune

				if echo != 0x0 {

					// Fill up the output buffer with the echo rune

					outBuf = make([]rune, len(buf))
					for i := range buf {
						outBuf[i] = echo
					}

				} else {

					outBuf = buf
				}

				lastWrite = clr.output(prompt, outBuf, cursorPos, lastWrite)
			}
		}
	}

	// Write final newline to be consistent with fallback line input

	fmt.Fprintln(clr.console, "")

	return stringutil.RuneSliceToString(buf), err
}

/*
output writes the current line in the terminal.
*/
func (clr *consoleLineTerminal) output(prompt string, buf []rune, cursorPos int, toClear int) int {
	promptLen := utf8.RuneCountInString(prompt)

	// Remove previous prompt text (on same line)

	fmt.Fprint(clr.console, "\r")
	fmt.Fprint(clr.console, stringutil.GenerateRollingString(" ", toClear))
	fmt.Fprint(clr.console, "\r")
	fmt.Fprint(clr.console, prompt)

	fmt.Fprintf(clr.console, stringutil.RuneSliceToString(buf))

	// Position the cursor

	if _, y, err := getch.CursorPosition(); err == nil {
		getch.SetCursorPosition(promptLen+cursorPos, y)
	}

	return promptLen + len(buf)
}

// Low-level input interfaces
// ==========================

var stdin io.Reader = os.Stdin

var getchStart = getch.Start
var getchStop = getch.Stop
var getchGetch = getch.Getch
