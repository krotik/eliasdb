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
	"io"
)

/*
filereadingTerminalMixin first provides lines from a given file before delegating
to a wrapped terminal
*/
type filereadingTerminalMixin struct {
	ConsoleLineTerminal          // Terminal which is being wrapped
	lines               []string // File containing the first lines
	linePointer         int      // Pointer to the next line
	termOnEOF           bool     // Flag if the terminal should send EOT after last file line
}

/*
AddFileReadingWrapper wraps a given terminal and provides the fist lines of
a given input reader as first lines before delegating to the wrapped terminal.
Terminates after the file has been red if termOnEOF is set.
*/
func AddFileReadingWrapper(term ConsoleLineTerminal, r io.Reader, termOnEOF bool) (ConsoleLineTerminal, error) {
	var ret ConsoleLineTerminal

	fileterm := &filereadingTerminalMixin{term, nil, 0, termOnEOF}

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fileterm.lines = append(fileterm.lines, scanner.Text())
	}

	ret = fileterm

	return ret, nil
}

/*
NextLine returns the next line in the lines array. After the final line it
either delegates to the wrapped terminal or sends EOT.
*/
func (ht *filereadingTerminalMixin) NextLine() (string, error) {
	var line string
	var err error

	if ht.linePointer < len(ht.lines) {

		line = ht.lines[ht.linePointer]
		ht.linePointer++

	} else if !ht.termOnEOF {

		line, err = ht.ConsoleLineTerminal.NextLine()

	} else {

		line = "\x04"
	}

	return line, err
}
