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
	"fmt"
	"sort"
	"strings"

	"devt.de/common/stringutil"
	"devt.de/common/termutil/getch"
)

/*
Dict is a dictionary object used by the AutoCompleteMixin
*/
type Dict interface {

	/*
	   Suggest returns dictionary suggestions based on a given prefix. Returns if there
	   is a direct match and a list of suggestions.
	*/
	Suggest(prefix string) ([]string, error)
}

/*
autocompleteLineTerminalMixin adds auto-complete functionality to a given ConsoleLineTerminals
*/
type autocompleteLineTerminalMixin struct {
	ConsoleLineTerminal      // Terminal which is being extended
	dict                Dict // Dictionary to use for suggestions
	tabCount            int  // Counter for tab presses
}

/*
AddAutoCompleteMixin adds auto-complete support for a given ConsoleLineTerminal.
The auto-complete function operates on a given Dict object which suggests either
a direct match or a list of matches. A single tab auto-completes if there is a
direct match. Two tabs and the console outputs all suggestions.
*/
func AddAutoCompleteMixin(term ConsoleLineTerminal, dict Dict) (ConsoleLineTerminal, error) {

	autoterm := &autocompleteLineTerminalMixin{term, dict, 0}

	// Add key handler

	autoterm.AddKeyHandler(autoterm.handleKeyInput)

	return autoterm, nil
}

/*
handleKeyInput handles the key input for the history mixin.
*/
func (at *autocompleteLineTerminalMixin) handleKeyInput(e *getch.KeyEvent, buf []rune) (bool, []rune, error) {
	var err error
	var ret []rune

	if e.Code == getch.KeyTab {
		var suggestions []string

		at.tabCount++

		currentLine := stringutil.RuneSliceToString(buf)
		words := strings.Split(currentLine, " ")
		prefix := strings.Join(words[:len(words)-1], " ")
		lastWord := words[len(words)-1]

		if suggestions, err = at.dict.Suggest(currentLine); err == nil {
			num := len(suggestions)

			if num == 1 {
				var newline string

				if suggestions[0] == lastWord {

					// Nothing more to auto-complete insert a space for next level suggestions

					newline = fmt.Sprintf("%v ", currentLine)

				} else {

					// If there is only one suggestion we can use it

					if prefix != "" {
						newline = fmt.Sprintf("%v ", prefix)
					}

					newline = fmt.Sprintf("%v%v ", newline, suggestions[0])
				}

				ret = stringutil.StringToRuneSlice(newline)

			} else if len(suggestions) > 1 {

				cp := stringutil.LongestCommonPrefix(suggestions)

				if len(cp) > len(lastWord) {
					var newline string

					if prefix != "" {
						newline = fmt.Sprintf("%v ", prefix)
					}

					ret = stringutil.StringToRuneSlice(fmt.Sprintf("%v%v", newline, cp))
				}

				if at.tabCount > 1 || ret == nil {

					// There are multiple suggestions and tab was pressed more than once

					at.WriteString(fmt.Sprintln())
					at.WriteString(stringutil.PrintStringTable(suggestions, 4))

					if at.tabCount == 2 {

						// Check if at least on suggestion is the full string

						for _, s := range suggestions {
							if s == lastWord {
								ret = stringutil.StringToRuneSlice(currentLine + " ")
								break
							}
						}
					}
				}
			}
		}

		if ret != nil {
			at.tabCount = 0
		}
	}

	return ret != nil, ret, err
}

// Dictionaries
// ============

/*
MultiWordDict models a dictionary which can present suggestions based on multiple
words. Only suggestions for the last word are returned. However, these suggestions
may depend on the preceding words.
*/
type MultiWordDict struct {
	chooser DictChooser
	dicts   map[string]Dict
}

/*
DictChooser chooses a WordListDict based on given prefix words. The function
also gets a presisted map of WordListDicts which can be used as a cache.
*/
type DictChooser func([]string, map[string]Dict) (Dict, error)

/*
NewMultiWordDict returns a new MultiWordDict. The client code needs to specify a
function to retrieve WordListDicts for given prefix words and can optionally
supply an initial map of WordListDicts.
*/
func NewMultiWordDict(chooser DictChooser, dicts map[string]Dict) *MultiWordDict {
	if dicts == nil {
		dicts = make(map[string]Dict)
	}
	return &MultiWordDict{chooser, dicts}
}

/*
Suggest returns dictionary suggestions based on a given prefix. Returns if there
is a direct match and a list of suggestions.
*/
func (md *MultiWordDict) Suggest(prefix string) ([]string, error) {

	// Split prefix into words

	prefixWords := strings.Split(prefix, " ")

	dict, err := md.chooser(prefixWords, md.dicts)

	if err == nil && dict != nil {
		return dict.Suggest(prefixWords[len(prefixWords)-1])
	}

	return nil, err
}

/*
WordListDict is a simple dictionary which looks up suggstions based on an
internal word list
*/
type WordListDict struct {
	words []string
}

/*
NewWordListDict returns a new WordListDict from a given list of words. The list
of words will be sorted.
*/
func NewWordListDict(words []string) *WordListDict {
	sort.Strings(words)
	return &WordListDict{words}
}

/*
Suggest returns dictionary suggestions based on a given prefix. Returns if there
is a direct match and a list of suggestions.
*/
func (wd *WordListDict) Suggest(prefix string) ([]string, error) {
	var suggestions []string

	// Do a binary search on the word list

	index := sort.SearchStrings(wd.words, prefix)

	if index < len(wd.words) {

		// Check the found word

		foundWord := wd.words[index]

		if strings.HasPrefix(foundWord, prefix) {

			// Build up suggestions

			suggestions = append(suggestions, foundWord)

			// Look for further matching words

			for i := index + 1; i < len(wd.words); i++ {
				if nextWord := wd.words[i]; strings.HasPrefix(nextWord, prefix) {
					suggestions = append(suggestions, nextWord)
				}
			}
		}
	}

	return suggestions, nil
}
