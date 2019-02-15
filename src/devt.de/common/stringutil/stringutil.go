/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package stringutil contains common function for string operations.
*/
package stringutil

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

/*
LongestCommonPrefix determines the longest common prefix of a given list of strings.
*/
func LongestCommonPrefix(s []string) string {
	var res string

	commonPrefix := func(str1, str2 string) string {
		var buf bytes.Buffer

		rs2 := StringToRuneSlice(str2)
		rs2len := len(rs2)

		for i, c := range str1 {
			if i >= rs2len {
				break
			} else if c == rs2[i] {
				buf.WriteRune(c)
			}
		}

		return buf.String()
	}

	lens := len(s)

	if lens > 0 {
		res = s[0]

		for i := 1; i < lens; i++ {
			res = commonPrefix(res, s[i])
		}
	}

	return res
}

/*
PrintStringTable prints a given list of strings as table with c columns.
*/
func PrintStringTable(ss []string, c int) string {
	var ret bytes.Buffer

	if c < 1 {
		return ""
	}

	//  Determine max widths of columns

	maxWidths := make(map[int]int)

	for i, s := range ss {
		col := i % c

		if l := utf8.RuneCountInString(s); l > maxWidths[col] {
			maxWidths[col] = l
		}
	}

	for i, s := range ss {
		col := i % c

		if i < len(ss)-1 {
			var formatString string

			if col != c-1 {
				formatString = fmt.Sprintf("%%-%vv ", maxWidths[col])
			} else {
				formatString = "%v"
			}

			ret.WriteString(fmt.Sprintf(formatString, s))

		} else {

			ret.WriteString(fmt.Sprintln(s))
			break
		}

		if col == c-1 {
			ret.WriteString(fmt.Sprintln())
		}
	}

	return ret.String()
}

/*
GraphicStringTableSymbols defines how to draw a graphic table.
*/
type GraphicStringTableSymbols struct {
	BoxHorizontal string
	BoxVertical   string
	BoxMiddle     string

	BoxCornerTopLeft     string
	BoxCornerTopRight    string
	BoxCornerBottomLeft  string
	BoxCornerBottomRight string

	BoxTopMiddle    string
	BoxLeftMiddle   string
	BoxRightMiddle  string
	BoxBottomMiddle string
}

/*
Standard graphic table drawing definitions.
*/
var (
	SingleLineTable       = &GraphicStringTableSymbols{"─", "│", "┼", "┌", "┐", "└", "┘", "┬", "├", "┤", "┴"}
	DoubleLineTable       = &GraphicStringTableSymbols{"═", "║", "╬", "╔", "╗", "╚", "╝", "╦", "╠", "╣", "╩"}
	SingleDoubleLineTable = &GraphicStringTableSymbols{"═", "│", "╪", "╒", "╕", "╘", "╛", "╤", "╞", "╡", "╧"}
	DoubleSingleLineTable = &GraphicStringTableSymbols{"─", "║", "╫", "╓", "╖", "╙", "╜", "╥", "╟", "╢", "╨"}
	MonoTable             = &GraphicStringTableSymbols{"#", "#", "#", "#", "#", "#", "#", "#", "#", "#", "#"}
)

/*
PrintGraphicStringTable prints a given list of strings in a graphic table
with c columns - creates a header after n rows using syms as drawing symbols.
*/
func PrintGraphicStringTable(ss []string, c int, n int, syms *GraphicStringTableSymbols) string {
	var topline, bottomline, middleline, ret bytes.Buffer

	if c < 1 {
		return ""
	}

	if syms == nil {
		syms = MonoTable
	}

	//  Determine max widths of columns

	maxWidths := make(map[int]int)

	for i, s := range ss {
		col := i % c

		l := utf8.RuneCountInString(s)

		if l > maxWidths[col] {
			maxWidths[col] = l
		}
	}

	// Determine total width and create top, middle and bottom line

	totalWidth := 1
	topline.WriteString(syms.BoxCornerTopLeft)
	bottomline.WriteString(syms.BoxCornerBottomLeft)
	middleline.WriteString(syms.BoxLeftMiddle)

	for i := 0; i < len(maxWidths); i++ {
		totalWidth += maxWidths[i] + 2

		topline.WriteString(GenerateRollingString(syms.BoxHorizontal, maxWidths[i]+1))
		bottomline.WriteString(GenerateRollingString(syms.BoxHorizontal, maxWidths[i]+1))
		middleline.WriteString(GenerateRollingString(syms.BoxHorizontal, maxWidths[i]+1))

		if i < len(maxWidths)-1 {
			topline.WriteString(syms.BoxTopMiddle)
			bottomline.WriteString(syms.BoxBottomMiddle)
			middleline.WriteString(syms.BoxMiddle)
		}
	}

	topline.WriteString(syms.BoxCornerTopRight)
	bottomline.WriteString(syms.BoxCornerBottomRight)
	middleline.WriteString(syms.BoxRightMiddle)

	// Draw the table

	ret.WriteString(topline.String())
	ret.WriteString(fmt.Sprintln())

	row := 0
	for i, s := range ss {
		col := i % c

		ret.WriteString(syms.BoxVertical)

		if i < len(ss)-1 {
			formatString := fmt.Sprintf("%%-%vv ", maxWidths[col])
			ret.WriteString(fmt.Sprintf(formatString, s))
		} else {
			formatString := fmt.Sprintf("%%-%vv ", maxWidths[col])
			ret.WriteString(fmt.Sprintf(formatString, s))

			for col < c-1 && col < len(ss)-1 {
				col++
				ret.WriteString(syms.BoxVertical)
				ret.WriteString(GenerateRollingString(" ", maxWidths[col]))
				ret.WriteString(" ")
			}

			ret.WriteString(syms.BoxVertical)
			ret.WriteString(fmt.Sprintln())

			break
		}

		if col == c-1 {
			ret.WriteString(syms.BoxVertical)
			ret.WriteString(fmt.Sprintln())
			row++

			if row == n {
				ret.WriteString(middleline.String())
				ret.WriteString(fmt.Sprintln())
			}
		}
	}

	ret.WriteString(bottomline.String())
	ret.WriteString(fmt.Sprintln())

	return ret.String()
}

/*
PrintCSVTable prints a given list of strings in a CSV table with c
columns.
*/
func PrintCSVTable(ss []string, c int) string {
	var ret bytes.Buffer
	var col int

	if c < 1 || len(ss) == 0 {
		return ""
	}

	// Write the table

	for i, s := range ss {
		col = i % c

		ret.WriteString(strings.TrimSpace(fmt.Sprint(s)))

		if col == c-1 {
			ret.WriteString(fmt.Sprintln())
		} else if i < len(ss)-1 {
			ret.WriteString(", ")
		}
	}

	if col != c-1 {
		ret.WriteString(fmt.Sprintln())
	}

	return ret.String()
}

/*
RuneSliceToString converts a slice of runes into a string.
*/
func RuneSliceToString(buf []rune) string {
	var sbuf bytes.Buffer
	for _, r := range buf {
		fmt.Fprintf(&sbuf, "%c", r)
	}
	return sbuf.String()
}

/*
StringToRuneSlice converts a string into a slice of runes.
*/
func StringToRuneSlice(s string) []rune {
	var buf []rune
	for _, r := range s {
		buf = append(buf, r)
	}
	return buf
}

var cSyleCommentsRegexp = regexp.MustCompile("(?s)//.*?\n|/\\*.*?\\*/")

/*
StripCStyleComments strips out C-Style comments from a given string.
*/
func StripCStyleComments(text []byte) []byte {
	return cSyleCommentsRegexp.ReplaceAll(text, nil)
}

/*
Plural returns the string 's' if the parameter is greater than one or
if the parameter is 0.
*/
func Plural(l int) string {
	if l > 1 || l == 0 {
		return "s"
	}
	return ""
}

/*
GlobParseError describes a failure to parse a glob expression
and gives the offending expression.
*/
type GlobParseError struct {
	Msg  string
	Pos  int
	Glob string
}

/*
Error Returns a string representation of the error.
*/
func (e *GlobParseError) Error() string {
	return fmt.Sprintf("%s at %d of %s", e.Msg, e.Pos, e.Glob)
}

/*
GlobToRegex converts a given glob expression into a regular expression.
*/
func GlobToRegex(glob string) (string, error) {

	buf := new(bytes.Buffer)
	brackets, braces := 0, 0
	n := len(glob)

	for i := 0; i < n; i++ {
		char := glob[i]

		switch char {
		case '\\':
			// Escapes
			i++
			if i >= n {
				return "", &GlobParseError{"Missing escaped character", i, glob}
			}
			buf.WriteByte(char)
			buf.WriteByte(glob[i])
			continue

		case '*':
			// Wildcard match multiple characters
			buf.WriteByte('.')
		case '?':
			// Wildcard match any single character
			buf.WriteByte('.')
			continue
		case '{':
			// Group (always non-capturing)
			buf.WriteString("(?:")
			braces++
			continue
		case '}':
			// End of group
			if braces > 0 {
				braces--
				buf.WriteByte(')')
				continue
			}
		case '[':
			// Character class
			if brackets > 0 {
				return "", &GlobParseError{"Unclosed character class", i, glob}
			}
			brackets++
		case ']':
			// End of character class
			brackets = 0
		case ',':
			// OR in groups
			if braces > 0 {
				buf.WriteByte('|')
			} else {
				buf.WriteByte(char)
			}
			continue
		case '^':
			// Beginning of line in character classes otherwise normal
			// escaped character
			if brackets == 0 {
				buf.WriteByte('\\')
			}
		case '!':
			// [! is the equivalent of [^ in glob
			if brackets > 0 && glob[i-1] == '[' {
				buf.WriteByte('^')
			} else {
				buf.WriteByte('!')
			}
			continue
		case '.', '$', '(', ')', '|', '+':
			// Escape all regex characters which are not glob characters
			buf.WriteByte('\\')
		}

		buf.WriteByte(char)
	}

	if brackets > 0 {
		return "", &GlobParseError{"Unclosed character class", n, glob}
	} else if braces > 0 {
		return "", &GlobParseError{"Unclosed group", n, glob}
	}

	return buf.String(), nil
}

/*
GlobStartingLiterals gets the first literals of a glob string.
*/
func GlobStartingLiterals(glob string) string {

	buf := new(bytes.Buffer)
	n := len(glob)

	for i := 0; i < n; i++ {
		char := glob[i]

		if char == '\\' || char == '*' || char == '?' ||
			char == '{' || char == '[' {
			break
		}
		buf.WriteByte(char)
	}

	return buf.String()
}

/*
LevenshteinDistance computes the Levenshtein distance between two strings.
*/
func LevenshteinDistance(str1, str2 string) int {
	if str1 == str2 {
		return 0
	}

	rslice1 := StringToRuneSlice(str1)
	rslice2 := StringToRuneSlice(str2)

	n, m := len(rslice1), len(rslice2)

	if n == 0 {
		return m
	} else if m == 0 {
		return n
	}

	v0 := make([]int, m+1, m+1)
	v1 := make([]int, m+1, m+1)

	for i := 0; i <= m; i++ {
		v0[i] = i
	}

	var cost int

	for i := 0; i < n; i++ {
		v1[0] = i + 1

		for j := 0; j < m; j++ {
			if rslice1[i] == rslice2[j] {
				cost = 0
			} else {
				cost = 1
			}

			v1[j+1] = min3(v1[j]+1, v0[j+1]+1, v0[j]+cost)
		}

		v0, v1 = v1, v0
	}

	return v0[m]
}

/*
3 way min for computing the Levenshtein distance.
*/
func min3(a, b, c int) int {
	ret := a
	if b < ret {
		ret = b
	}
	if c < ret {
		ret = c
	}
	return ret
}

/*
VersionStringCompare compares two version strings. Returns: 0 if the strings are
equal; -1 if the first string is smaller; 1 if the first string is greater.
*/
func VersionStringCompare(str1, str2 string) int {
	val1 := strings.Split(str1, ".")
	val2 := strings.Split(str2, ".")

	idx := 0

	for idx < len(val1) && idx < len(val2) && val1[idx] == val2[idx] {
		idx++
	}

	switch {
	case idx < len(val1) && idx < len(val2):
		return versionStringPartCompare(val1[idx], val2[idx])
	case len(val1) > len(val2):
		return 1
	case len(val1) < len(val2):
		return -1
	}
	return 0
}

/*
versionStringPartCompare compares two version string parts. Returns: 0 if the
strings are equal; -1 if the first string is smaller; 1 if the first string is
greater.
*/
func versionStringPartCompare(str1, str2 string) int {
	pat := regexp.MustCompile("^([0-9]+)([\\D].*)?")

	res1 := pat.FindStringSubmatch(str1)
	res2 := pat.FindStringSubmatch(str2)

	switch {
	case res1 == nil && res2 == nil:
		return strings.Compare(str1, str2)
	case res1 == nil && res2 != nil:
		return -1
	case res1 != nil && res2 == nil:
		return 1
	}

	v1, _ := strconv.Atoi(res1[1])
	v2, _ := strconv.Atoi(res2[1])

	res := 0

	switch {
	case v1 > v2:
		res = 1
	case v1 < v2:
		res = -1
	}

	if res == 0 {

		switch {
		case res1[2] != "" && res2[2] == "":
			return 1
		case res1[2] == "" && res2[2] != "":
			return -1
		case res1[2] != "" && res2[2] != "":
			return strings.Compare(res1[2], res2[2])
		}
	}

	return res
}

/*
IsAlphaNumeric checks if a string contains only alpha numerical characters or "_".
*/
func IsAlphaNumeric(str string) bool {
	ret, _ := regexp.MatchString("^[a-zA-Z0-9_]*$", str)
	return ret
}

/*
IsTrueValue checks if a given string is a true value.
*/
func IsTrueValue(str string) bool {
	str = strings.ToLower(str)
	return str == "true" || str == "yes" || str == "on" || str == "ok" ||
		str == "1" || str == "active" || str == "enabled"
}

/*
IndexOf returns the index of str in slice or -1 if it does not exist.
*/
func IndexOf(str string, slice []string) int {
	for i, s := range slice {
		if str == s {
			return i
		}
	}

	return -1
}

/*
MapKeys returns the keys of a map as a sorted list.
*/
func MapKeys(m map[string]interface{}) []string {
	ret := make([]string, 0, len(m))

	for k := range m {
		ret = append(ret, k)
	}

	sort.Strings(ret)

	return ret
}

/*
CreateDisplayString changes all "_" characters into spaces and properly capitalizes
the resulting string.
*/
func CreateDisplayString(str string) string {
	if len(str) == 0 {
		return ""
	}

	return ProperTitle(strings.Replace(str, "_", " ", -1))
}

// The following words should not be capitalized
//
var notCapitalize = map[string]string{
	"a":    "",
	"an":   "",
	"and":  "",
	"at":   "",
	"but":  "",
	"by":   "",
	"for":  "",
	"from": "",
	"in":   "",
	"nor":  "",
	"on":   "",
	"of":   "",
	"or":   "",
	"the":  "",
	"to":   "",
	"with": "",
}

/*
ProperTitle will properly capitalize a title string by capitalizing the first, last
and any important words. Not capitalized are articles: a, an, the; coordinating
conjunctions: and, but, or, for, nor; prepositions (fewer than five
letters): on, at, to, from, by.
*/
func ProperTitle(input string) string {
	words := strings.Fields(strings.ToLower(input))
	size := len(words)

	for index, word := range words {
		if _, ok := notCapitalize[word]; !ok || index == 0 || index == size-1 {
			words[index] = strings.Title(word)
		}
	}

	return strings.Join(words, " ")
}

/*
GenerateRollingString creates a string by repeating a given string pattern.
*/
func GenerateRollingString(seq string, size int) string {
	var buf bytes.Buffer

	rs := StringToRuneSlice(seq)
	l := len(rs)

	if l == 0 {
		return ""
	}

	for i := 0; i < size; i++ {
		buf.WriteRune(rs[i%l])
	}

	return buf.String()
}

/*
ConvertToString tries to convert a given object into a stable string. This
function can be used to display nested maps.
*/
func ConvertToString(v interface{}) string {

	if vStringer, ok := v.(fmt.Stringer); ok {
		return vStringer.String()
	}

	if _, err := json.Marshal(v); err != nil {
		v = containerStringConvert(v)
	}

	if vString, ok := v.(string); ok {
		return vString
	} else if res, err := json.Marshal(v); err == nil {
		return string(res)
	}

	return fmt.Sprint(v)
}

/*
containerStringConvert converts container contents into strings.
*/
func containerStringConvert(v interface{}) interface{} {
	res := v

	if mapContainer, ok := v.(map[interface{}]interface{}); ok {
		newRes := make(map[string]interface{})

		for mk, mv := range mapContainer {
			newRes[ConvertToString(mk)] = containerStringConvert(mv)
		}

		res = newRes

	} else if mapList, ok := v.([]interface{}); ok {
		newRes := make([]interface{}, len(mapList))

		for i, lv := range mapList {
			newRes[i] = containerStringConvert(lv)
		}

		res = newRes
	}

	return res
}

/*
MD5HexString calculates the MD5 sum of a string and returns it as hex string.
*/
func MD5HexString(str string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(str)))
}

/*
LengthConstantEquals compares two strings in length-constant time. This
function is deliberately inefficient in that it does not stop at the earliest
possible time. This is to prevent timing attacks when comparing password
hashes.
*/
func LengthConstantEquals(str1 []byte, str2 []byte) bool {
	diff := len(str1) ^ len(str2)

	for i := 0; i < len(str1) && i < len(str2); i++ {
		diff |= int(str1[i] ^ str2[i])
	}

	return diff == 0
}
