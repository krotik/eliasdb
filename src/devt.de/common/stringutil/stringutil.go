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
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var cSyleCommentsRegexp = regexp.MustCompile("(?s)//.*?\n|/\\*.*?\\*/")

/*
StripCStyleComments strips out C-Style comments from a given string.
*/
func StripCStyleComments(text []byte) []byte {
	return cSyleCommentsRegexp.ReplaceAll(text, nil)
}

/*
Plural returns the string 's' if the parameter is greater than one.
*/
func Plural(l int) string {
	if l > 1 {
		return "s"
	}
	return ""
}

// GlobParseError describes a failure to parse a glob expression
// and gives the offending expression.
type GlobParseError struct {
	Msg  string
	Pos  int
	Glob string
}

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
		case '.':
		case '$':
		case '(':
		case ')':
		case '|':
		case '+':
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

	n, m := len(str1), len(str2)

	if n == 0 {
		return m
	} else if m == 0 {
		return n
	}

	v0 := make([]int, m+1, m+1)
	v1 := make([]int, m+1, m+1)
	var vswap []int

	for i := 0; i <= m; i++ {
		v0[i] = i
	}

	var cost int

	for i := 0; i < n; i++ {
		v1[0] = i + 1

		for j := 0; j < m; j++ {
			if str1[i] == str2[j] {
				cost = 0
			} else {
				cost = 1
			}

			v1[j+1] = min3(v1[j]+1, v0[j+1]+1, v0[j]+cost)
		}

		vswap = v0
		v0 = v1
		v1 = vswap
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
CreateDisplayString changes all "_" characters into spaces and capitalizes
every word.
*/
func CreateDisplayString(str string) string {
	if len(str) == 0 {
		return ""
	}

	return strings.Title(strings.ToLower(strings.Replace(str, "_", " ", -1)))
}

/*
GenerateRollingString creates a string by repeating a given string pattern.
*/
func GenerateRollingString(seq string, size int) string {
	l := len(seq)

	if l == 0 {
		return ""
	}

	buf := new(bytes.Buffer)

	for i := 0; i < size; i++ {
		buf.WriteByte(seq[i%l])
	}

	return buf.String()
}

/*
MD5HexString calculates the MD5 sum of a string and returns it as hex string.
*/
func MD5HexString(str string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(str)))
}
