/* 
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. 
 */

/*
Lexer to convert a given search query into a list of tokens.
*/
package parser

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"devt.de/common/stringutil"
)

// Lexer tokens
// ============

/*
token represents a token which is returned by the lexer.
*/
type LexToken struct {
	Id    LexTokenId // Token kind
	Pos   int        // Starting position (in bytes)
	Val   string     // Token value
	Lline int        // Line in the input this token appears
	Lpos  int        // Position in the input line this token appears
}

/*
Return the position of this token in the origianl input as a string.
*/
func (t LexToken) PosString() string {
	return fmt.Sprintf("Line %v, Pos %v", t.Lline, t.Lpos)
}

/*
String returns a string representation of a token.
*/
func (t LexToken) String() string {

	switch {

	case t.Id == T_EOF:
		return "EOF"

	case t.Id == T_Error:
		return fmt.Sprintf("Error: %s (%s)", t.Val, t.PosString())

	case t.Id > TOKEN_SYMBOLS && t.Id < TOKEN_KEYWORDS:
		return fmt.Sprintf("%s", strings.ToUpper(t.Val))

	case t.Id > TOKEN_KEYWORDS:
		return fmt.Sprintf("<%s>", strings.ToUpper(t.Val))

	case len(t.Val) > 10:

		// Special case for very long values

		return fmt.Sprintf("%.10q...", t.Val)
	}

	return fmt.Sprintf("%q", t.Val)
}

/*
Map of keywords - these require spaces between them
*/
var keywordMap = map[string]LexTokenId{
	"get":           T_GET,
	"lookup":        T_LOOKUP,
	"from":          T_FROM,
	"group":         T_GROUP,
	"with":          T_WITH,
	"filtering":     T_FILTERING,
	"ordering":      T_ORDERING,
	"nulltraversal": T_NULLTRAVERSAL,
	"where":         T_WHERE,
	"traverse":      T_TRAVERSE,
	"end":           T_END,
	"primary":       T_PRIMARY,
	"show":          T_SHOW,
	"as":            T_AS,
	"format":        T_FORMAT,
	"and":           T_AND,
	"or":            T_OR,
	"like":          T_LIKE,
	"in":            T_IN,
	"contains":      T_CONTAINS,
	"beginswith":    T_BEGINSWITH,
	"endswith":      T_ENDSWITH,
	"containsnot":   T_CONTAINSNOT,
	"not":           T_NOT,
	"notin":         T_NOTIN,
	"false":         T_FALSE,
	"true":          T_TRUE,
	"unique":        T_UNIQUE,
	"uniquecount":   T_UNIQUECOUNT,
	"null":          T_NULL,
	"isnotnull":     T_ISNOTNULL,
	"ascending":     T_ASCENDING,
	"descending":    T_DESCENDING,
}

/*
Special symbols which will always be unique - these will separate unquoted strings
*/
var symbolMap = map[string]LexTokenId{
	"@":  T_AT,
	">=": T_GEQ,
	"<=": T_LEQ,
	"!=": T_NEQ,
	"=":  T_EQ,
	">":  T_GT,
	"<":  T_LT,
	"(":  T_LPAREN,
	")":  T_RPAREN,
	"[":  T_LBRACK,
	"]":  T_RBRACK,
	",":  T_COMMA,
	"+":  T_PLUS,
	"-":  T_MINUS,
	"*":  T_TIMES,
	"/":  T_DIV,
	"//": T_DIVINT,
	"%":  T_MODINT,
}

// Lexer
// =====

/*
Special rune which represents the end of the input
*/
const RUNE_EOF = -1

/*
Function which represents the current state of the lexer and returns the next state
*/
type lexFunc func(*lexer) lexFunc

/*
Lexer data structure
*/
type lexer struct {
	name   string        // Name to identify the input
	input  string        // Input string of the lexer
	pos    int           // Current rune pointer
	line   int           // Current line pointer
	lastnl int           // Last newline position
	width  int           // Width of last rune
	start  int           // Start position of the current red token
	scope  LexTokenId    // Current scope
	tokens chan LexToken // Channel for lexer output
}

/*
FirstWord returns the first word of a given input.
*/
func FirstWord(input string) string {
	var word string
	l := &lexer{"", input, 0, 0, 0, 0, 0, -1, nil}

	if skipWhiteSpace(l) {
		l.startNew()
		lexTextBlock(l, false)
		word = input[l.start:l.pos]
	}

	return word
}

/*
Lex lexes a given input. Returns a channel which contains tokens.
*/
func Lex(name string, input string) chan LexToken {
	l := &lexer{name, input, 0, 0, 0, 0, 0, -1, make(chan LexToken)}
	go l.run()
	return l.tokens
}

/*
LexToList lexes a given input. Returns a list of tokens.
*/
func LexToList(name string, input string) []LexToken {
	tokens := make([]LexToken, 0)

	for t := range Lex(name, input) {
		tokens = append(tokens, t)
	}

	return tokens
}

/*
Main look of the lexer.
*/
func (l *lexer) run() {

	if skipWhiteSpace(l) {
		for state := lexToken; state != nil; {
			state = state(l)

			if !skipWhiteSpace(l) {
				break
			}
		}
	}

	close(l.tokens)
}

/*
next returns the next rune in the input and advances the current rune pointer
if the peek flag is not set. If the peek flag is set then the rune pointer
is not advanced.
*/
func (l *lexer) next(peek bool) rune {

	// Check if we reached the end

	if int(l.pos) >= len(l.input) {
		return RUNE_EOF
	}

	// Decode the next rune

	r, w := utf8.DecodeRuneInString(l.input[l.pos:])

	if !peek {
		l.width = w
		l.pos += l.width
	}

	return r
}

/*
backup sets the pointer one rune back. Can only be called once per next call.
*/
func (l *lexer) backup() {
	if l.width == -1 {
		panic("Can only backup once per next call")
	}

	l.pos -= l.width
	l.width = -1
}

/*
startNew starts a new token.
*/
func (l *lexer) startNew() {
	l.start = l.pos
}

/*
emitToken passes a token back to the client.
*/
func (l *lexer) emitToken(t LexTokenId) {
	if t == T_EOF {
		l.emitTokenAndValue(t, "")
		return
	}

	if l.tokens != nil {
		l.tokens <- LexToken{t, l.start, l.input[l.start:l.pos],
			l.line + 1, l.start - l.lastnl + 1}
	}
}

/*
emitTokenAndValue passes a token with a given value back to the client.
*/
func (l *lexer) emitTokenAndValue(t LexTokenId, val string) {
	if l.tokens != nil {
		l.tokens <- LexToken{t, l.start, val, l.line + 1, l.start - l.lastnl + 1}
	}
}

/*
emitError passes an error token back to the client.
*/
func (l *lexer) emitError(msg string) {
	if l.tokens != nil {
		l.tokens <- LexToken{T_Error, l.start, msg, l.line + 1, l.start - l.lastnl + 1}
	}
}

// State functions
// ===============

/*
lexToken is the main entry function for the lexer.
*/
func lexToken(l *lexer) lexFunc {

	// Check if we got a quoted value or a comment

	n1 := l.next(false)
	n2 := l.next(true)
	l.backup()

	if n1 == '#' {
		return skipRestOfLine
	}

	if (n1 == '"' || n1 == '\'') || (n1 == 'r' && (n2 == '"' || n2 == '\'')) {
		return lexValue
	}

	// Lex a block of text and emit any found tokens

	l.startNew()
	lexTextBlock(l, true)

	// Try to lookup the keyword or an unquoted value

	keywordCandidate := strings.ToLower(l.input[l.start:l.pos])

	token, ok := keywordMap[keywordCandidate]

	if !ok {
		token, ok = symbolMap[keywordCandidate]
	}

	if ok {

		// Special start token was found

		l.emitToken(token)

		switch token {
		case T_GET:
			l.scope = token
			return lexNodeKind
		case T_LOOKUP:
			l.scope = token
			return lexNodeKind
		}

	} else {

		// An unknown token was found - it must be an unquoted value
		// emit and continue

		l.emitToken(T_VALUE)
	}

	return lexToken
}

/*
skipRestOfLine skips all characters until the next newline character.
*/
func skipRestOfLine(l *lexer) lexFunc {
	r := l.next(false)

	for r != '\n' && r != RUNE_EOF {
		r = l.next(false)
	}

	if r == RUNE_EOF {
		return nil
	}

	return lexToken
}

/*
lexNodeKind lexes a node kind string.
*/
func lexNodeKind(l *lexer) lexFunc {
	l.startNew()
	lexTextBlock(l, false)

	nodeKindCandidate := strings.ToLower(l.input[l.start:l.pos])
	if !stringutil.IsAlphaNumeric(nodeKindCandidate) {
		l.emitError("Invalid node kind " + fmt.Sprintf("'%v'", nodeKindCandidate) +
			" - can only contain [a-zA-Z0-9_]")
		return nil
	} else {
		l.emitToken(T_NODEKIND)
	}

	if l.scope == T_GET {
		return lexToken
	} else {

		// In a lookup scope more values are following

		return lexValue
	}
}

/*
lexValue lexes a value which can describe names, values, regexes, etc ...

Values can be declared in different ways:

' ... ' or " ... "
Characters are parsed between quotes (escape sequences are interpreted)

r' ... ' or r" ... "
Characters are parsed plain between quote
*/
func lexValue(l *lexer) lexFunc {
	l.startNew()

	allowEscapes := false
	endToken := ' '

	r := l.next(false)

	// Check if we have a raw quoted string

	if q := l.next(true); r == 'r' && (q == '"' || q == '\'') {
		endToken = q
		l.next(false)
	} else if r == '"' || r == '\'' {
		allowEscapes = true
		endToken = r
	} else {
		l.emitError("Value expected")
		return nil
	}

	r = l.next(false)
	lLine := l.line
	lLastnl := l.lastnl

	for r != endToken {

		if r == '\n' {
			lLine++
			lLastnl = l.pos
		}
		r = l.next(false)

		if r == RUNE_EOF {
			l.emitError("Unexpected end while reading value")
			return nil
		}
	}

	if allowEscapes {
		val := l.input[l.start+1 : l.pos-1]

		// Interpret escape sequences right away

		s, err := strconv.Unquote("\"" + val + "\"")
		if err != nil {
			l.emitError(err.Error() + " while parsing escape sequences")
			return nil
		}

		l.emitTokenAndValue(T_VALUE, s)

	} else {
		l.emitTokenAndValue(T_VALUE, l.input[l.start+2:l.pos-1])

	}

	//  Set newline

	l.line = lLine
	l.lastnl = lLastnl

	return lexToken
}

// Helper functions
// ================

/*
skipWhiteSpace skips any number of whitespace characters. Returns false if the parser
reaches EOF while skipping whitespaces.
*/
func skipWhiteSpace(l *lexer) bool {
	r := l.next(false)
	for unicode.IsSpace(r) || unicode.IsControl(r) || r == RUNE_EOF {
		if r == '\n' {
			l.line++
			l.lastnl = l.pos
		}
		r = l.next(false)

		if r == RUNE_EOF {
			l.emitToken(T_EOF)
			return false
		}
	}

	l.backup()
	return true
}

/*
lexTextBlock lexes a block of text without whitespaces. Interprets
optionally all one or two letter tokens.
*/
func lexTextBlock(l *lexer, interpretToken bool) {

	r := l.next(false)

	if interpretToken {

		// Check if we start with a known symbol

		nr := l.next(true)
		if _, ok := symbolMap[strings.ToLower(string(r)+string(nr))]; ok {
			l.next(false)
			return
		}

		if _, ok := symbolMap[strings.ToLower(string(r))]; ok {
			return
		}
	}

	for !unicode.IsSpace(r) && !unicode.IsControl(r) && r != RUNE_EOF {

		if interpretToken {

			// Check if we find a token in the block

			if _, ok := symbolMap[strings.ToLower(string(r))]; ok {
				l.backup()
				return
			}

			nr := l.next(true)
			if _, ok := symbolMap[strings.ToLower(string(r)+string(nr))]; ok {
				l.backup()
				return
			}
		}

		r = l.next(false)
	}

	if r != RUNE_EOF {
		l.backup()
	}
}
