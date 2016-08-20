/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package parser

import (
	"fmt"
	"testing"
)

const complexTextQuery = `# This is a comment

LOOKUP Song "a","b","c", "blaД"
FROM GROUP test
WHERE r'ДTest AttrblaД' = "blaД" and a < b and a = TRAVERS or not a--1 > b or c contains d and e = 10 - -1 * 10 + 5 or -b
TRAVERSE Song:PerformedSong:Author:Author WHERE f = 6 # This is a comment
    TRAVERSE Author:PerformedAlbum:Album:Album
    END   
    TRAVERSE :PerformedAlbum::Album
    END   
    TRAVERSE :::
    END   
    TRAVERSE Author:PerformedAlbum:Album:Album
        TRAVERSE Author:PerformedAlbum:Album:Album WHERE r:a beginsWith "jj" and b endsWith "kk"
        END   
    END   
END    
TRAVERSE Data:StoredData:File:File WHERE a < b and 7 = (6 + 5) and @count("File:File:StoredData:Data") > 1
END 
TRAVERSE Data:StoredData:File:File WHERE @someFunc(1,2,3,"Data",@someMore()) = -1
END 
PRIMARY Author
SHOW 
    Song:title AS r'Title (mytitle)' FORMAT text,
    r'Song!2:t title' AS "Title test" FORMAT text:bla_bla_blub,
    Song:title AS Title FORMAT text:bla_bla_blub:dudududu,
    !4:kind
WITH ORDERING(ASCENDING dd,DESCENDING dsd), FILTERING(ISNOTNULL ss,UNIQUE aa)

`

func TestComplexLexing(t *testing.T) {
	if res := LexToList("mytest", complexTextQuery); fmt.Sprint(res) !=
		`[<LOOKUP> "Song" "a" , "b" , "c" , "blaД" <FROM> <GROUP> "test" <WHERE> `+
			`"ДTest Attr"... = "blaД" <AND> "a" < "b" <AND> "a" = "TRAVERS" <OR> <NOT> `+
			`"a" - - "1" > "b" <OR> "c" <CONTAINS> "d" <AND> "e" = "10" - - "1" * "10" `+
			`+ "5" <OR> - "b" <TRAVERSE> "Song:Perfo"... <WHERE> "f" = "6" <TRAVERSE> `+
			`"Author:Per"... <END> <TRAVERSE> ":Performed"... <END> <TRAVERSE> ":::" `+
			`<END> <TRAVERSE> "Author:Per"... <TRAVERSE> "Author:Per"... <WHERE> "r:a" `+
			`<BEGINSWITH> "jj" <AND> "b" <ENDSWITH> "kk" <END> <END> <END> <TRAVERSE> `+
			`"Data:Store"... <WHERE> "a" < "b" <AND> "7" = ( "6" + "5" ) <AND> @ `+
			`"count" ( "File:File:"... ) > "1" <END> <TRAVERSE> "Data:Store"... `+
			`<WHERE> @ "someFunc" ( "1" , "2" , "3" , "Data" , @ "someMore" ( ) ) `+
			`= - "1" <END> <PRIMARY> "Author" <SHOW> "Song:title" <AS> "Title `+
			`(myt"... <FORMAT> "text" , "Song!2:t t"... <AS> "Title test" `+
			`<FORMAT> "text:bla_b"... , "Song:title" <AS> "Title" <FORMAT> `+
			`"text:bla_b"... , "!4:kind" <WITH> <ORDERING> ( <ASCENDING> "dd" , `+
			`<DESCENDING> "dsd" ) , <FILTERING> ( <ISNOTNULL> `+
			`"ss" , <UNIQUE> "aa" ) EOF]` {
		t.Error("Unexpected lexer result:", res)
		return
	}
}

func TestSimpleLexing(t *testing.T) {

	// Test empty string parsing

	if res := fmt.Sprint(LexToList("mytest", "    \t   ")); res != "[EOF]" {
		t.Error("Unexpected lexer result:", res)
		return
	}

	// Test invalid node kind

	input := "   \n gEt \n my@node where xxx"
	if res := LexToList("mytest", input); fmt.Sprint(res) !=
		"[<GET> Error: Invalid node kind 'my@node' - can only contain [a-zA-Z0-9_] (Line 3, Pos 2)]" {
		t.Error("Unexpected lexer result:", res)
		return
	}

	// Test valid node kind

	input = "GET mynode"
	if res := LexToList("mytest", input); fmt.Sprint(res) != `[<GET> "mynode" EOF]` {
		t.Error("Unexpected lexer result:", res)
		return
	}

	// Test unquoted value parsing

	input = `GET mynode WHERE name = "myname:x"`
	if res := LexToList("mytest", input); fmt.Sprint(res) != `[<GET> "mynode" <WHERE> "name" = "myname:x" EOF]` {
		t.Error("Unexpected lexer result:", res)
		return
	}

	// Test arithmetics

	input = `GET mynode WHERE name = a + 1 and (ver+x) * 5 > name2`
	if res := LexToList("mytest", input); fmt.Sprint(res) !=
		`[<GET> "mynode" <WHERE> "name" = "a" + "1" <AND> ( "ver" + "x" ) * "5" > "name2" EOF]` {
		t.Error("Unexpected lexer result:", res)
		return
	}

	input = `GET mynode WHERE test = a * 1.3 and (12 / 55aa) * 5 DIV 3 % 1 > true`
	if res := LexToList("mytest", input); fmt.Sprint(res) !=
		`[<GET> "mynode" <WHERE> "test" = "a" * "1.3" <AND> ( "12" / "55aa" ) * "5" "DIV" "3" % "1" > <TRUE> EOF]` {
		t.Error("Unexpected lexer result:", res)
		return
	}

	// Test comments

	input = `GET mynode  # WHERE testcomment = a * 1.3
WHERE a = b
#end`
	if res := LexToList("mytest", input); fmt.Sprint(res) !=
		`[<GET> "mynode" <WHERE> "a" = "b" EOF]` {
		t.Error("Unexpected lexer result:", res)
		return
	}

	// Test traversal

	input = `GET mynode WHERE Author = rabatt TRAVERSE Song:PerformedSong:Author:Author WHERE Author = 6 # This is a comment
END`
	if res := LexToList("mytest", input); fmt.Sprint(res) !=
		`[<GET> "mynode" <WHERE> "Author" = "rabatt" <TRAVERSE> "Song:Perfo"... <WHERE> "Author" = "6" <END> EOF]` {
		t.Error("Unexpected lexer result:", res)
		return
	}
}

func TestValueParsing(t *testing.T) {

	// First word recognition

	if res := FirstWord("   aBBa  test"); res != "aBBa" {
		t.Error("Unexpected first word:", res)
		return
	}

	if res := FirstWord("test"); res != "test" {
		t.Error("Unexpected first word:", res)
		return
	}

	if res := FirstWord("   test"); res != "test" {
		t.Error("Unexpected first word:", res)
		return
	}

	if res := FirstWord("  \n     "); res != "" {
		t.Error("Unexpected first word:", res)
		return
	}

	if res := FirstWord(""); res != "" {
		t.Error("Unexpected first word:", res)
		return
	}

	// Test normal quoted case

	input := `WHERE "name"`

	if res := LexToList("mytest", input); res[1].Val != "name" {
		t.Error("Unexpected value:", res)
		return
	}

	// Test raw quoted

	input = `WHERE r'name'`

	if res := LexToList("mytest", input); res[1].Val != "name" {
		t.Error("Unexpected value:", res)
		return
	}

	// Test quoted with escape sequence

	input = `WHERE "\ntest"`
	if res := LexToList("mytest", input); res[1].Val != "\ntest" {
		t.Error("Unexpected value:", res)
		return
	}

	// Test raw input with spaces and uninterpreted escape sequence

	input = `WHERE r"name is not '\ntest'"`

	if res := LexToList("mytest", input); res[1].Val != "name is not '\\ntest'" {
		t.Error("Unexpected value:", res)
		return
	} else if s := fmt.Sprint(res[1]); s != `"name is no"...` {
		t.Error("Unexpected print result:", s)
	}

	// Test escape sequence error

	input = `WHERE "name\j"`

	if res := LexToList("mytest", input); res[1].Val != "invalid syntax while parsing escape sequences" || res[1].PosString() != "Line 1, Pos 7" {
		t.Error("Unexpected value:", res)
		return
	}

	// Test newline within string + error reporting

	input = ` WHERE r"name
 " WHERE bla`

	if res := LexToList("mytest", input); res[1].Val != "name\n " {
		t.Error("Unexpected value:", res)
		return
	}

	// Test correct line advancing

	input = ` WHERE r'name
 ' WHERE "bla
"`

	if res := LexToList("mytest", input); res[3].Val != "invalid syntax while parsing escape sequences" || res[3].PosString() != "Line 2, Pos 10" {
		t.Error("Unexpected value:", res)
		return
	}

	// Test parse value error

	input = ` lookup x x`

	if res := LexToList("mytest", input); res[2].Val != "Value expected" || res[2].PosString() != "Line 1, Pos 11" {
		t.Error("Unexpected value:", res)
		return
	}

	input = ` lookup x "x`

	if res := LexToList("mytest", input); res[2].Val != "Unexpected end while reading value" || res[2].PosString() != "Line 1, Pos 11" {
		t.Error("Unexpected value:", res)
		return
	}

	input = `where aaaa!=aaa`

	if res := LexToList("mytest", input); fmt.Sprint(res) != `[<WHERE> "aaaa" != "aaa" EOF]` {
		t.Error("Unexpected value:", res)
		return
	}
}

func TestLexerInputControl(t *testing.T) {

	test := &lexer{"test", "test x\xe2\x8c\x98c", 0, 0, 0, 0, 0, -1, nil}

	if r := test.next(false); r != 't' {
		t.Error("Unexpected first rune:", r)
		return
	}
	if r := test.next(false); r != 'e' {
		t.Error("Unexpected first rune:", r)
		return
	}

	test.next(false)
	test.next(false)
	test.next(false)

	if r := test.next(false); r != 'x' {
		t.Error("Unexpected first rune:", r)
		return
	}
	if test.width != 1 {
		t.Error("Unexpected length of rune")
		return
	}

	// Test peeking

	if r := test.next(true); r != '\u2318' {
		t.Error("Unexpected first rune:", r)
		return
	}
	if r := test.next(false); r != '\u2318' {
		t.Error("Unexpected first rune:", r)
		return
	}
	if test.width != 3 {
		t.Error("Unexpected length of rune")
		return
	}

	if r := test.next(false); r != 'c' {
		t.Error("Unexpected first rune:", r)
		return
	}
	if test.width != 1 {
		t.Error("Unexpected length of rune")
		return
	}

	if test.next(false) != RuneEOF {
		t.Error("Unexpected last rune")
		return
	}
	if test.next(true) != RuneEOF {
		t.Error("Unexpected last rune")
		return
	}

	test.backup()

	if r := test.next(false); r != 'c' {
		t.Error("Unexpected first rune:", r)
		return
	}
	if test.width != 1 {
		t.Error("Unexpected length of rune")
		return
	}

	test.backup()

	if r := test.next(false); r != 'c' {
		t.Error("Unexpected first rune:", r)
		return
	}
	if test.width != 1 {
		t.Error("Unexpected length of rune")
		return
	}

	testBackupPanic(t, test)
}

func testBackupPanic(t *testing.T, l *lexer) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Doing a backup twice did not cause a panic.")
		}
	}()

	l.backup()
	l.backup()
}
