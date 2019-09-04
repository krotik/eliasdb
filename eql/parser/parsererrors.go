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
	"errors"
	"fmt"
)

/*
newParserError creates a new ParserError object.
*/
func (p *parser) newParserError(t error, d string, token LexToken) error {
	return &Error{p.name, t, d, token.Lline, token.Lpos}
}

/*
Error models a parser related error
*/
type Error struct {
	Source string // Name of the source which was given to the parser
	Type   error  // Error type (to be used for equal checks)
	Detail string // Details of this error
	Line   int    // Line of the error
	Pos    int    // Position of the error
}

/*
Error returns a human-readable string representation of this error.
*/
func (pe *Error) Error() string {
	var ret string

	if pe.Detail != "" {
		ret = fmt.Sprintf("Parse error in %s: %v (%v)", pe.Source, pe.Type, pe.Detail)
	} else {
		ret = fmt.Sprintf("Parse error in %s: %v", pe.Source, pe.Type)
	}

	if pe.Line != 0 {
		return fmt.Sprintf("%s (Line:%d Pos:%d)", ret, pe.Line, pe.Pos)
	}

	return ret
}

/*
Parser related error types
*/
var (
	ErrUnexpectedEnd            = errors.New("Unexpected end")
	ErrLexicalError             = errors.New("Lexical error")
	ErrUnknownToken             = errors.New("Unknown term")
	ErrImpossibleNullDenotation = errors.New("Term cannot start an expression")
	ErrImpossibleLeftDenotation = errors.New("Term can only start an expression")
	ErrUnexpectedToken          = errors.New("Unexpected term")
)
