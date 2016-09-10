/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package interpreter

import (
	"errors"
	"fmt"

	"devt.de/eliasdb/eql/parser"
)

/*
newRuntimeError creates a new RuntimeError object.
*/
func (rt *eqlRuntimeProvider) newRuntimeError(t error, d string, node *parser.ASTNode) error {
	return &RuntimeError{rt.name, t, d, node, node.Token.Lline, node.Token.Lpos}
}

/*
RuntimeError is a runtime related error
*/
type RuntimeError struct {
	Source string          // Name of the source which was given to the parser
	Type   error           // Error type (to be used for equal checks)
	Detail string          // Details of this error
	Node   *parser.ASTNode // AST Node where the error occurred
	Line   int             // Line of the error
	Pos    int             // Position of the error
}

/*
Error returns a human-readable string representation of this error.
*/
func (re *RuntimeError) Error() string {
	ret := fmt.Sprintf("EQL error in %s: %v (%v)", re.Source, re.Type, re.Detail)

	if re.Line != 0 {
		return fmt.Sprintf("%s (Line:%d Pos:%d)", ret, re.Line, re.Pos)
	}

	return ret
}

/*
Runtime related error types
*/
var (
	ErrNotARegex        = errors.New("Value of operand is not a valid regex")
	ErrNotANumber       = errors.New("Value of operand is not a number")
	ErrNotAList         = errors.New("Value of operand is not a list")
	ErrInvalidConstruct = errors.New("Invalid construct")
	ErrUnknownNodeKind  = errors.New("Unknown node kind")
	ErrInvalidSpec      = errors.New("Invalid traversal spec")
	ErrInvalidWhere     = errors.New("Invalid where clause")
	ErrInvalidColData   = errors.New("Invalid column data spec")
	ErrEmptyTraversal   = errors.New("Empty traversal")
)

/*
ResultError is a result related error (e.g. wrong defined show clause)
*/
type ResultError struct {
	Source string // Name of the source which was given to the parser
	Type   error  // Error type (to be used for equal checks)
	Detail string // Details of this error
}

/*
Error returns a human-readable string representation of this error.
*/
func (re *ResultError) Error() string {
	return fmt.Sprintf("EQL result error in %s: %v (%v)", re.Source, re.Type, re.Detail)
}
