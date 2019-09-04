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
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"

	"devt.de/krotik/common/lang/graphql/parser"
	"devt.de/krotik/common/stringutil"
)

/*
handleRuntimeError handles any errors which happen at runtime.
*/
func (rtp *GraphQLRuntimeProvider) handleRuntimeError(err error, path []string, node *parser.ASTNode) {
	if err != nil {

		// Depuplicate errors (no point in showing the same error twice)

		hasher := sha256.New()
		hasher.Write([]byte(err.Error()))
		hasher.Write([]byte(fmt.Sprint(path)))
		hasher.Write([]byte(fmt.Sprint(node.Token.Lline)))
		hasher.Write([]byte(fmt.Sprint(node.Token.Lpos)))
		errorHash := base64.URLEncoding.EncodeToString(hasher.Sum(nil))

		if stringutil.IndexOf(errorHash, rtp.ErrorKeys) == -1 {

			rtp.Errors = append(rtp.Errors,
				&RuntimeError{rtp.Name, ErrRuntimeError, err.Error(), node,
					node.Token.Lline, node.Token.Lpos, false, rtp})
			rtp.ErrorPaths = append(rtp.ErrorPaths, path)
			rtp.ErrorKeys = append(rtp.ErrorKeys, errorHash)
		}
	}
}

/*
newRuntimeError creates a new RuntimeError object.
*/
func (rtp *GraphQLRuntimeProvider) newFatalRuntimeError(t error, d string, node *parser.ASTNode) error {
	return &RuntimeError{rtp.Name, t, d, node, node.Token.Lline, node.Token.Lpos, true, rtp}
}

/*
RuntimeError is a runtime related error
*/
type RuntimeError struct {
	Source          string                  // Name of the source which was given to the parser
	Type            error                   // Error type (to be used for equal checks)
	Detail          string                  // Details of this error
	Node            *parser.ASTNode         // AST Node where the error occurred
	Line            int                     // Line of the error
	Pos             int                     // Position of the error
	IsFatal         bool                    // Is a fatal error which should stop the whole operation
	RuntimeProvider *GraphQLRuntimeProvider // Runtime provider which produced this error
}

/*
Error returns a human-readable string representation of this error.
*/
func (re *RuntimeError) Error() string {

	op := re.RuntimeProvider.QueryType
	if op == "" {
		op = "operation"
	}

	fatal := ""
	if re.IsFatal {
		fatal = "Fatal "
	}

	ret := fmt.Sprintf("%sGraphQL %s error in %s: %v (%v)", fatal, op,
		re.Source, re.Type, re.Detail)

	if re.Line != 0 {
		ret = fmt.Sprintf("%s (Line:%d Pos:%d)", ret, re.Line, re.Pos)
	}

	return ret
}

/*
Runtime related error types
*/
var (
	ErrInvalidConstruct    = errors.New("Invalid construct")
	ErrAmbiguousDefinition = errors.New("Ambiguous definition")
	ErrMissingOperation    = errors.New("Missing operation")
	ErrRuntimeError        = errors.New("Runtime error")
)
