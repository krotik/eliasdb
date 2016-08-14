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
Constants for parser and lexer.
*/

package parser

// Lexer tokens
// ============

type LexTokenId int

const (
	T_Error LexTokenId = iota // Lexing error token with a message as val
	T_EOF                     // End-of-file token

	T_VALUE    // Simple value
	T_NODEKIND // Node kind value

	TOKEN_SYMBOLS // Used to separate symbols from other tokens in this list

	T_GEQ
	T_LEQ
	T_NEQ
	T_EQ
	T_GT
	T_LT
	T_LPAREN
	T_RPAREN
	T_LBRACK
	T_RBRACK
	T_COMMA
	T_AT
	T_PLUS
	T_MINUS
	T_TIMES
	T_DIV
	T_DIVINT
	T_MODINT

	// The colon '' has a context specific meaning and is checked by the parser

	TOKEN_KEYWORDS // Used to separate keywords from other tokens in this list

	T_GET
	T_LOOKUP
	T_FROM
	T_GROUP
	T_WITH
	T_LIST
	T_NULLTRAVERSAL
	T_FILTERING
	T_ORDERING
	T_WHERE
	T_TRAVERSE
	T_END
	T_PRIMARY
	T_SHOW
	T_AS
	T_FORMAT
	T_AND
	T_OR
	T_LIKE
	T_IN
	T_CONTAINS
	T_BEGINSWITH
	T_ENDSWITH
	T_CONTAINSNOT
	T_NOT
	T_NOTIN
	T_FALSE
	T_TRUE
	T_UNIQUE
	T_UNIQUECOUNT
	T_NULL
	T_ISNULL
	T_ISNOTNULL
	T_ASCENDING
	T_DESCENDING
)

// Parser AST nodes
// ================

const (
	N_EOF = "EOF"

	N_VALUE         = "value"
	N_TRUE          = "true"
	N_FALSE         = "false"
	N_NULL          = "null"
	N_FUNC          = "func"
	N_ORDERING      = "ordering"
	N_FILTERING     = "filtering"
	N_NULLTRAVERSAL = "nulltraversal"

	// Special tokens - always handled in a denotation function

	N_COMMA  = "comma"
	N_GROUP  = "group"
	N_END    = "end"
	N_AS     = "as"
	N_FORMAT = "format"

	// Keywords

	N_GET    = "get"
	N_LOOKUP = "lookup"
	N_FROM   = "from"
	N_WHERE  = "where"

	N_UNIQUE      = "unique"
	N_UNIQUECOUNT = "uniquecount"
	N_ISNOTNULL   = "isnotnull"
	N_ASCENDING   = "asc"
	N_DESCENDING  = "desc"

	N_TRAVERSE = "traverse"
	N_PRIMARY  = "primary"
	N_SHOW     = "show"
	N_SHOWTERM = "showterm"
	N_WITH     = "with"
	N_LIST     = "list"

	// Boolean operations

	N_OR  = "or"
	N_AND = "and"
	N_NOT = "not"

	N_GEQ = ">="
	N_LEQ = "<="
	N_NEQ = "!="
	N_EQ  = "="
	N_GT  = ">"
	N_LT  = "<"

	// List operations

	N_IN    = "in"
	N_NOTIN = "notin"

	// String operations

	N_LIKE        = "like"
	N_CONTAINS    = "contains"
	N_BEGINSWITH  = "beginswith"
	N_ENDSWITH    = "endswith"
	N_CONTAINSNOT = "containsnot"

	// Simple arithmetic expressions

	N_PLUS   = "plus"
	N_MINUS  = "minus"
	N_TIMES  = "times"
	N_DIV    = "div"
	N_MODINT = "modint"
	N_DIVINT = "divint"

	// Brackets

	N_LPAREN = "("
	N_RPAREN = ")"
	N_LBRACK = "["
	N_RBRACK = "]"
)
