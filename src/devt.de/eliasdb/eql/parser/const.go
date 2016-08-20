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
Package parser contains the EQL parser.

Constants for parser and lexer.
*/
package parser

/*
LexTokenID represents a unique lexer token ID
*/
type LexTokenID int

/*
Available lexer token types
*/
const (
	TokenError LexTokenID = iota // Lexing error token with a message as val
	TokenEOF                     // End-of-file token

	TokenVALUE    // Simple value
	TokenNODEKIND // Node kind value

	TOKENodeSYMBOLS // Used to separate symbols from other tokens in this list

	TokenGEQ
	TokenLEQ
	TokenNEQ
	TokenEQ
	TokenGT
	TokenLT
	TokenLPAREN
	TokenRPAREN
	TokenLBRACK
	TokenRBRACK
	TokenCOMMA
	TokenAT
	TokenPLUS
	TokenMINUS
	TokenTIMES
	TokenDIV
	TokenDIVINT
	TokenMODINT

	// The colon '' has a context specific meaning and is checked by the parser

	TOKENodeKEYWORDS // Used to separate keywords from other tokens in this list

	TokenGET
	TokenLOOKUP
	TokenFROM
	TokenGROUP
	TokenWITH
	TokenLIST
	TokenNULLTRAVERSAL
	TokenFILTERING
	TokenORDERING
	TokenWHERE
	TokenTRAVERSE
	TokenEND
	TokenPRIMARY
	TokenSHOW
	TokenAS
	TokenFORMAT
	TokenAND
	TokenOR
	TokenLIKE
	TokenIN
	TokenCONTAINS
	TokenBEGINSWITH
	TokenENDSWITH
	TokenCONTAINSNOT
	TokenNOT
	TokenNOTIN
	TokenFALSE
	TokenTRUE
	TokenUNIQUE
	TokenUNIQUECOUNT
	TokenNULL
	TokenISNULL
	TokenISNOTNULL
	TokenASCENDING
	TokenDESCENDING
)

/*
Available parser AST node types
*/
const (
	NodeEOF = "EOF"

	NodeVALUE         = "value"
	NodeTRUE          = "true"
	NodeFALSE         = "false"
	NodeNULL          = "null"
	NodeFUNC          = "func"
	NodeORDERING      = "ordering"
	NodeFILTERING     = "filtering"
	NodeNULLTRAVERSAL = "nulltraversal"

	// Special tokens - always handled in a denotation function

	NodeCOMMA  = "comma"
	NodeGROUP  = "group"
	NodeEND    = "end"
	NodeAS     = "as"
	NodeFORMAT = "format"

	// Keywords

	NodeGET    = "get"
	NodeLOOKUP = "lookup"
	NodeFROM   = "from"
	NodeWHERE  = "where"

	NodeUNIQUE      = "unique"
	NodeUNIQUECOUNT = "uniquecount"
	NodeISNOTNULL   = "isnotnull"
	NodeASCENDING   = "asc"
	NodeDESCENDING  = "desc"

	NodeTRAVERSE = "traverse"
	NodePRIMARY  = "primary"
	NodeSHOW     = "show"
	NodeSHOWTERM = "showterm"
	NodeWITH     = "with"
	NodeLIST     = "list"

	// Boolean operations

	NodeOR  = "or"
	NodeAND = "and"
	NodeNOT = "not"

	NodeGEQ = ">="
	NodeLEQ = "<="
	NodeNEQ = "!="
	NodeEQ  = "="
	NodeGT  = ">"
	NodeLT  = "<"

	// List operations

	NodeIN    = "in"
	NodeNOTIN = "notin"

	// String operations

	NodeLIKE        = "like"
	NodeCONTAINS    = "contains"
	NodeBEGINSWITH  = "beginswith"
	NodeENDSWITH    = "endswith"
	NodeCONTAINSNOT = "containsnot"

	// Simple arithmetic expressions

	NodePLUS   = "plus"
	NodeMINUS  = "minus"
	NodeTIMES  = "times"
	NodeDIV    = "div"
	NodeMODINT = "modint"
	NodeDIVINT = "divint"

	// Brackets

	NodeLPAREN = "("
	NodeRPAREN = ")"
	NodeLBRACK = "["
	NodeRBRACK = "]"
)
