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
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"devt.de/krotik/common/errorutil"
	"devt.de/krotik/common/stringutil"
)

/*
Map of pretty printer templates for AST nodes

There is special treatment for NodeVALUE, NodeGET, NodeLOOKUP, NodeTRAVERSE,
NodeFUNC, NodeSHOW, NodeSHOWTERM, NodeORDERING, NodeFILTERING, NodeWITH,
NodeLPAREN, NodeRPAREN, NodeLBRACK and NodeRBRACK.
*/
var prettyPrinterMap = map[string]*template.Template{
	NodeTRUE:                 template.Must(template.New(NodeTRUE).Parse("true")),
	NodeFALSE:                template.Must(template.New(NodeFALSE).Parse("false")),
	NodeNULL:                 template.Must(template.New(NodeNULL).Parse("null")),
	NodeNULLTRAVERSAL + "_1": template.Must(template.New(NodeNULLTRAVERSAL).Parse("nulltraversal({{.c1}})")),

	// Special tokens - always handled in a denotation function

	NodeGROUP + "_1":  template.Must(template.New(NodeGROUP).Parse("group {{.c1}}")),
	NodeEND:           template.Must(template.New(NodeEND).Parse("end")),
	NodeAS + "_1":     template.Must(template.New(NodeAS).Parse("as {{.c1}}")),
	NodeFORMAT + "_1": template.Must(template.New(NodeFORMAT).Parse("format {{.c1}}")),

	// Keywords

	NodeFROM + "_1":  template.Must(template.New(NodeFROM).Parse("from {{.c1}}")),
	NodeWHERE + "_1": template.Must(template.New(NodeWHERE).Parse("where {{.c1}}")),

	NodeUNIQUE + "_1":      template.Must(template.New(NodeUNIQUE).Parse("unique {{.c1}}")),
	NodeUNIQUECOUNT + "_1": template.Must(template.New(NodeUNIQUECOUNT).Parse("uniquecount {{.c1}}")),
	NodeISNOTNULL + "_1":   template.Must(template.New(NodeISNOTNULL).Parse("isnotnull {{.c1}}")),
	NodeASCENDING + "_1":   template.Must(template.New(NodeASCENDING).Parse("ascending {{.c1}}")),
	NodeDESCENDING + "_1":  template.Must(template.New(NodeDESCENDING).Parse("descending {{.c1}}")),

	NodePRIMARY + "_1": template.Must(template.New(NodePRIMARY).Parse("primary {{.c1}}")),
	NodeLIST:           template.Must(template.New(NodeLIST).Parse("list")),

	// Boolean operations

	NodeNOT + "_1": template.Must(template.New(NodeNOT).Parse("not {{.c1}}")),

	NodeGEQ + "_2": template.Must(template.New(NodeGEQ).Parse("{{.c1}} >= {{.c2}}")),
	NodeLEQ + "_2": template.Must(template.New(NodeLEQ).Parse("{{.c1}} <= {{.c2}}")),
	NodeNEQ + "_2": template.Must(template.New(NodeNEQ).Parse("{{.c1}} != {{.c2}}")),
	NodeEQ + "_2":  template.Must(template.New(NodeEQ).Parse("{{.c1}} = {{.c2}}")),
	NodeGT + "_2":  template.Must(template.New(NodeGT).Parse("{{.c1}} > {{.c2}}")),
	NodeLT + "_2":  template.Must(template.New(NodeLT).Parse("{{.c1}} < {{.c2}}")),

	// List operations

	NodeIN + "_2":    template.Must(template.New(NodeIN).Parse("{{.c1}} in {{.c2}}")),
	NodeNOTIN + "_2": template.Must(template.New(NodeNOTIN).Parse("{{.c1}} notin {{.c2}}")),

	// String operations

	NodeLIKE + "_2":        template.Must(template.New(NodeLIKE).Parse("{{.c1}} like {{.c2}}")),
	NodeCONTAINS + "_2":    template.Must(template.New(NodeCONTAINS).Parse("{{.c1}} contains {{.c2}}")),
	NodeBEGINSWITH + "_2":  template.Must(template.New(NodeBEGINSWITH).Parse("{{.c1}} beginswith {{.c2}}")),
	NodeENDSWITH + "_2":    template.Must(template.New(NodeENDSWITH).Parse("{{.c1}} endswith {{.c2}}")),
	NodeCONTAINSNOT + "_2": template.Must(template.New(NodeCONTAINSNOT).Parse("{{.c1}} containsnot {{.c2}}")),

	// Simple arithmetic expressions

	NodePLUS + "_2":   template.Must(template.New(NodePLUS).Parse("{{.c1}} + {{.c2}}")),
	NodeMINUS + "_1":  template.Must(template.New(NodeMINUS).Parse("-{{.c1}}")),
	NodeMINUS + "_2":  template.Must(template.New(NodeMINUS).Parse("{{.c1}} - {{.c2}}")),
	NodeTIMES + "_2":  template.Must(template.New(NodeTIMES).Parse("{{.c1}} * {{.c2}}")),
	NodeDIV + "_2":    template.Must(template.New(NodeDIV).Parse("{{.c1}} / {{.c2}}")),
	NodeMODINT + "_2": template.Must(template.New(NodeMODINT).Parse("{{.c1}} % {{.c2}}")),
	NodeDIVINT + "_2": template.Must(template.New(NodeDIVINT).Parse("{{.c1}} // {{.c2}}")),
}

/*
Map of nodes where the precedence might have changed because of parentheses
*/
var bracketPrecedenceMap = map[string]bool{
	NodePLUS:  true,
	NodeMINUS: true,
	NodeAND:   true,
	NodeOR:    true,
}

/*
PrettyPrint produces a pretty printed EQL query from a given AST.
*/
func PrettyPrint(ast *ASTNode) (string, error) {
	var visit func(ast *ASTNode, level int) (string, error)

	quoteValue := func(val string, allowNonQuotation bool) string {

		if val == "" {
			return `""`
		}

		isNumber, _ := regexp.MatchString("^[0-9][0-9\\.e-+]*$", val)
		isInlineString, _ := regexp.MatchString("^[a-zA-Z0-9_:.]*$", val)

		if allowNonQuotation && (isNumber || isInlineString) {
			return val
		} else if strings.ContainsRune(val, '"') {
			if strings.ContainsRune(val, '\'') {
				val = strings.Replace(val, "\"", "\\\"", -1)
			} else {
				return fmt.Sprintf("'%v'", val)
			}
		}
		return fmt.Sprintf("\"%v\"", val)
	}

	visit = func(ast *ASTNode, level int) (string, error) {

		// Handle special cases which don't have children

		if ast.Name == NodeVALUE || (ast.Name == NodeSHOWTERM && len(ast.Children) == 0) {
			return quoteValue(ast.Token.Val, true), nil
		}

		var children map[string]string
		var tempKey = ast.Name
		var buf bytes.Buffer

		// First pretty print children

		if len(ast.Children) > 0 {
			children = make(map[string]string)
			for i, child := range ast.Children {
				res, err := visit(child, level+1)
				if err != nil {
					return "", err
				}

				if _, ok := bracketPrecedenceMap[child.Name]; ok && ast.binding > child.binding {
					res = fmt.Sprintf("(%v)", res)
				}

				children[fmt.Sprint("c", i+1)] = res
			}

			tempKey += fmt.Sprint("_", len(children))
		}

		// Handle special cases requiring children

		if ast.Name == NodeLIST {

			buf.WriteString("[")
			if children != nil {
				i := 1
				for ; i < len(children); i++ {
					buf.WriteString(children[fmt.Sprint("c", i)])
					buf.WriteString(", ")
				}
				buf.WriteString(children[fmt.Sprint("c", i)])
			}
			buf.WriteString("]")
			return buf.String(), nil

		} else if ast.Name == NodeLOOKUP {

			buf.WriteString("lookup ")
			buf.WriteString(children["c1"])
			if 1 < len(children) {
				buf.WriteString(" ")
			}

			i := 1
			for ; i < len(children) && ast.Children[i].Name == NodeVALUE; i++ {
				buf.WriteString(quoteValue(ast.Children[i].Token.Val, false))

				if i < len(children)-1 && ast.Children[i+1].Name == NodeVALUE {
					buf.WriteString(", ")
				}
			}

			if i < len(children) {
				buf.WriteString(" ")
			}

			for ; i < len(children); i++ {
				buf.WriteString(children[fmt.Sprint("c", i+1)])
				if i < len(children)-1 && ast.Children[i+1].Name != NodeSHOW {
					buf.WriteString(" ")
				}
			}

			return buf.String(), nil

		} else if ast.Name == NodeGET {

			buf.WriteString("get ")
			buf.WriteString(children["c1"])
			if 1 < len(children) {
				buf.WriteString(" ")
			}

			for i := 1; i < len(children); i++ {
				buf.WriteString(children[fmt.Sprint("c", i+1)])
				if i < len(children)-1 && ast.Children[i+1].Name != NodeSHOW {
					buf.WriteString(" ")
				}
			}

			return buf.String(), nil

		} else if ast.Name == NodeTRAVERSE {

			buf.WriteString("\n")
			buf.WriteString(stringutil.GenerateRollingString(" ", level*2))
			buf.WriteString("traverse ")

			for i := 0; i < len(children); i++ {
				buf.WriteString(children[fmt.Sprint("c", i+1)])
				if i < len(children)-1 {
					buf.WriteString(" ")
				}
			}

			buf.WriteString("\n")
			buf.WriteString(stringutil.GenerateRollingString(" ", level*2))
			buf.WriteString("end")

			return buf.String(), nil

		} else if ast.Name == NodeFUNC {

			buf.WriteString("@")
			buf.WriteString(children["c1"])
			buf.WriteString("(")

			for i := 1; i < len(children); i++ {
				buf.WriteString(children[fmt.Sprint("c", i+1)])
				if i < len(children)-1 {
					buf.WriteString(", ")
				}
			}

			buf.WriteString(")")

			return buf.String(), nil

		} else if ast.Name == NodeSHOW {

			buf.WriteString("\nshow\n  ")

			for i := 0; i < len(children); i++ {
				buf.WriteString(children[fmt.Sprint("c", i+1)])
				if i < len(children)-1 {
					buf.WriteString(",\n  ")
				}
			}

			return buf.String(), nil

		} else if ast.Name == NodeSHOWTERM {

			if ast.Token.Val != "" && ast.Token.Val != "@" {
				buf.WriteString(quoteValue(ast.Token.Val, true))
				buf.WriteString(" ")
			}

			for i := 0; i < len(children); i++ {
				buf.WriteString(children[fmt.Sprint("c", i+1)])
				if i < len(children)-1 {
					buf.WriteString(" ")
				}
			}

			return buf.String(), nil

		} else if ast.Name == NodeORDERING {

			buf.WriteString("ordering")
			buf.WriteString("(")

			for i := 0; i < len(children); i++ {
				buf.WriteString(children[fmt.Sprint("c", i+1)])
				if i < len(children)-1 {
					buf.WriteString(", ")
				}
			}

			buf.WriteString(")")

			return buf.String(), nil

		} else if ast.Name == NodeFILTERING {

			buf.WriteString("filtering")
			buf.WriteString("(")

			for i := 0; i < len(children); i++ {
				buf.WriteString(children[fmt.Sprint("c", i+1)])
				if i < len(children)-1 {
					buf.WriteString(", ")
				}
			}

			buf.WriteString(")")

			return buf.String(), nil

		} else if ast.Name == NodeWITH {

			buf.WriteString("\nwith\n")

			for i := 0; i < len(children); i++ {
				buf.WriteString("  ")
				buf.WriteString(children[fmt.Sprint("c", i+1)])
				if i < len(children)-1 {
					buf.WriteString(",\n")
				}
			}

			return buf.String(), nil

		} else if ast.Name == NodeAND || ast.Name == NodeOR {

			for i := 0; i < len(children); i++ {
				buf.WriteString(children[fmt.Sprint("c", i+1)])
				if i < len(children)-1 {
					buf.WriteString(" ")
					buf.WriteString(strings.ToLower(ast.Token.Val))
					buf.WriteString(" ")
				}
			}

			return buf.String(), nil
		}

		// Retrieve the template

		temp, ok := prettyPrinterMap[tempKey]
		if !ok {
			return "", fmt.Errorf("Could not find template for %v (tempkey: %v)",
				ast.Name, tempKey)
		}

		// Use the children as parameters for template

		errorutil.AssertOk(temp.Execute(&buf, children))

		return buf.String(), nil
	}

	return visit(ast, 0)
}
