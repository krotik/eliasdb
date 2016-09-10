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
	"fmt"
	"strconv"
	"strings"

	"devt.de/eliasdb/eql/parser"
	"devt.de/eliasdb/graph"
	"devt.de/eliasdb/graph/data"
)

/*
allowMultiEval allows multiple calls to eval of runtime components without
resetting state (used for testing)
*/
var allowMultiEval = false

// Special flags which can be set by with statements

type withFlags struct {
	ordering     []byte // Result ordering
	orderingCol  []int  // Columns which should be ordered
	notnullCol   []int  // Columns which must not be null
	uniqueCol    []int  // Columns which will only contain unique values
	uniqueColCnt []bool // Flag if unique values should be counted
}

const (
	withOrderingAscending  = 0x1
	withOrderingDescending = 0x2
)

/*
GroupNodeKind is a special group node kind
*/
const GroupNodeKind = "group"

// General runtime provider
// ========================

/*
eqlRuntimeProvider defines the main interpreter
datastructure and all functions for general evaluation.
*/
type eqlRuntimeProvider struct {
	name       string         // Name to identify the input
	part       string         // Graph partition to query
	gm         *graph.Manager // GraphManager to operate on
	ni         NodeInfo       // NodeInfo to use for formatting
	groupScope string         // Group scope for query

	allowNilTraversal bool       // Flag if empty traversals should be included in the result
	withFlags         *withFlags // Special flags which can be set by with statements

	primaryKind  string                 // Primary node kind
	nextStartKey func() (string, error) // Function to get the next start key

	traversals []*parser.ASTNode // Array of all top level query traversals
	where      *parser.ASTNode   // First where clause
	show       *parser.ASTNode   // Show clause node

	specs      []string            // Flat list of traversals of this query
	attrsNodes []map[string]string // Attributes for nodes to query on each traversal
	attrsEdges []map[string]string // Attributes for nodes to query on each traversal
	rowNode    []data.Node         // Current row of nodes which is evaluated
	rowEdge    []data.Edge         // Current row of edges which is evaluated

	colLabels []string   // Labels for columns
	colFormat []string   // Format for columns
	colData   []string   // Data for columns
	colFunc   []FuncShow // Function to transform column value

	_attrsNodesFetch [][]string // Internal copy of attrsNodes better suited for fetchPart calls
	_attrsEdgesFetch [][]string // Internal copy of attrsEdges better suited for fetchPart calls
}

/*
Initialise and validate data structures.
*/
func (p *eqlRuntimeProvider) init(startKind string,
	rootChildren []*parser.ASTNode) error {

	// By default we don't include empty traversals in the result

	p.allowNilTraversal = false

	// Clear any with flags

	p.withFlags = &withFlags{make([]byte, 0), make([]int, 0), make([]int, 0),
		make([]int, 0), make([]bool, 0)}

	// Reinitialise datastructures

	p.groupScope = ""
	p.traversals = make([]*parser.ASTNode, 0)
	p.where = nil
	p.show = nil

	p.specs = make([]string, 0)
	p.attrsNodes = make([]map[string]string, 0)
	p.attrsEdges = make([]map[string]string, 0)
	p.rowNode = nil
	p.rowEdge = nil
	p._attrsNodesFetch = nil
	p._attrsEdgesFetch = nil

	p.colLabels = make([]string, 0)
	p.colFormat = make([]string, 0)
	p.colData = make([]string, 0)
	p.colFunc = make([]FuncShow, 0)

	p.primaryKind = ""

	p.specs = append(p.specs, startKind)
	p.attrsNodes = append(p.attrsNodes, make(map[string]string))
	p.attrsEdges = append(p.attrsEdges, make(map[string]string))

	// With clause is interpreted straight after finishing the columns

	var withChild *parser.ASTNode

	// Go through the children, check if they are valid and initialise them

	for _, child := range rootChildren {

		if child.Name == parser.NodeWHERE {

			// Check if the show clause or some traversals are already populated

			if p.show != nil || len(p.traversals) > 0 {
				return p.newRuntimeError(ErrInvalidConstruct,
					"condition must be before show clause and traversals", child)
			}

			// Reset state of where and store it

			if err := child.Runtime.Validate(); err != nil {
				return err
			}

			p.where = child

		} else if child.Name == parser.NodeTRAVERSE {

			// Check if show clause or where clause is already populated

			if p.show != nil {
				return p.newRuntimeError(ErrInvalidConstruct,
					"traversals must be before show clause", child)
			}

			// Reset state of traversal and add it to the traversal list

			if err := child.Runtime.Validate(); err != nil {
				return err
			}

			p.traversals = append(p.traversals, child)

		} else if child.Name == parser.NodeSHOW {

			p.show = child

		} else if child.Name == parser.NodeFROM {

			// Set the group state

			p.groupScope = child.Children[0].Children[0].Token.Val

		} else if child.Name == parser.NodePRIMARY {

			pk := child.Children[0].Token.Val

			for _, nk := range p.gm.NodeKinds() {
				if nk == pk {
					p.primaryKind = pk
				}
			}

			if p.primaryKind == "" {
				return p.newRuntimeError(ErrUnknownNodeKind, pk, child.Children[0])
			}

		} else if child.Name == parser.NodeWITH {

			withChild = child

		} else {

			return p.newRuntimeError(ErrInvalidConstruct, child.Name, child)
		}
	}

	// Populate column related attributes

	nodeKindPos, edgeKindPos, err := p.initCols()
	if err != nil {
		return err
	}

	// Interpret with clause straight after populating the columns

	if withChild != nil {
		if err := p.initWithFlags(withChild, nodeKindPos, edgeKindPos); err != nil {
			return err
		}
	}

	if p.primaryKind == "" {
		p.primaryKind = startKind
	}

	return nil
}

/*
initWithFlags populates the withFlags datastructure. It is assumed that the
columns have been populated before calling this function.
*/
func (p *eqlRuntimeProvider) initWithFlags(withNode *parser.ASTNode,
	nodeKindPos map[string][]int, edgeKindPos map[string][]int) error {

	// Helper function to find a specified column

	findColumn := func(colData string, node *parser.ASTNode) (int, error) {

		col := -1
		colDataSplit := strings.SplitN(colData, ":", 3)

		switch len(colDataSplit) {
		case 1:

			// Find the first column which displays the given attribute

			for i, cd := range p.colData {
				cds := strings.SplitN(cd, ":", 3)
				if cds[2] == colDataSplit[0] {
					col = i
				}
			}

		case 2:

			// Search for first kind / attribute occurrence

			kind := colDataSplit[0]
			attr := colDataSplit[1]

			searchColData := func(pos int, t string) {
				cstr := fmt.Sprint(pos+1, ":", t, ":", attr)

				for i, c := range p.colData {
					if c == cstr {
						col = i
					}
				}
			}

			if poslist, ok := nodeKindPos[kind]; ok {
				searchColData(poslist[0], "n")

			} else if poslist, ok := edgeKindPos[kind]; ok {
				searchColData(poslist[0], "e")

			} else {
				return -1, p.newRuntimeError(ErrInvalidConstruct,
					"Cannot determine column for with term: "+colData, node)
			}

		case 3:

			// Search for exact specification

			for i, c := range p.colData {
				if c == colData {
					col = i
				}
			}
		}

		if col == -1 {
			return -1, p.newRuntimeError(ErrInvalidConstruct,
				"Cannot determine column for with term: "+colData, node)
		}

		return col, nil
	}

	// Go through all children and initialise the withFlags data structure

	for _, child := range withNode.Children {

		if child.Name == parser.NodeNULLTRAVERSAL && child.Children[0].Name == parser.NodeTRUE {

			p.allowNilTraversal = true

		} else if child.Name == parser.NodeFILTERING {

			for _, child := range child.Children {

				if child.Name == parser.NodeISNOTNULL || child.Name == parser.NodeUNIQUE || child.Name == parser.NodeUNIQUECOUNT {

					c, err := findColumn(child.Children[0].Token.Val, child)
					if err != nil {
						return err
					}

					if child.Name == parser.NodeISNOTNULL {
						p.withFlags.notnullCol = append(p.withFlags.notnullCol, c)
					} else if child.Name == parser.NodeUNIQUE {
						p.withFlags.uniqueCol = append(p.withFlags.uniqueCol, c)
						p.withFlags.uniqueColCnt = append(p.withFlags.uniqueColCnt, false)
					} else if child.Name == parser.NodeUNIQUECOUNT {
						p.withFlags.uniqueCol = append(p.withFlags.uniqueCol, c)
						p.withFlags.uniqueColCnt = append(p.withFlags.uniqueColCnt, true)
					}
				} else {
					return p.newRuntimeError(ErrInvalidConstruct, child.Token.Val, child)
				}
			}

		} else if child.Name == parser.NodeORDERING {

			for _, child := range child.Children {

				if child.Name == parser.NodeASCENDING || child.Name == parser.NodeDESCENDING {

					c, err := findColumn(child.Children[0].Token.Val, child)
					if err != nil {
						return err
					}

					if child.Name == parser.NodeASCENDING {
						p.withFlags.ordering = append(p.withFlags.ordering, withOrderingAscending)
					} else {
						p.withFlags.ordering = append(p.withFlags.ordering, withOrderingDescending)
					}

					p.withFlags.orderingCol = append(p.withFlags.orderingCol, c)

				} else {
					return p.newRuntimeError(ErrInvalidConstruct, child.Token.Val, child)
				}
			}

		} else {
			return p.newRuntimeError(ErrInvalidConstruct, child.Token.Val, child)
		}
	}

	return nil
}

/*
initCols populates the column related attributes. This function assumes that
specs is filled with all necessary traversals.

The following formats for a show term are allowed:

<step>:<type>:<attr>  - Attribute from whatever is at the given traversal step
<kind>:<attr>         - First matching kind in a row provides the attribute
<attr>                - Show attribute from root node kind
*/
func (p *eqlRuntimeProvider) initCols() (map[string][]int, map[string][]int, error) {

	// Fill lookup maps for traversal kind positions
	// Show term match by kind uses these

	nodeKindPos := make(map[string][]int)
	edgeKindPos := make(map[string][]int)

	addPos := func(kmap map[string][]int, kind string, pos int) {
		if l, ok := kmap[kind]; ok {
			kmap[kind] = append(l, pos)
		} else {
			kmap[kind] = []int{pos}
		}
	}

	for i, spec := range p.specs {

		if i == 0 {
			addPos(nodeKindPos, spec, i)
		} else {
			sspec := strings.Split(spec, ":")

			if sspec[1] != "" {
				addPos(edgeKindPos, sspec[1], i)
			}
			if sspec[3] != "" {
				addPos(nodeKindPos, sspec[3], i)
			}
		}
	}

	// Fill up column lists

	if p.show == nil || len(p.show.Children) == 0 {

		// If no show clause is defined ask the NodeInfo to provide a summary list

		for i, spec := range p.specs {

			sspec := strings.Split(spec, ":")
			kind := sspec[len(sspec)-1]

			for _, attr := range p.ni.SummaryAttributes(kind) {

				// Make sure the attribute is in attrsNodes

				p.attrsNodes[i][attr] = ""

				// Fill col attributes (we only show nodes)

				p.colLabels = append(p.colLabels, p.ni.AttributeDisplayString(kind, attr))
				p.colFormat = append(p.colFormat, "auto")
				p.colData = append(p.colData, fmt.Sprintf("%v:n:%s", i+1, attr))
				p.colFunc = append(p.colFunc, nil)
			}
		}

	} else {

		var err error
		var attr, label, colData string
		var pos int
		var isNode bool
		var colFunc FuncShow

		// Go through the elements of the provided show clause

		for _, col := range p.show.Children {

			if col.Name != parser.NodeSHOWTERM {
				return nil, nil, p.newRuntimeError(ErrInvalidConstruct, col.Name, col)
			}

			// Reset label value

			label = ""
			colFunc = nil

			// Create the correct colData value

			if col.Token.ID == parser.TokenAT {

				// We have a function get the attribute which it operates on

				funcName := col.Children[0].Children[0].Token.Val

				funcInst, ok := showFunc[funcName]
				if !ok {
					return nil, nil, p.newRuntimeError(ErrInvalidConstruct,
						"Unknown function: "+funcName, col)
				}

				colFunc, colData, label, err = funcInst(col.Children[0], p)
				if err != nil {
					return nil, nil, p.newRuntimeError(ErrInvalidConstruct,
						err.Error(), col)
				}

			} else {
				colData = col.Token.Val
			}

			colDataSplit := strings.SplitN(colData, ":", 3)

			switch len(colDataSplit) {
			case 1:
				// Show attribute from root node kind

				attr = colDataSplit[0]
				pos = 0
				isNode = true
				colData = "1:n:" + attr
				if label == "" {
					label = p.ni.AttributeDisplayString(p.specs[0], attr)
				}

			case 2:
				// First matching kind in a row provides the attribute

				kind := colDataSplit[0]

				if poslist, ok := nodeKindPos[kind]; ok {
					attr = colDataSplit[1]
					pos = poslist[0]
					isNode = true
					colData = fmt.Sprint(pos+1) + ":n:" + attr

				} else if poslist, ok := edgeKindPos[kind]; ok {
					attr = colDataSplit[1]
					pos = poslist[0]
					isNode = false
					colData = fmt.Sprint(pos+1) + ":e:" + attr

				} else {

					return nil, nil, p.newRuntimeError(ErrInvalidConstruct,
						"Cannot determine data position for kind: "+kind, col)
				}

				if label == "" {
					label = p.ni.AttributeDisplayString(kind, attr)
				}

			case 3:
				// Attribute from whatever is at the given traversal step

				attr = colDataSplit[2]

				pos, err = strconv.Atoi(colDataSplit[0])
				if err != nil {
					return nil, nil, p.newRuntimeError(ErrInvalidConstruct,
						"Invalid data index: "+colData+" ("+err.Error()+")", col)
				} else if pos < 1 {
					return nil, nil, p.newRuntimeError(ErrInvalidConstruct,
						"Invalid data index: "+colData+" (index must be greater than 0)", col)
				}
				pos--

				if colDataSplit[1] == "n" {
					isNode = true
				} else if colDataSplit[1] == "e" {
					isNode = false
				} else {
					return nil, nil, p.newRuntimeError(ErrInvalidConstruct,
						"Invalid data source '"+colDataSplit[1]+"' (either n - Node or e - Edge)", col)
				}

				if label == "" {
					label = p.ni.AttributeDisplayString("", attr)
				}
			}

			if pos >= len(p.attrsNodes) {
				return nil, nil, p.newRuntimeError(ErrInvalidColData,
					fmt.Sprintf("Data index out of range: %v", pos+1), col)
			}

			// Determine label and format

			colLabel := label
			colFormat := "auto"

			for _, t := range col.Children {

				if t.Name == parser.NodeAS {
					colLabel = t.Children[0].Token.Val
				} else if t.Name == parser.NodeFORMAT {
					colFormat = t.Children[0].Token.Val
				} else if t.Name != parser.NodeFUNC {
					return nil, nil, p.newRuntimeError(ErrInvalidConstruct, t.Name, t)
				}
			}

			// Fill col attributes

			p.colLabels = append(p.colLabels, colLabel)
			p.colFormat = append(p.colFormat, colFormat)
			p.colData = append(p.colData, colData)
			p.colFunc = append(p.colFunc, colFunc)

			// Populate attrsNodes and attrsEdges

			if isNode {
				p.attrsNodes[pos][attr] = ""
			} else {
				p.attrsEdges[pos][attr] = ""
			}
		}
	}

	return nodeKindPos, edgeKindPos, nil
}

/*
next advances to the next query row. Returns false if no more rows are available.
It is assumed that all traversal specs and query attrs have been filled.
*/
func (p *eqlRuntimeProvider) next() (bool, error) {

	// Create fetch lists if it is the first next() call

	if p._attrsNodesFetch == nil {

		makeFetchList := func(attrs []map[string]string, isEdge bool) [][]string {
			var fetchlist [][]string

			for _, attrs := range attrs {
				var attrsFetch []string

				for attr := range attrs {

					// Condition needs to be different for nodes and edges

					if !isEdge && attr != "" && attr != data.NodeKey && attr != data.NodeKind {

						attrsFetch = append(attrsFetch, attr)

					} else if attr != "" && attr != data.NodeKey && attr != data.NodeKind &&
						attr != data.EdgeEnd1Key && attr != data.EdgeEnd1Kind &&
						attr != data.EdgeEnd1Role && attr != data.EdgeEnd1Cascading &&
						attr != data.EdgeEnd2Key && attr != data.EdgeEnd2Kind &&
						attr != data.EdgeEnd2Role && attr != data.EdgeEnd2Cascading {

						attrsFetch = append(attrsFetch, attr)
					}
				}

				fetchlist = append(fetchlist, attrsFetch)
			}

			return fetchlist
		}

		p._attrsNodesFetch = makeFetchList(p.attrsNodes, false)
		p._attrsEdgesFetch = makeFetchList(p.attrsEdges, true)
	}

	// Make sure we have the row and rowEdge arrays

	if p.rowNode == nil {
		p.rowNode = make([]data.Node, 0)
		p.rowEdge = make([]data.Edge, 0)
	}

	// Check if a traversal can handle the call

	for _, child := range p.traversals {
		childRuntime := child.Runtime.(*traversalRuntime)
		if childRuntime.hasMoreNodes() {
			_, err := childRuntime.Eval()
			return err == nil, err
		}
	}

	// Get next root node

	startKey, err := p.nextStartKey()
	if err != nil || startKey == "" {
		return false, err
	}

	// Fetch node - always require the key attribute
	// to make sure we get a node back if it exists

	node, err := p.gm.FetchNodePart(p.part, startKey, p.specs[0],
		append(p._attrsNodesFetch[0], "key"))

	if err != nil || node == nil {
		return false, err
	}

	// Decide if this node should be added

	addNode := true

	if p.where != nil {
		res, err := p.where.Runtime.(CondRuntime).CondEval(node, nil)
		if err != nil {
			return false, err
		}

		addNode = res.(bool)
	}

	if addNode {

		// Add node and the first traversal

		if len(p.rowNode) == 0 {
			p.rowNode = append(p.rowNode, node)
			p.rowEdge = append(p.rowEdge, nil)
		} else {

			// Clear out the row

			for i := range p.rowNode {
				p.rowNode[i] = nil
				p.rowEdge[i] = nil
			}

			// Fill in the first node

			p.rowNode[0] = node
			p.rowEdge[0] = nil
		}

		// Give the new source to the children and let them evaluate

		for _, child := range p.traversals {
			childRuntime := child.Runtime.(*traversalRuntime)

			if err := childRuntime.newSource(node); err == ErrEmptyTraversal {

				// If an empty traversal error comes back advance until
				// there is an element or the end

				p.rowNode[0] = nil
				p.rowEdge[0] = nil

				return p.next()

			} else if err != nil {
				return false, err
			}
		}

	} else {

		// Recursively call next until there is a condition-matching node or
		// there are no more start keys available

		return p.next()
	}

	return true, nil
}

/*
Instance function for general components
*/
type generalInst func(*eqlRuntimeProvider, *parser.ASTNode) parser.Runtime

/*
Runtime map for general components
*/
var generalProviderMap = map[string]generalInst{
	parser.NodeEOF:      invalidRuntimeInst,
	parser.NodeVALUE:    valueRuntimeInst,
	parser.NodeTRUE:     valueRuntimeInst,
	parser.NodeFALSE:    valueRuntimeInst,
	parser.NodeNULL:     valueRuntimeInst,
	parser.NodeLIST:     valueRuntimeInst,
	parser.NodeFUNC:     valueRuntimeInst,
	parser.NodeTRAVERSE: traversalRuntimeInst,
	parser.NodeWHERE:    whereRuntimeInst,

	// Condition components
	// ====================

	parser.NodeEQ:  equalRuntimeInst,
	parser.NodeNEQ: notEqualRuntimeInst,
	parser.NodeLT:  lessThanRuntimeInst,
	parser.NodeLEQ: lessThanEqualsRuntimeInst,
	parser.NodeGT:  greaterThanRuntimeInst,
	parser.NodeGEQ: greaterThanEqualsRuntimeInst,

	parser.NodeNOT: notRuntimeInst,
	parser.NodeAND: andRuntimeInst,
	parser.NodeOR:  orRuntimeInst,

	// Simple arithmetic expressions

	parser.NodePLUS:   plusRuntimeInst,
	parser.NodeMINUS:  minusRuntimeInst,
	parser.NodeTIMES:  timesRuntimeInst,
	parser.NodeDIV:    divRuntimeInst,
	parser.NodeMODINT: modIntRuntimeInst,
	parser.NodeDIVINT: divIntRuntimeInst,

	// List operations

	parser.NodeIN:    inRuntimeInst,
	parser.NodeNOTIN: notInRuntimeInst,

	// String operations

	parser.NodeLIKE:        likeRuntimeInst,
	parser.NodeCONTAINS:    containsRuntimeInst,
	parser.NodeCONTAINSNOT: containsNotRuntimeInst,
	parser.NodeBEGINSWITH:  beginsWithRuntimeInst,
	parser.NodeENDSWITH:    endsWithRuntimeInst,
}
