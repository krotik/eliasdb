/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package data

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

/*
Node models nodes in the graph
*/
type Node interface {

	/*
	   Key returns a potentially non human-readable unique key for this node.
	*/
	Key() string

	/*
	   Name returns a human-readable name for this node.
	*/
	Name() string

	/*
	   Kind returns a human-readable kind for this node.
	*/
	Kind() string

	/*
		Data returns the node data of this node.
	*/
	Data() map[string]interface{}

	/*
		Attr returns an attribute of this node.
	*/
	Attr(attr string) interface{}

	/*
		SetAttr sets an attribute of this node. Setting a nil
		value removes the attribute.
	*/
	SetAttr(attr string, val interface{})

	/*
		IndexMap returns a representation of this node as a string map which
		can be used to provide a full-text search.
	*/
	IndexMap() map[string]string

	/*
	   String returns a string representation of this node.
	*/
	String() string
}

/*
NodeKey is the key attribute for a node
*/
const NodeKey = "key"

/*
NodeName is the name attribute for a node
*/
const NodeName = "name"

/*
NodeKind is the kind attribute for a node
*/
const NodeKind = "kind"

/*
CopyNode returns a shallow copy of a given node.
*/
func CopyNode(node Node) Node {
	ret := NewGraphNode()
	for k, v := range node.Data() {
		ret.SetAttr(k, v)
	}
	return ret
}

/*
graphNode data structure.
*/
type graphNode struct {
	data map[string]interface{} // Data which is held by this node
}

/*
NewGraphNode creates a new Node instance.
*/
func NewGraphNode() Node {
	return &graphNode{make(map[string]interface{})}
}

/*
NewGraphNodeFromMap creates a new Node instance.
*/
func NewGraphNodeFromMap(data map[string]interface{}) Node {
	return &graphNode{data}
}

/*
Key returns a potentially non human-readable unique key for this node.
*/
func (gn *graphNode) Key() string {
	return gn.stringAttr(NodeKey)
}

/*
Kind returns a human-readable kind for this node.
*/
func (gn *graphNode) Kind() string {
	return gn.stringAttr(NodeKind)
}

/*
Data returns the node data of this node.
*/
func (gn *graphNode) Data() map[string]interface{} {
	return gn.data
}

/*
Name returns a human-readable name for this node.
*/
func (gn *graphNode) Name() string {
	return gn.stringAttr(NodeName)
}

/*
Attr returns an attribute of this node.
*/
func (gn *graphNode) Attr(attr string) interface{} {
	val, _ := gn.data[attr]
	return val
}

/*
SetAttr sets an attribute of this node. Setting a nil
value removes the attribute.
*/
func (gn *graphNode) SetAttr(attr string, val interface{}) {
	if val != nil {
		gn.data[attr] = val
	} else {
		delete(gn.data, attr)
	}
}

/*
Return the value of an attribute as a string. Or an
empty string if it can't be represented as a string.
*/
func (gn *graphNode) stringAttr(attr string) string {
	val, found := gn.data[attr]

	if st, ok := val.(string); found && ok {
		return st
	} else if st, ok := val.(fmt.Stringer); found && ok {
		return st.String()
	} else if found {
		return fmt.Sprintf("%v", val)
	}

	return ""
}

/*
IndexMap returns a representation of this node as a string map which
can be used to provide a full-text search.
*/
func (gn *graphNode) IndexMap() map[string]string {
	return createIndexMap(gn, func(attr string) bool {
		return attr == NodeKey || attr == NodeKind
	})
}

/*
createIndexMap creates a representation of a node as a string map. A filter
function can be specified to filters out specific attributes.
*/
func createIndexMap(gn *graphNode, attFilter func(attr string) bool) map[string]string {
	var addMap func(prefix string, data map[string]interface{})

	ret := make(map[string]string)

	addMap = func(prefix string, data map[string]interface{}) {

		for key, val := range data {
			attr := prefix + key

			// Ignore attributes which are uninteresting for a full-text search

			if attFilter(attr) {
				continue
			}

			// Detect nested structures and recurse into them

			if valmap, ok := val.(map[string]interface{}); ok {
				addMap(prefix+key+".", valmap)
			}

			// See the type of val and print it accordingly - ignore byte slices

			if st, ok := val.(string); ok {

				// Value is actually a string - no change needed

				ret[attr] = st

			} else if st, ok := val.(fmt.Stringer); ok {

				// Value has a proper string representation - use that

				ret[attr] = st.String()

			} else if _, ok := val.([]byte); !ok {

				// For all other cases (except ignored byte slices) try first a
				// JSON representation

				jsonBytes, err := json.Marshal(val)
				jsonString := string(jsonBytes)

				if err == nil && jsonString != "{}" {

					ret[attr] = string(jsonString)

				} else {

					// Otherwise do best effort printing

					ret[attr] = fmt.Sprintf("%v", val)
				}
			}
		}
	}

	addMap("", gn.data)

	return ret
}

/*
String returns a string representation of this node.
*/
func (gn *graphNode) String() string {
	return dataToString("GraphNode", gn)
}

/*
dataToString returns a string representation of a data item.
*/
func dataToString(dataType string, gn *graphNode) string {
	var buf bytes.Buffer
	attrlist := make([]string, 0, len(gn.data))
	maxlen := 0

	for attr := range gn.data {
		attrlist = append(attrlist, attr)
		if alen := len(attr); alen > maxlen {
			maxlen = alen
		}
	}

	sort.StringSlice(attrlist).Sort()

	buf.WriteString(dataType + ":\n")

	buf.WriteString(fmt.Sprintf("    %"+
		strconv.Itoa(maxlen)+"v : %v\n", "key", gn.Key()))
	buf.WriteString(fmt.Sprintf("    %"+
		strconv.Itoa(maxlen)+"v : %v\n", "kind", gn.Kind()))

	for _, attr := range attrlist {
		if attr == NodeKey || attr == NodeKind {
			continue
		}
		buf.WriteString(fmt.Sprintf("    %"+
			strconv.Itoa(maxlen)+"v : %v\n", attr, gn.data[attr]))
	}

	return buf.String()
}
