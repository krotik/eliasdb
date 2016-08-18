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
Edges are items stored in the graph. Edges connect nodes. The graphEdge object
is the minimal implementation of the Edge interface and represents a simple edge.
Edges have attributes which may or may not be presentable as a string. Setting a
nil value to an attribute is equivalent to removing the attribute.
*/
package data

import "fmt"

type Edge interface {
	Node

	/*
		End1Key returns the key of the first end of this edge.
	*/
	End1Key() string

	/*
		End1Kind returns the kind of the first end of this edge.
	*/
	End1Kind() string

	/*
		End1Role returns the role of the first end of this edge.
	*/
	End1Role() string

	/*
		Flag to indicate that delete operations from this end are cascaded
		to the other end.
	*/
	End1IsCascading() bool

	/*
		End2Key returns the key of the second end of this edge.
	*/
	End2Key() string

	/*
		End2Kind returns the kind of the second end of this edge.
	*/
	End2Kind() string

	/*
		End2Role returns the role of the second end of this edge.
	*/
	End2Role() string

	/*
		Flag to indicate that delete operations from this end are cascaded
		to the other end.
	*/
	End2IsCascading() bool

	/*
		Spec returns the spec for this edge from the view of a specified endpoint.
		A spec is always of the form: <End Role>:<Kind>:<End Role>:<Other node kind>
	*/
	Spec(key string) string

	/*
		OtherEndKey returns the key of the endpoint which is on the other side
		from the given key.
	*/
	OtherEndKey(key string) string

	/*
		OtherEndKind returns the kind of the endpoint which is on the other side
		from the given key.
	*/
	OtherEndKind(key string) string
}

/*
Key of the first end
*/
const EDGE_END1_KEY = "end1key"

/*
Kind of the first end
*/
const EDGE_END1_KIND = "end1kind"

/*
Role of the first end
*/
const EDGE_END1_ROLE = "end1role"

/*
Flag to cascade delete operations from the first end
*/
const EDGE_END1_CASCADING = "end1cascading"

/*
Key of the second end
*/
const EDGE_END2_KEY = "end2key"

/*
Kind of the second end
*/
const EDGE_END2_KIND = "end2kind"

/*
Role of the second end
*/
const EDGE_END2_ROLE = "end2role"

/*
Flag to cascade delete operations from the second end
*/
const EDGE_END2_CASCADING = "end2cascading"

/*
graphEdge data structure.
*/
type graphEdge struct {
	*graphNode
}

/*
NewGraphEdge creates a new Edge instance.
*/
func NewGraphEdge() Edge {
	return &graphEdge{&graphNode{make(map[string]interface{})}}
}

/*
NewGraphEdgeFromNode creates a new Edge instance.
*/
func NewGraphEdgeFromNode(node Node) Edge {
	if node == nil {
		return nil
	}
	return &graphEdge{&graphNode{node.Data()}}
}

/*
End1Key returns the key of the first end of this edge.
*/
func (ge *graphEdge) End1Key() string {
	return ge.stringAttr(EDGE_END1_KEY)
}

/*
	End1Kind returns the kind of the first end of this edge.
*/
func (ge *graphEdge) End1Kind() string {
	return ge.stringAttr(EDGE_END1_KIND)
}

/*
	End1Role returns the role of the first end of this edge.
*/
func (ge *graphEdge) End1Role() string {
	return ge.stringAttr(EDGE_END1_ROLE)
}

/*
	Flag to indicate that delete operations from this end are cascaded
	to the other end.
*/
func (ge *graphEdge) End1IsCascading() bool {
	return ge.Attr(EDGE_END1_CASCADING).(bool)
}

/*
	End2Key returns the key of the second end of this edge.
*/
func (ge *graphEdge) End2Key() string {
	return ge.stringAttr(EDGE_END2_KEY)
}

/*
	End2Kind returns the kind of the second end of this edge.
*/
func (ge *graphEdge) End2Kind() string {
	return ge.stringAttr(EDGE_END2_KIND)
}

/*
	End2Role returns the role of the second end of this edge.
*/
func (ge *graphEdge) End2Role() string {
	return ge.stringAttr(EDGE_END2_ROLE)
}

/*
	Flag to indicate that delete operations from this end are cascaded
	to the other end.
*/
func (ge *graphEdge) End2IsCascading() bool {
	return ge.Attr(EDGE_END2_CASCADING).(bool)
}

/*
Spec returns the spec for this edge from the view of a specified endpoint.
A spec is always of the form: <End Role>:<Kind>:<End Role>:<Other node kind>
*/
func (ge *graphEdge) Spec(key string) string {
	if key == ge.End1Key() {
		return fmt.Sprintf("%s:%s:%s:%s", ge.End1Role(), ge.Kind(), ge.End2Role(), ge.End2Kind())
	} else if key == ge.End2Key() {
		return fmt.Sprintf("%s:%s:%s:%s", ge.End2Role(), ge.Kind(), ge.End1Role(), ge.End1Kind())
	}
	return ""
}

/*
OtherEndKey returns the key of the endpoint which is on the other side
from the given key.
*/
func (ge *graphEdge) OtherEndKey(key string) string {
	if key == ge.End1Key() {
		return ge.End2Key()
	} else if key == ge.End2Key() {
		return ge.End1Key()
	}
	return ""
}

/*
OtherEndKind returns the kind of the endpoint which is on the other side
from the given key.
*/
func (ge *graphEdge) OtherEndKind(key string) string {
	if key == ge.End1Key() {
		return ge.End2Kind()
	} else if key == ge.End2Key() {
		return ge.End1Kind()
	}
	return ""
}

/*
IndexMap returns a representation of this node as a string map which
can be used to provide a full-text search.
*/
func (ge *graphEdge) IndexMap() map[string]string {
	return createIndexMap(ge.graphNode, func(attr string) bool {
		return attr == NODE_KEY || attr == NODE_KIND || attr == EDGE_END1_KEY ||
			attr == EDGE_END1_KIND || attr == EDGE_END1_ROLE || attr == EDGE_END1_CASCADING ||
			attr == EDGE_END2_KEY || attr == EDGE_END2_KIND || attr == EDGE_END2_ROLE ||
			attr == EDGE_END2_CASCADING
	})
}

/*
String returns a string representation of this edge.
*/
func (ge *graphEdge) String() string {
	return dataToString("GraphEdge", ge.graphNode)
}
