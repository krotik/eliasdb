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
Package data contains classes and functions to handle graph data.

Nodes

Nodes are items stored in the graph. The graphNode object is the minimal
implementation of the Node interface and represents a simple node. Setting a
nil value to an attribute is equivalent to removing the attribute. An attribute
value can be any object which can be serialized by gob.

Edges

Edges are items stored in the graph. Edges connect nodes. The graphEdge object
is the minimal implementation of the Edge interface and represents a simple edge.
Setting a nil value to an attribute is equivalent to removing the attribute. An
attribute value can be any object which can be serialized by gob.
*/
package data

import "fmt"

/*
Edge models edges in the graph
*/
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
		End1IsCascading is a flag to indicate that delete operations from this
		end are cascaded to the other end.
	*/
	End1IsCascading() bool

	/*
		End1IsCascadingLast is a flag to indicate that cascading delete
		operations are only executed if this is the last/only edge of
		this kind to the other end. The flag is ignored if End1IsCascading is
		false.
	*/
	End1IsCascadingLast() bool

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
		End2IsCascading is a flag to indicate that delete operations from this
		end are cascaded to the other end.
	*/
	End2IsCascading() bool

	/*
		End2IsCascadingLast is a flag to indicate that cascading delete
		operations are only executed if this is the last/only edge of
		this kind to the other end. The flag is ignored if End2IsCascading is
		false.
	*/
	End2IsCascadingLast() bool

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
EdgeEnd1Key is the key of the first end
*/
const EdgeEnd1Key = "end1key"

/*
EdgeEnd1Kind is the kind of the first end
*/
const EdgeEnd1Kind = "end1kind"

/*
EdgeEnd1Role is the role of the first end
*/
const EdgeEnd1Role = "end1role"

/*
EdgeEnd1Cascading is the flag to cascade delete operations from the first end
*/
const EdgeEnd1Cascading = "end1cascading"

/*
EdgeEnd1CascadingLast is a flag to indicate that cascading delete
operations are only executed on the last/only edge of
a kind
*/
const EdgeEnd1CascadingLast = "end1cascadinglast"

/*
EdgeEnd2Key is the key of the second end
*/
const EdgeEnd2Key = "end2key"

/*
EdgeEnd2Kind is the kind of the second end
*/
const EdgeEnd2Kind = "end2kind"

/*
EdgeEnd2Role is the role of the second end
*/
const EdgeEnd2Role = "end2role"

/*
EdgeEnd2Cascading is the flag to cascade delete operations from the second end
*/
const EdgeEnd2Cascading = "end2cascading"

/*
EdgeEnd2CascadingLast is a flag to indicate that cascading delete
operations are only executed on the last/only edge of
a kind
*/
const EdgeEnd2CascadingLast = "end2cascadinglast"

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
	return ge.stringAttr(EdgeEnd1Key)
}

/*
End1Kind returns the kind of the first end of this edge.
*/
func (ge *graphEdge) End1Kind() string {
	return ge.stringAttr(EdgeEnd1Kind)
}

/*
End1Role returns the role of the first end of this edge.
*/
func (ge *graphEdge) End1Role() string {
	return ge.stringAttr(EdgeEnd1Role)
}

/*
End1IsCascading is a flag to indicate that delete operations from this
end are cascaded to the other end.
*/
func (ge *graphEdge) End1IsCascading() bool {
	return ge.Attr(EdgeEnd1Cascading).(bool)
}

/*
End1IsCascadingLast is a flag to indicate that cascading delete
operations are only executed if this is the last/only edge of
this kind to the other end. The flag is ignored if End1IsCascading is
false.
*/
func (ge *graphEdge) End1IsCascadingLast() bool {
	a := ge.Attr(EdgeEnd1CascadingLast)
	return a != nil && a.(bool)
}

/*
End2Key returns the key of the second end of this edge.
*/
func (ge *graphEdge) End2Key() string {
	return ge.stringAttr(EdgeEnd2Key)
}

/*
End2Kind returns the kind of the second end of this edge.
*/
func (ge *graphEdge) End2Kind() string {
	return ge.stringAttr(EdgeEnd2Kind)
}

/*
End2Role returns the role of the second end of this edge.
*/
func (ge *graphEdge) End2Role() string {
	return ge.stringAttr(EdgeEnd2Role)
}

/*
End2IsCascading is a flag to indicate that delete operations from this
end are cascaded to the other end.
*/
func (ge *graphEdge) End2IsCascading() bool {
	return ge.Attr(EdgeEnd2Cascading).(bool)
}

/*
End2IsCascadingLast is a flag to indicate that cascading delete
operations are only executed if this is the last/only edge of
this kind to the other end. The flag is ignored if End2IsCascading is
false.
*/
func (ge *graphEdge) End2IsCascadingLast() bool {
	a := ge.Attr(EdgeEnd2CascadingLast)
	return a != nil && a.(bool)
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
		return attr == NodeKey || attr == NodeKind || attr == EdgeEnd1Key ||
			attr == EdgeEnd1Kind || attr == EdgeEnd1Role ||
			attr == EdgeEnd1Cascading || attr == EdgeEnd1CascadingLast ||
			attr == EdgeEnd2Key || attr == EdgeEnd2Kind || attr == EdgeEnd2Role ||
			attr == EdgeEnd2Cascading || attr == EdgeEnd2CascadingLast
	})
}

/*
String returns a string representation of this edge.
*/
func (ge *graphEdge) String() string {
	return dataToString("GraphEdge", ge.graphNode)
}
