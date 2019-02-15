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
	"sort"

	"devt.de/common/datautil"
)

/*
NodeCompare compares node attributes.
*/
func NodeCompare(node1 Node, node2 Node, attrs []string) bool {

	if attrs == nil {
		if len(node1.Data()) != len(node2.Data()) {
			return false
		}

		attrs = make([]string, 0, len(node1.Data()))

		for attr := range node1.Data() {
			attrs = append(attrs, attr)
		}
	}

	for _, attr := range attrs {
		if node1.Attr(attr) != node2.Attr(attr) {
			return false
		}
	}

	return true
}

/*
NodeClone clones a node.
*/
func NodeClone(node Node) Node {
	var data map[string]interface{}
	datautil.CopyObject(node.Data(), &data)
	return &graphNode{data}
}

/*
NodeMerge merges two nodes together in a third node. The node values are copied
by reference.
*/
func NodeMerge(node1 Node, node2 Node) Node {
	data := make(map[string]interface{})
	for k, v := range node1.Data() {
		data[k] = v
	}
	for k, v := range node2.Data() {
		data[k] = v
	}
	return &graphNode{data}
}

/*
NodeSort sorts a list of nodes.
*/
func NodeSort(list []Node) {
	sort.Sort(NodeSlice(list))
}

/*
NodeSlice attaches the methods of sort.Interface to []Node, sorting in
increasing order by key and kind.
*/
type NodeSlice []Node

/*
Len belongs to the sort.Interface.
*/
func (p NodeSlice) Len() int { return len(p) }

/*
Less belongs to the sort.Interface.
*/
func (p NodeSlice) Less(i, j int) bool {
	in := p[i]
	jn := p[j]
	if in.Kind() != jn.Kind() {
		return in.Kind() < jn.Kind()
	}
	return in.Key() < jn.Key()
}

/*
Swap belongs to the sort.Interface.
*/
func (p NodeSlice) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
