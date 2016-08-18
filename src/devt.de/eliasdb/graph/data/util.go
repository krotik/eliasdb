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

import "devt.de/common/datautil"

/*
Function to compare node attributes.
*/
func NodeCompare(node1 Node, node2 Node, attrs []string) bool {

	if attrs == nil {
		if len(node1.Data()) != len(node2.Data()) {
			return false
		}

		attrs = make([]string, 0, len(node1.Data()))

		for attr, _ := range node1.Data() {
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
Function to clone a node.
*/
func NodeClone(node Node) Node {
	var data map[string]interface{}
	datautil.CopyObject(node.Data(), &data)
	return &graphNode{data}
}

/*
Merges two nodes together in a third node. The node values are copied by reference.
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
