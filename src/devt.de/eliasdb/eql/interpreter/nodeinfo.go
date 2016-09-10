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
	"sort"

	"devt.de/common/stringutil"
	"devt.de/eliasdb/graph"
	"devt.de/eliasdb/graph/data"
)

/*
NodeInfo interface. NodeInfo objects are used by the EQL interpreter to format
search results.
*/
type NodeInfo interface {
	/*
		SummaryAttributes returns the attributes which should be shown
		in a list view for a given node kind.
	*/
	SummaryAttributes(kind string) []string

	/*
	   Return the display string for a given attribute.
	*/
	AttributeDisplayString(kind string, attr string) string

	/*
		Check if a given string can be a valid node attribute.
	*/
	IsValidAttr(attr string) bool
}

/*
defaultNodeInfo data structure
*/
type defaultNodeInfo struct {
	gm *graph.Manager
}

/*
NewDefaultNodeInfo creates a new default NodeInfo instance. The default NodeInfo
provides the most generic rendering information to the interpreter.
*/
func NewDefaultNodeInfo(gm *graph.Manager) NodeInfo {
	return &defaultNodeInfo{gm}
}

/*
SummaryAttributes returns the attributes which should be shown
in a list view for a given node kind.
*/
func (ni *defaultNodeInfo) SummaryAttributes(kind string) []string {

	if kind == "" {
		return []string{data.NodeKey, data.NodeKind, data.NodeName}
	}

	attrs := ni.gm.NodeAttrs(kind)

	ret := make([]string, 0, len(attrs))
	for _, attr := range attrs {

		if attr == data.NodeKey || attr == data.NodeKind {
			continue
		}

		ret = append(ret, attr)
	}

	sort.StringSlice(ret).Sort()

	// Prepend the key attribute

	ret = append([]string{data.NodeKey}, ret...)

	return ret
}

/*
Return the display string for a given attribute.
*/
func (ni *defaultNodeInfo) AttributeDisplayString(kind string, attr string) string {
	if (attr == data.NodeKey || attr == data.NodeKind || attr == data.NodeName) && kind != "" {
		return stringutil.CreateDisplayString(kind) + " " +
			stringutil.CreateDisplayString(attr)
	}

	return stringutil.CreateDisplayString(attr)
}

/*
Check if a given string can be a valid node attribute.
*/
func (ni *defaultNodeInfo) IsValidAttr(attr string) bool {
	return ni.gm.IsValidAttr(attr)
}
