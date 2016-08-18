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
A graph manager handles the graph storage and provides the API for
the graph database.

Names database
==============
Names can be encoded (into a number) or decoded (into a string)

32 bit values for any given node attribute names
16 bit values for any given edge role names
16 bit values for any given edge kind names

Nodes database
==============
Each node kind database stores:

PREFIX_NS_ATTRS + node key -> [ ATTRS ]
(a list of attributes of a certain node)

PREFIX_NS_ATTR +  node key + attr num -> value
(attribute value of a certain node)

PREFIX_NS_SPECS + node key -> map[spec]""
(a lookup for available specs for a certain node)

PREFIX_NS_EDGE + node key + spec -> map[edge key]edgeinfo{other node key, other node kind}]
(connection from one node to another via a spec)

Edges database
==============
Each edge kind database stores:

PREFIX_NS_ATTRS + edge key -> [ ATTRS ]
(a list of attributes of a certain edge)

PREFIX_NS_ATTR + edge key + attr num -> value
(attribute value of a certain edge)

Index database
==============
This is managed by util/indexmanager.go
*/
package graph

import (
	"fmt"
	"sort"
	"strconv"
	"sync"

	"devt.de/eliasdb/graph/data"
	"devt.de/eliasdb/graph/graphstorage"
	"devt.de/eliasdb/graph/util"
)

/*
GraphManager data structure
*/
type GraphManager struct {
	gs       graphstorage.GraphStorage    // Graph storage of this graph manager
	gr       *graphRulesManager           // Manager for graph rules
	nm       *util.NamesManager           // Manager object which manages name encodings
	mapCache map[string]map[string]string // Cache which caches maps stored in the main database
	mutex    *sync.RWMutex                // Mutex to protect atomic graph operations
}

/*
NewGraphManager returns a new GraphManager instance.
*/
func NewGraphManager(gs graphstorage.GraphStorage) *GraphManager {
	gm := createGraphManager(gs)

	gm.SetGraphRule(&SystemRuleDeleteNodeEdges{})
	gm.SetGraphRule(&SystemRuleUpdateNodeStats{})

	return gm
}

/*
createGraphManager creates a new GraphManager instance.
*/
func createGraphManager(gs graphstorage.GraphStorage) *GraphManager {

	mdb := gs.MainDB()

	// Check version

	if version, ok := mdb[MAINDB_VERSION]; !ok {

		mdb[MAINDB_VERSION] = strconv.Itoa(VERSION)
		gs.FlushMain()

	} else {

		if v, _ := strconv.Atoi(version); v > VERSION {

			panic(fmt.Sprintf("Cannot open graph storage of version: %v - "+
				"max supported version: %v", version, VERSION))

		} else if v < VERSION {

			// Update the version if it is older

			mdb[MAINDB_VERSION] = strconv.Itoa(VERSION)
			gs.FlushMain()
		}
	}

	gm := &GraphManager{gs, &graphRulesManager{nil, make(map[string]GraphRule),
		make(map[int]map[string]GraphRule)}, util.NewNamesManager(mdb),
		make(map[string]map[string]string), &sync.RWMutex{}}

	gm.gr.gm = gm

	return gm
}

/*
Name returns the name of this graph manager.
*/
func (gm *GraphManager) Name() string {
	return fmt.Sprint("Graph ", gm.gs.Name())
}

/*
SetGraphRule sets a GraphRule.
*/
func (gm *GraphManager) SetGraphRule(rule GraphRule) {
	gm.gr.SetGraphRule(rule)
}

/*
GraphRules returns a list of all available graph rules.
*/
func (gm *GraphManager) GraphRules() []string {
	return gm.gr.GraphRules()
}

/*
NodeIndexQuery returns an object to query the full text search index for nodes.
*/
func (gm *GraphManager) NodeIndexQuery(part string, kind string) (IndexQuery, error) {
	iht, err := gm.getNodeIndexHTree(part, kind, false)
	if err != nil || iht == nil {
		return nil, err
	}

	return util.NewIndexManager(iht), nil
}

/*
EdgeIndexQuery returns an object to query the full text search index for edges.
*/
func (gm *GraphManager) EdgeIndexQuery(part string, kind string) (IndexQuery, error) {
	iht, err := gm.getEdgeIndexHTree(part, kind, false)
	if err != nil || iht == nil {
		return nil, err
	}

	return util.NewIndexManager(iht), nil
}

/*
Partitions returns all existing partitions.
*/
func (gm *GraphManager) Partitions() []string {
	return gm.mainStringList(MAINDB_PARTS)
}

/*
NodeKinds returns all possible node kinds.
*/
func (gm *GraphManager) NodeKinds() []string {
	return gm.mainStringList(MAINDB_NODE_KINDS)
}

/*
EdgeKinds returns all possible node kinds.
*/
func (gm *GraphManager) EdgeKinds() []string {
	return gm.mainStringList(MAINDB_EDGE_KINDS)
}

/*
NodeAttrs returns all possible node attributes for a given node kind.
*/
func (gm *GraphManager) NodeAttrs(kind string) []string {
	return gm.mainStringList(MAINDB_NODE_ATTRS + kind)
}

/*
NodeEdges returns all possible node edge specs for a given node kind.
*/
func (gm *GraphManager) NodeEdges(kind string) []string {
	return gm.mainStringList(MAINDB_NODE_EDGES + kind)
}

/*
EdgeAttrs returns all possible edge attributes for a given edge kind.
*/
func (gm *GraphManager) EdgeAttrs(kind string) []string {
	return gm.mainStringList(MAINDB_EDGE_ATTRS + kind)
}

/*
mainStringList return a list in the MainDB.
*/
func (gm *GraphManager) mainStringList(name string) []string {
	items := gm.getMainDBMap(name)

	ret := make([]string, 0)

	if items != nil {
		for item, _ := range items {
			ret = append(ret, item)
		}
	}

	sort.StringSlice(ret).Sort()

	return ret
}

/*
Check if a given string can be a valid node attribute.
*/
func (gm *GraphManager) IsValidAttr(attr string) bool {
	return gm.nm.Encode32(attr, false) != "" ||
		attr == data.NODE_KEY || attr == data.NODE_KIND ||
		attr == data.EDGE_END1_KEY || attr == data.EDGE_END1_KIND ||
		attr == data.EDGE_END1_ROLE || attr == data.EDGE_END1_CASCADING ||
		attr == data.EDGE_END2_KEY || attr == data.EDGE_END2_KIND ||
		attr == data.EDGE_END2_ROLE || attr == data.EDGE_END2_CASCADING
}
