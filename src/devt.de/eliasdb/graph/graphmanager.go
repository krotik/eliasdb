/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
Manager data structure
*/
type Manager struct {
	gs           graphstorage.Storage         // Graph storage of this graph manager
	gr           *graphRulesManager           // Manager for graph rules
	nm           *util.NamesManager           // Manager object which manages name encodings
	mapCache     map[string]map[string]string // Cache which caches maps stored in the main database
	mutex        *sync.RWMutex                // Mutex to protect atomic graph operations
	storageMutex *sync.Mutex                  // Special mutex for storage object access
}

/*
NewGraphManager returns a new GraphManager instance.
*/
func NewGraphManager(gs graphstorage.Storage) *Manager {
	gm := createGraphManager(gs)

	gm.SetGraphRule(&SystemRuleDeleteNodeEdges{})
	gm.SetGraphRule(&SystemRuleUpdateNodeStats{})

	return gm
}

/*
createGraphManager creates a new GraphManager instance.
*/
func createGraphManager(gs graphstorage.Storage) *Manager {

	mdb := gs.MainDB()

	// Check version

	if version, ok := mdb[MainDBVersion]; !ok {

		mdb[MainDBVersion] = strconv.Itoa(VERSION)
		gs.FlushMain()

	} else {

		if v, _ := strconv.Atoi(version); v > VERSION {

			panic(fmt.Sprintf("Cannot open graph storage of version: %v - "+
				"max supported version: %v", version, VERSION))

		} else if v < VERSION {

			// Update the version if it is older

			mdb[MainDBVersion] = strconv.Itoa(VERSION)
			gs.FlushMain()
		}
	}

	gm := &Manager{gs, &graphRulesManager{nil, make(map[string]Rule),
		make(map[int]map[string]Rule)}, util.NewNamesManager(mdb),
		make(map[string]map[string]string), &sync.RWMutex{}, &sync.Mutex{}}

	gm.gr.gm = gm

	return gm
}

/*
Name returns the name of this graph manager.
*/
func (gm *Manager) Name() string {
	return fmt.Sprint("Graph ", gm.gs.Name())
}

/*
SetGraphRule sets a GraphRule.
*/
func (gm *Manager) SetGraphRule(rule Rule) {
	gm.gr.SetGraphRule(rule)
}

/*
GraphRules returns a list of all available graph rules.
*/
func (gm *Manager) GraphRules() []string {
	return gm.gr.GraphRules()
}

/*
NodeIndexQuery returns an object to query the full text search index for nodes.
*/
func (gm *Manager) NodeIndexQuery(part string, kind string) (IndexQuery, error) {
	iht, err := gm.getNodeIndexHTree(part, kind, false)
	if err != nil || iht == nil {
		return nil, err
	}

	return util.NewIndexManager(iht), nil
}

/*
EdgeIndexQuery returns an object to query the full text search index for edges.
*/
func (gm *Manager) EdgeIndexQuery(part string, kind string) (IndexQuery, error) {
	iht, err := gm.getEdgeIndexHTree(part, kind, false)
	if err != nil || iht == nil {
		return nil, err
	}

	return util.NewIndexManager(iht), nil
}

/*
Partitions returns all existing partitions.
*/
func (gm *Manager) Partitions() []string {
	return gm.mainStringList(MainDBParts)
}

/*
NodeKinds returns all possible node kinds.
*/
func (gm *Manager) NodeKinds() []string {
	return gm.mainStringList(MainDBNodeKinds)
}

/*
EdgeKinds returns all possible node kinds.
*/
func (gm *Manager) EdgeKinds() []string {
	return gm.mainStringList(MainDBEdgeKinds)
}

/*
NodeAttrs returns all possible node attributes for a given node kind.
*/
func (gm *Manager) NodeAttrs(kind string) []string {
	return gm.mainStringList(MainDBNodeAttrs + kind)
}

/*
NodeEdges returns all possible node edge specs for a given node kind.
*/
func (gm *Manager) NodeEdges(kind string) []string {
	return gm.mainStringList(MainDBNodeEdges + kind)
}

/*
EdgeAttrs returns all possible edge attributes for a given edge kind.
*/
func (gm *Manager) EdgeAttrs(kind string) []string {
	return gm.mainStringList(MainDBEdgeAttrs + kind)
}

/*
mainStringList return a list in the MainDB.
*/
func (gm *Manager) mainStringList(name string) []string {
	items := gm.getMainDBMap(name)

	var ret []string

	if items != nil {
		for item := range items {
			ret = append(ret, item)
		}
	}

	sort.StringSlice(ret).Sort()

	return ret
}

/*
IsValidAttr checks if a given string can be a valid node attribute.
*/
func (gm *Manager) IsValidAttr(attr string) bool {
	return gm.nm.Encode32(attr, false) != "" ||
		attr == data.NodeKey || attr == data.NodeKind ||
		attr == data.EdgeEnd1Key || attr == data.EdgeEnd1Kind ||
		attr == data.EdgeEnd1Role || attr == data.EdgeEnd1Cascading ||
		attr == data.EdgeEnd2Key || attr == data.EdgeEnd2Kind ||
		attr == data.EdgeEnd2Role || attr == data.EdgeEnd2Cascading
}
