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
Graph rules provide automatic operations which help to keep the graph consistent.
*/
package graph

import (
	"sort"
	"strings"
	"sync"

	"devt.de/eliasdb/graph/data"
	"devt.de/eliasdb/graph/util"
)

/*
GraphRulesManager data structure
*/
type graphRulesManager struct {
	gm       *GraphManager                // GraphManager which provides events
	rules    map[string]GraphRule         // Map of graph rules
	eventMap map[int]map[string]GraphRule // Map of events to graph rules
}

type GraphRule interface {

	/*
	   Name returns the name of the rule.
	*/
	Name() string

	/*
		Handles returns a list of events which are handled by this rule.
	*/
	Handles() []int

	/*
		Handle handles an event. The function should write all changes to the
		given transaction.
	*/
	Handle(gm *GraphManager, trans *GraphTrans, event int, data ...interface{}) error
}

/*
graphEvent main event handler which receives all graph related events.
*/
func (gr *graphRulesManager) graphEvent(trans *GraphTrans, event int, data ...interface{}) error {
	var errors []string

	rules, ok := gr.eventMap[event]

	if ok {

		for _, rule := range rules {

			// Craete a GraphManager clone which can be used for queries only

			gmclone := gr.cloneGraphManager()
			gmclone.mutex.RLock()
			defer gmclone.mutex.RUnlock()

			// Handle the event

			err := rule.Handle(gmclone, trans, event, data...)

			if err != nil {
				if errors == nil {
					errors = make([]string, 0)
				}
				errors = append(errors, err.Error())
			}
		}
	}

	if errors != nil {
		return &util.GraphError{util.ErrRule, strings.Join(errors, ";")}
	}

	return nil
}

/*
Clone a given graph manager and insert a new RWMutex.
*/
func (gr *graphRulesManager) cloneGraphManager() *GraphManager {
	return &GraphManager{gr.gm.gs, gr, gr.gm.nm, gr.gm.mapCache, &sync.RWMutex{}}
}

/*
SetGraphRule sets a GraphRule.
*/
func (gr *graphRulesManager) SetGraphRule(rule GraphRule) {
	gr.rules[rule.Name()] = rule

	for _, handledEvent := range rule.Handles() {

		rules, ok := gr.eventMap[handledEvent]
		if !ok {
			rules = make(map[string]GraphRule)
			gr.eventMap[handledEvent] = rules
		}

		rules[rule.Name()] = rule
	}
}

/*
GraphRules returns a list of all available graph rules.
*/
func (gr *graphRulesManager) GraphRules() []string {
	ret := make([]string, 0, len(gr.rules))

	for rule, _ := range gr.rules {
		ret = append(ret, rule)
	}

	sort.StringSlice(ret).Sort()

	return ret
}

// System rule SystemRuleDeleteNodeEdges
// =====================================

/*
System rule to delete all edges when a node is deleted. Deletes also the other
end if the cascading flag is set on the edge.
*/
type SystemRuleDeleteNodeEdges struct {
}

/*
Name returns the name of the rule.
*/
func (r *SystemRuleDeleteNodeEdges) Name() string {
	return "system.deletenodeedges"
}

/*
Handles returns a list of events which are handled by this rule.
*/
func (r *SystemRuleDeleteNodeEdges) Handles() []int {
	return []int{EVENT_NODE_DELETED}
}

/*
Handle handles an event.
*/
func (r *SystemRuleDeleteNodeEdges) Handle(gm *GraphManager, trans *GraphTrans, event int, ed ...interface{}) error {
	part := ed[0].(string)
	node := ed[1].(data.Node)

	// Get all connected nodes and relationships

	nnodes, edges, err := gm.TraverseMulti(part, node.Key(), node.Kind(), ":::", false)
	if err != nil {
		return err
	}

	for i, edge := range edges {

		// Remove the edge in any case

		trans.RemoveEdge(part, edge.Key(), edge.Kind())

		// Remove the node on the other side if the edge is cascading on this end

		if edge.End1IsCascading() {

			// No error handling at this point since only a wrong partition
			// name can cause an issue and this would have failed before

			trans.RemoveNode(part, nnodes[i].Key(), nnodes[i].Kind())
		}
	}

	return nil
}

// System rule SystemRuleUpdateNodeStats
// =====================================

/*
System rule to update node stat entries in the MainDB.
*/
type SystemRuleUpdateNodeStats struct {
}

/*
Name returns the name of the rule.
*/
func (r *SystemRuleUpdateNodeStats) Name() string {
	return "system.updatenodestats"
}

/*
Handles returns a list of events which are handled by this rule.
*/
func (r *SystemRuleUpdateNodeStats) Handles() []int {
	return []int{EVENT_NODE_CREATED, EVENT_NODE_UPDATED,
		EVENT_EDGE_CREATED, EVENT_EDGE_UPDATED}
}

/*
Handle handles an event.
*/
func (r *SystemRuleUpdateNodeStats) Handle(gm *GraphManager, trans *GraphTrans, event int, ed ...interface{}) error {
	attrMap := MAINDB_NODE_ATTRS

	if event == EVENT_EDGE_CREATED {
		edge := ed[1].(data.Edge)

		updateNodeRels := func(key string, kind string) {
			spec := edge.Spec(key)
			specs := gm.getMainDBMap(MAINDB_NODE_EDGES + kind)

			if specs != nil {
				if _, ok := specs[spec]; !ok {
					specs[spec] = ""
					gm.storeMainDBMap(MAINDB_NODE_EDGES+kind, specs)
				}
			}
		}

		// Update stored relationships for both ends

		updateNodeRels(edge.End1Key(), edge.End1Kind())
		updateNodeRels(edge.End2Key(), edge.End2Kind())

		attrMap = MAINDB_EDGE_ATTRS
	}

	node := ed[1].(data.Node)
	kind := node.Kind()

	// Check if a new partition or kind was used

	if event == EVENT_NODE_CREATED || event == EVENT_EDGE_CREATED {
		part := ed[0].(string)

		updateMainDB := func(entry string, val string) {
			vals := gm.getMainDBMap(entry)
			if _, ok := vals[val]; !ok {
				vals[val] = ""
				gm.storeMainDBMap(entry, vals)
			}
		}

		updateMainDB(MAINDB_PARTS, part)

		if event == EVENT_NODE_CREATED {
			updateMainDB(MAINDB_NODE_KINDS, kind)
		} else {
			updateMainDB(MAINDB_EDGE_KINDS, kind)
		}
	}

	storeAttrs := false

	attrs := gm.getMainDBMap(attrMap + kind)

	if attrs != nil {

		// Update stored node attributes

		for attr, _ := range node.Data() {
			if _, ok := attrs[attr]; !ok {
				attrs[attr] = ""
				storeAttrs = true
			}
		}

		// Store attribute map if something was changed

		if storeAttrs {
			gm.storeMainDBMap(attrMap+kind, attrs)
		}
	}

	return nil
}
