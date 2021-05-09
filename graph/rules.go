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
	"sort"
	"strings"
	"sync"

	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/graph/util"
)

/*
GraphRulesManager data structure
*/
type graphRulesManager struct {
	gm       *Manager                // GraphManager which provides events
	rules    map[string]Rule         // Map of graph rules
	eventMap map[int]map[string]Rule // Map of events to graph rules
}

/*
Rule models a graph rule.
*/
type Rule interface {

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
	Handle(gm *Manager, trans Trans, event int, data ...interface{}) error
}

/*
graphEvent main event handler which receives all graph related events.
*/
func (gr *graphRulesManager) graphEvent(trans Trans, event int, data ...interface{}) error {
	var result error
	var errors []string

	rules, ok := gr.eventMap[event]

	handled := false // Flag to return a special handled error if no other error occured

	if ok {

		for _, rule := range rules {

			// Craete a GraphManager clone which can be used for queries only

			gmclone := gr.cloneGraphManager()
			gmclone.mutex.RLock()
			defer gmclone.mutex.RUnlock()

			// Handle the event

			err := rule.Handle(gmclone, trans, event, data...)

			if err != nil {
				if err == ErrEventHandled {
					handled = true
				} else {
					if errors == nil {
						errors = make([]string, 0)
					}
					errors = append(errors, err.Error())
				}
			}
		}
	}

	if errors != nil {
		return &util.GraphError{Type: util.ErrRule, Detail: strings.Join(errors, ";")}
	}

	if handled {
		result = ErrEventHandled
	}

	return result
}

/*
Clone a given graph manager and insert a new RWMutex.
*/
func (gr *graphRulesManager) cloneGraphManager() *Manager {
	return &Manager{gr.gm.gs, gr, gr.gm.nm, gr.gm.mapCache, &sync.RWMutex{}, &sync.Mutex{}}
}

/*
SetGraphRule sets a GraphRule.
*/
func (gr *graphRulesManager) SetGraphRule(rule Rule) {
	gr.rules[rule.Name()] = rule

	for _, handledEvent := range rule.Handles() {

		rules, ok := gr.eventMap[handledEvent]
		if !ok {
			rules = make(map[string]Rule)
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

	for rule := range gr.rules {
		ret = append(ret, rule)
	}

	sort.StringSlice(ret).Sort()

	return ret
}

// System rule SystemRuleDeleteNodeEdges
// =====================================

/*
SystemRuleDeleteNodeEdges is a system rule to delete all edges when a node is
deleted. Deletes also the other end if the cascading flag is set on the edge.
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
	return []int{EventNodeDeleted}
}

/*
Handle handles an event.
*/
func (r *SystemRuleDeleteNodeEdges) Handle(gm *Manager, trans Trans, event int, ed ...interface{}) error {
	part := ed[0].(string)
	node := ed[1].(data.Node)

	// Get all connected nodes and relationships

	nnodes, edges, err := gm.TraverseMulti(part, node.Key(), node.Kind(), ":::", false)
	if err != nil {
		return err
	}

	edgeRemovalCount := make(map[string]int) // Count of cascading last edges which are removed

	// Nodes which need to be checked if the last edge of a certain kind has been removed

	var nodeRemovalCheckNodes []data.Node
	var nodeRemovalCheckSpecs []string

	for i, edge := range edges {

		// Remove the edge in any case

		trans.RemoveEdge(part, edge.Key(), edge.Kind())

		// Remove the node on the other side if the edge is cascading on this end

		if edge.End1IsCascading() {

			if edge.End1IsCascadingLast() {

				// Only remove the node at the other end if all edges of this kind
				// have been removed from that node after this operation

				// Get edge spec from other side

				nodeOtherSide := nnodes[i]
				specOtherSide := edge.Spec(nodeOtherSide.Key())

				if c, ok := edgeRemovalCount[specOtherSide]; ok {
					edgeRemovalCount[specOtherSide] = c + 1
				} else {
					edgeRemovalCount[specOtherSide] = 1
				}

				nodeRemovalCheckSpecs = append(nodeRemovalCheckSpecs, specOtherSide)
				nodeRemovalCheckNodes = append(nodeRemovalCheckNodes, nodeOtherSide)

			} else {

				// No error handling at this point since only a wrong partition
				// name can cause an issue and this would have failed before

				trans.RemoveNode(part, nnodes[i].Key(), nnodes[i].Kind())
			}
		}
	}

	// Check cascading last edges

	for i, node := range nodeRemovalCheckNodes {
		specToCheck := nodeRemovalCheckSpecs[i]
		removalCount := edgeRemovalCount[specToCheck]

		if err == nil {

			_, edges, err = gm.TraverseMulti(part, node.Key(), node.Kind(), specToCheck, false)

			if len(edges)-removalCount == 0 {

				trans.RemoveNode(part, node.Key(), node.Kind())
			}
		}
	}

	return err
}

// System rule SystemRuleUpdateNodeStats
// =====================================

/*
SystemRuleUpdateNodeStats is a system rule to update info entries such as
known node or edge kinds in the MainDB.
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
	return []int{EventNodeCreated, EventNodeUpdated,
		EventEdgeCreated, EventEdgeUpdated}
}

/*
Handle handles an event.
*/
func (r *SystemRuleUpdateNodeStats) Handle(gm *Manager, trans Trans, event int, ed ...interface{}) error {
	attrMap := MainDBNodeAttrs

	if event == EventEdgeCreated {
		edge := ed[1].(data.Edge)

		updateNodeRels := func(key string, kind string) {
			spec := edge.Spec(key)
			specs := gm.getMainDBMap(MainDBNodeEdges + kind)

			if specs != nil {
				if _, ok := specs[spec]; !ok {
					specs[spec] = ""
					gm.storeMainDBMap(MainDBNodeEdges+kind, specs)
				}
			}
		}

		// Update stored relationships for both ends

		updateNodeRels(edge.End1Key(), edge.End1Kind())
		updateNodeRels(edge.End2Key(), edge.End2Kind())

		attrMap = MainDBEdgeAttrs
	}

	node := ed[1].(data.Node)
	kind := node.Kind()

	// Check if a new partition or kind was used

	if event == EventNodeCreated || event == EventEdgeCreated {
		part := ed[0].(string)

		updateMainDB := func(entry string, val string) {
			vals := gm.getMainDBMap(entry)
			if _, ok := vals[val]; !ok {
				vals[val] = ""
				gm.storeMainDBMap(entry, vals)
			}
		}

		updateMainDB(MainDBParts, part)

		if event == EventNodeCreated {
			updateMainDB(MainDBNodeKinds, kind)
		} else {
			updateMainDB(MainDBEdgeKinds, kind)
		}
	}

	storeAttrs := false

	attrs := gm.getMainDBMap(attrMap + kind)

	if attrs != nil {

		// Update stored node attributes

		for attr := range node.Data() {
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
