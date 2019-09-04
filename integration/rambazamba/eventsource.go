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
Package rambazamba contains an eventsource for Rambazamba which forwards
internal EliasDB events to Rambazamba engines.
*/
package rambazamba

import (
	"fmt"
	"io"

	"devt.de/krotik/common/defs/rambazamba"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/data"
)

/*
AddEventPublisher adds an EventPublisher to a given Manager using an EventBridge.
*/
func AddEventPublisher(gm *graph.Manager, publisher rambazamba.EventPublisher, errOut io.Writer) {
	gm.SetGraphRule(&EventBridge{publisher, errOut})
}

/*
EventMapping is a mapping between EliasDB event types to Rambazamba event kinds.
*/
var EventMapping = map[int]string{

	/*
	   EventNodeCreated is thrown when a node gets created.

	   Parameters: partition of created node, created node
	*/
	graph.EventNodeCreated: "db.node.created",

	/*
	   EventNodeUpdated is thrown when a node gets updated.

	   Parameters: partition of updated node, updated node, old node
	*/
	graph.EventNodeUpdated: "db.node.updated",

	/*
	   EventNodeDeleted is thrown when a node gets deleted.

	   Parameters: partition of deleted node, deleted node
	*/
	graph.EventNodeDeleted: "db.node.deleted",

	/*
	   EventEdgeCreated is thrown when an edge gets created.

	   Parameters: partition of created edge, created edge
	*/
	graph.EventEdgeCreated: "db.edge.created",

	/*
	   EventEdgeUpdated is thrown when an edge gets updated.

	   Parameters: partition of updated edge, updated edge, old edge
	*/
	graph.EventEdgeUpdated: "db.edge.updated",

	/*
	   EventEdgeDeleted is thrown when an edge gets deleted.

	   Parameters: partition of deleted edge, deleted edge
	*/
	graph.EventEdgeDeleted: "db.edge.deleted",
}

// Event bridge between EliasDB and Rambazamba
// ===========================================

/*
EventBridge is a rule for a graph manager to forward all graph events to
Rambazamba.
*/
type EventBridge struct {
	publisher rambazamba.EventPublisher
	errOut    io.Writer
}

/*
Name returns the name of the rule.
*/
func (r *EventBridge) Name() string {
	return "rambazamba.eventbridge"
}

/*
Handles returns a list of events which are handled by this rule.
*/
func (r *EventBridge) Handles() []int {
	return []int{graph.EventNodeCreated, graph.EventNodeUpdated, graph.EventNodeDeleted,
		graph.EventEdgeCreated, graph.EventEdgeUpdated, graph.EventEdgeDeleted}
}

/*
Handle handles an event.
*/
func (r *EventBridge) Handle(gm *graph.Manager, trans graph.Trans, event int, ed ...interface{}) error {

	if name, ok := EventMapping[event]; ok {

		// Build up state

		state := map[interface{}]interface{}{
			"part": fmt.Sprint(ed[0]),
		}

		switch event {
		case graph.EventNodeCreated:
			state["node"] = ed[1].(data.Node)

		case graph.EventNodeUpdated:
			state["node"] = ed[1].(data.Node)
			state["old_node"] = ed[2].(data.Node)

		case graph.EventNodeDeleted:
			state["node"] = ed[1].(data.Node)

		case graph.EventEdgeCreated:
			state["edge"] = ed[1].(data.Edge)

		case graph.EventEdgeUpdated:
			state["edge"] = ed[1].(data.Edge)
			state["old_edge"] = ed[2].(data.Edge)

		case graph.EventEdgeDeleted:
			state["edge"] = ed[1].(data.Edge)
		}

		// Try to inject the event

		err := r.publisher.AddEvent(name, []string{name}, state)
		if err != nil && r.errOut != nil {
			r.errOut.Write([]byte(err.Error()))
		}
	}

	return nil
}
