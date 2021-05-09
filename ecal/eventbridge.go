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
Package ecal contains the main API for the event condition action language (ECAL).
*/
package ecal

import (
	"fmt"
	"strings"

	"devt.de/krotik/common/errorutil"
	"devt.de/krotik/ecal/engine"
	"devt.de/krotik/ecal/scope"
	"devt.de/krotik/ecal/util"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/data"
)

/*
EventMapping is a mapping between EliasDB event types to EliasDB specific event kinds in ECAL.
*/
var EventMapping = map[int]string{

	/*
	   EventNodeCreated is thrown when a node was created.

	   Parameters: partition of created node, created node
	*/
	graph.EventNodeCreated: "db.node.created",

	/*
	   EventNodeUpdated is thrown when a node was updated.

	   Parameters: partition of updated node, updated node, old node
	*/
	graph.EventNodeUpdated: "db.node.updated",

	/*
	   EventNodeDeleted is thrown when a node was deleted.

	   Parameters: partition of deleted node, deleted node
	*/
	graph.EventNodeDeleted: "db.node.deleted",

	/*
	   EventEdgeCreated is thrown when an edge was created.

	   Parameters: partition of created edge, created edge
	*/
	graph.EventEdgeCreated: "db.edge.created",

	/*
	   EventEdgeUpdated is thrown when an edge was updated.

	   Parameters: partition of updated edge, updated edge, old edge
	*/
	graph.EventEdgeUpdated: "db.edge.updated",

	/*
	   EventEdgeDeleted is thrown when an edge was deleted.

	   Parameters: partition of deleted edge, deleted edge
	*/
	graph.EventEdgeDeleted: "db.edge.deleted",

	/*
	   EventNodeStore is thrown before a node is stored (always overwriting existing values).

	   Parameters: partition of node to store, node to store
	*/
	graph.EventNodeStore: "db.node.store",

	/*
	   EventNodeUpdate is thrown before a node is updated.

	   Parameters: partition of node to update, node to update
	*/
	graph.EventNodeUpdate: "db.node.update",

	/*
	   EventNodeDelete is thrown before a node is deleted.

	   Parameters: partition of node to delete, key of node to delete, kind of node to delete
	*/
	graph.EventNodeDelete: "db.node.delete",

	/*
	   EventEdgeStore is thrown before an edge is stored (always overwriting existing values).

	   Parameters: partition of stored edge, stored edge
	*/
	graph.EventEdgeStore: "db.edge.store",

	/*
	   EventEdgeDelete is thrown before an edge is deleted.

	   Parameters: partition of deleted edge, deleted edge
	*/
	graph.EventEdgeDelete: "db.edge.delete",
}

/*
EventBridge is a rule for a graph manager to forward all graph events to ECAL.
*/
type EventBridge struct {
	Processor engine.Processor
	Logger    util.Logger
}

/*
Name returns the name of the rule.
*/
func (eb *EventBridge) Name() string {
	return "ecal.eventbridge"
}

/*
Handles returns a list of events which are handled by this rule.
*/
func (eb *EventBridge) Handles() []int {
	return []int{
		graph.EventNodeCreated,
		graph.EventNodeUpdated,
		graph.EventNodeDeleted,
		graph.EventEdgeCreated,
		graph.EventEdgeUpdated,
		graph.EventEdgeDeleted,
		graph.EventNodeStore,
		graph.EventNodeUpdate,
		graph.EventNodeDelete,
		graph.EventEdgeStore,
		graph.EventEdgeDelete,
	}
}

/*
Handle handles an event.
*/
func (eb *EventBridge) Handle(gm *graph.Manager, trans graph.Trans, event int, ed ...interface{}) error {
	var err error

	if name, ok := EventMapping[event]; ok {
		eventName := fmt.Sprintf("EliasDB: %v", name)
		eventKind := strings.Split(name, ".")

		// Construct an event which can be used to check if any rule will trigger.
		// This is to avoid the relative costly state construction below for events
		// which would not trigger any rules.

		triggerCheckEvent := engine.NewEvent(eventName, eventKind, nil)

		if !eb.Processor.IsTriggering(triggerCheckEvent) {
			return nil
		}

		// Build up state

		state := map[interface{}]interface{}{
			"part":  fmt.Sprint(ed[0]),
			"trans": trans,
		}

		// Include the right arguments into the state

		switch event {
		case graph.EventNodeCreated, graph.EventNodeUpdate, graph.EventNodeDeleted, graph.EventNodeStore:
			state["node"] = scope.ConvertJSONToECALObject(ed[1].(data.Node).Data())

		case graph.EventNodeUpdated:
			state["node"] = scope.ConvertJSONToECALObject(ed[1].(data.Node).Data())
			state["old_node"] = scope.ConvertJSONToECALObject(ed[2].(data.Node).Data())

		case graph.EventEdgeCreated, graph.EventEdgeDeleted, graph.EventEdgeStore:
			state["edge"] = scope.ConvertJSONToECALObject(ed[1].(data.Edge).Data())

		case graph.EventEdgeUpdated:
			state["edge"] = scope.ConvertJSONToECALObject(ed[1].(data.Edge).Data())
			state["old_edge"] = scope.ConvertJSONToECALObject(ed[2].(data.Edge).Data())

		case graph.EventNodeDelete, graph.EventEdgeDelete:
			state["key"] = fmt.Sprint(ed[1])
			state["kind"] = fmt.Sprint(ed[2])
		}

		// Try to inject the event

		event := engine.NewEvent(fmt.Sprintf("EliasDB: %v", name), strings.Split(name, "."), state)

		var m engine.Monitor
		m, err = eb.Processor.AddEventAndWait(event, nil)

		if err == nil {

			// If there was no direct error adding the event then check if an error was
			// raised in a sink

			if errs := m.(*engine.RootMonitor).AllErrors(); len(errs) > 0 {
				var errList []error

				for _, e := range errs {

					addError := true

					for _, se := range e.ErrorMap {

						// Check if the sink returned a special graph.ErrEventHandled error

						if re, ok := se.(*util.RuntimeErrorWithDetail); ok && re.Detail == graph.ErrEventHandled.Error() {
							addError = false
						}
					}

					if addError {
						errList = append(errList, e)
					}
				}

				if len(errList) > 0 {
					err = &errorutil.CompositeError{Errors: errList}
				} else {
					err = graph.ErrEventHandled
				}
			}
		}

		if err != nil {
			eb.Logger.LogDebug(fmt.Sprintf("EliasDB event %v was handled by ECAL and returned: %v", name, err))
		}
	}

	return err
}
