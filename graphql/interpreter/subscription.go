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
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"devt.de/krotik/common/cryptutil"
	"devt.de/krotik/common/errorutil"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/data"
)

/*
SystemRuleGraphQLSubscriptionsName is the name of the graph manager rule which
deals with subscriptions.
*/
const SystemRuleGraphQLSubscriptionsName = "system.graphqlsubscriptions"

/*
SubscriptionCallbackHandler receives source stream events for a subscription.
*/
type SubscriptionCallbackHandler interface {

	/*
		Publish is called for every event in the source stream of a subscription.
		This function should map the source stream event to a response stream event.
	*/
	Publish(map[string]interface{}, error)

	/*
	   IsFinished should return true if this handler should no longer
	   receive events.
	*/
	IsFinished() bool
}

var ruleMap = make(map[string]*SystemRuleGraphQLSubscriptions)

/*
InitSubscription ensures that the current graph manager has a rule for
subscriptions to monitor data changes and forwards events to the subscription
callback handler.
*/
func (rtp *GraphQLRuntimeProvider) InitSubscription(rt *documentRuntime) {
	var rule *SystemRuleGraphQLSubscriptions

	if rt.rtp.subscriptionHandler != nil {

		// We already got a handler no need to create another

		return
	}

	// Lookup or create rule

	for _, r := range rtp.gm.GraphRules() {
		if strings.HasPrefix(r, SystemRuleGraphQLSubscriptionsName) {
			id := strings.Split(r, "-")[1]
			rule = ruleMap[id]
			errorutil.AssertTrue(rule != nil, "Previously created rule not found")
		}
	}

	if rule == nil {
		rule = &SystemRuleGraphQLSubscriptions{
			fmt.Sprintf("%x", cryptutil.GenerateUUID()),
			make(map[string]*subscriptionHandler),
			&sync.RWMutex{},
		}
		rtp.gm.SetGraphRule(rule)
		ruleMap[rule.ID] = rule
	}

	rtp.subscriptionHandler = &subscriptionHandler{
		fmt.Sprintf("%x", cryptutil.GenerateUUID()),
		rtp.part,
		make(map[string]string),
		&sync.RWMutex{},
		rt,
		"",
		rtp.callbackHandler,
		rule,
	}

	rule.AddHandler(rtp.subscriptionHandler)
}

/*
subscriptionHandler coordinates a subscription.
*/
type subscriptionHandler struct {
	id                 string                          // Unique ID which identifies the handler
	part               string                          // Partition this handler is monitoring
	monitoredKinds     map[string]string               // All kinds which are monitored (for updates)
	monitoredKindsLock *sync.RWMutex                   // Lock for monitored kinds
	rt                 *documentRuntime                // GraphQL document which can be executed
	lastResponse       string                          // Last response which was given to the callback handler
	callbackHandler    SubscriptionCallbackHandler     // Handler which consumes updates
	rule               *SystemRuleGraphQLSubscriptions // Rule which is providing events
}

/*
HandleEvent handles an event from a rule and forwards it to the callbackHandler
if appropriate.
*/
func (h *subscriptionHandler) HandleEvent(event int, part string, node data.Node) {

	defer func() {

		// Check if the subscription is still needed - this call can be used
		// for done() call on a WaitGroup.

		if h.callbackHandler.IsFinished() {

			// Unsubscribe this handler - we are done

			h.rule.RemoveHandler(h)
		}
	}()

	// Only care if we are in the right partition

	if part == h.part {

		if event == graph.EventNodeUpdated {

			// If a node is updated only proceed if its kind is monitored

			if _, ok := h.monitoredKinds[node.Kind()]; !ok {
				return
			}
		}

		// Rerun the query

		resData, err := h.rt.Eval()

		// Stringify the result and see if it is different from the last response

		resBytes, _ := json.MarshalIndent(resData, "", "  ")
		resString := string(resBytes)

		if h.lastResponse != resString || err != nil {

			// Finally send the new result

			h.callbackHandler.Publish(resData, err)
			h.lastResponse = resString
		}
	}
}

/*
EnsureMonitoredKind ensure that the given kind is monitored for updates.
*/
func (h *subscriptionHandler) EnsureMonitoredKind(kind string) {
	h.monitoredKindsLock.RLock()
	if _, ok := h.monitoredKinds[kind]; !ok {
		h.monitoredKindsLock.RUnlock()

		h.monitoredKindsLock.Lock()
		defer h.monitoredKindsLock.Unlock()
		h.monitoredKinds[kind] = ""
	} else {
		h.monitoredKindsLock.RUnlock()
	}
}

/*
FetchNode intercepts a FetchNode call to the graph.Manager in order to subscribe
to node updates if necessary.
*/
func (rtp *GraphQLRuntimeProvider) FetchNode(part string, key string, kind string) (data.Node, error) {
	return rtp.FetchNodePart(part, key, kind, nil)
}

/*
FetchNodePart intercepts a FetchNodePart call to the graph.Manager in order to subscribe
to node updates if necessary.
*/
func (rtp *GraphQLRuntimeProvider) FetchNodePart(part string, key string, kind string, attrs []string) (data.Node, error) {
	if rtp.subscriptionHandler != nil {
		go rtp.subscriptionHandler.EnsureMonitoredKind(kind)
	}
	return rtp.gm.FetchNodePart(part, key, kind, attrs)
}

/*
SystemRuleGraphQLSubscriptions is a system rule to propagate state changes in the
datastore to all relevant GraphQL subscriptions.
*/
type SystemRuleGraphQLSubscriptions struct {
	ID           string // Unique ID which identifies the rule
	handlers     map[string]*subscriptionHandler
	handlersLock *sync.RWMutex
}

/*
Name returns the name of the rule.
*/
func (r *SystemRuleGraphQLSubscriptions) Name() string {
	return fmt.Sprintf("%s-%s", SystemRuleGraphQLSubscriptionsName, r.ID)
}

/*
Handles returns a list of events which are handled by this rule.
*/
func (r *SystemRuleGraphQLSubscriptions) Handles() []int {
	return []int{
		graph.EventNodeCreated,
		graph.EventNodeUpdated,
		graph.EventNodeDeleted,
	}
}

/*
Handle handles an event.
*/
func (r *SystemRuleGraphQLSubscriptions) Handle(gm *graph.Manager, trans graph.Trans, event int, ed ...interface{}) error {
	part := ed[0].(string)
	node := ed[1].(data.Node)

	r.handlersLock.RLock()
	defer r.handlersLock.RUnlock()

	for _, handler := range r.handlers {

		// Event is handled in a separate go routine

		go handler.HandleEvent(event, part, node)
	}

	return nil
}

/*
AddHandler adds a new handler for rule events.
*/
func (r *SystemRuleGraphQLSubscriptions) AddHandler(handler *subscriptionHandler) {
	r.handlersLock.Lock()
	defer r.handlersLock.Unlock()
	r.handlers[handler.id] = handler
}

/*
RemoveHandler removes a handler from receiving further rule events.
*/
func (r *SystemRuleGraphQLSubscriptions) RemoveHandler(handler *subscriptionHandler) {
	r.handlersLock.Lock()
	defer r.handlersLock.Unlock()
	delete(r.handlers, handler.id)
}
