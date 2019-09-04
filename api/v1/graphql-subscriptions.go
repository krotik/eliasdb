/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package v1

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"devt.de/krotik/common/stringutil"
	"devt.de/krotik/eliasdb/api"
	"devt.de/krotik/eliasdb/graphql"
)

/*
EndpointGraphQLSubscriptions is the GraphQL endpoint URL for subscriptions (rooted). Handles websockets under graphql-subscriptions/
*/
const EndpointGraphQLSubscriptions = api.APIRoot + APIv1 + "/graphql-subscriptions/"

/*
upgrader can upgrade normal requests to websocket communications
*/
var upgrader = websocket.Upgrader{
	Subprotocols:    []string{"graphql-subscriptions"},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

var subscriptionCallbackError error

/*
GraphQLSubscriptionsEndpointInst creates a new endpoint handler.
*/
func GraphQLSubscriptionsEndpointInst() api.RestEndpointHandler {
	return &graphQLSubscriptionsEndpoint{}
}

/*
Handler object for GraphQL operations.
*/
type graphQLSubscriptionsEndpoint struct {
	*api.DefaultEndpointHandler
}

/*
HandleGET handles GraphQL subscription queries.
*/
func (e *graphQLSubscriptionsEndpoint) HandleGET(w http.ResponseWriter, r *http.Request, resources []string) {

	// Update the incomming connection to a websocket
	// If the upgrade fails then the client gets an HTTP error response.

	conn, err := upgrader.Upgrade(w, r, nil)

	// Websocket connections support one concurrent reader and one concurrent writer.
	// See: https://godoc.org/github.com/gorilla/websocket#hdr-Concurrency

	connRMutex := &sync.Mutex{}
	connWMutex := &sync.Mutex{}

	if err != nil {

		// We give details here on what went wrong

		w.Write([]byte(err.Error()))
		return
	}

	subID := ""

	// Ensure we have a partition to query

	partition := r.URL.Query().Get("partition")
	if partition == "" && len(resources) > 0 {
		partition = resources[0]
	}

	if partition == "" {
		connWMutex.Lock()
		e.WriteError(conn, subID, "Need a 'partition' in path or as url parameter", true)
		connWMutex.Unlock()
		return
	}

	connWMutex.Lock()
	conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"init_success","payload":{}}`))
	connWMutex.Unlock()

	// Create the callback handler for the subscription

	callbackHandler := &subscriptionCallbackHandler{
		finished: false,
		publish: func(data map[string]interface{}, err error) {
			var res []byte

			// Error for unit testing

			err = subscriptionCallbackError

			// This is called if data im the datastore changes

			if err == nil {
				res, err = json.Marshal(map[string]interface{}{
					"id":      subID,
					"type":    "subscription_data",
					"payload": data,
				})
			}

			if err != nil {
				connWMutex.Lock()
				e.WriteError(conn, subID, err.Error(), true)
				connWMutex.Unlock()
				return
			}

			connWMutex.Lock()
			conn.WriteMessage(websocket.TextMessage, res)
			connWMutex.Unlock()
		},
	}

	for {

		// Read websocket message

		connRMutex.Lock()
		_, msg, err := conn.ReadMessage()
		connRMutex.Unlock()

		if err != nil {

			// Unregister the callback handler

			callbackHandler.finished = true

			// If the client is still listening write the error message
			// This is a NOP if the client hang up

			connWMutex.Lock()
			e.WriteError(conn, subID, err.Error(), true)
			connWMutex.Unlock()

			return
		}

		data := make(map[string]interface{})

		if err := json.Unmarshal(msg, &data); err != nil {

			connWMutex.Lock()
			e.WriteError(conn, subID, err.Error(), false)
			connWMutex.Unlock()

			continue
		}

		// Check we got a message with a type

		if msgType, ok := data["type"]; ok {

			// Check if the user wants to start a new subscription

			if _, ok := data["query"]; msgType == "subscription_start" && ok {
				var res []byte

				subID = fmt.Sprint(data["id"])

				if _, ok := data["variables"]; !ok {
					data["variables"] = nil
				}

				if _, ok := data["operationName"]; !ok {
					data["operationName"] = nil
				}

				resData, err := graphql.RunQuery(stringutil.CreateDisplayString(partition)+" query",
					partition, data, api.GM, callbackHandler, false)

				if err == nil {
					res, err = json.Marshal(map[string]interface{}{
						"id":      subID,
						"type":    "subscription_data",
						"payload": resData,
					})
				}

				if err != nil {

					connWMutex.Lock()
					e.WriteError(conn, subID, err.Error(), false)
					connWMutex.Unlock()

					continue
				}

				connWMutex.Lock()

				conn.WriteMessage(websocket.TextMessage, []byte(
					fmt.Sprintf(`{"id":"%s","type":"subscription_success","payload":{}}`, subID)))

				conn.WriteMessage(websocket.TextMessage, res)

				connWMutex.Unlock()
			}
		}
	}
}

/*
WriteError writes an error message to the websocket.
*/
func (e *graphQLSubscriptionsEndpoint) WriteError(conn *websocket.Conn,
	subID string, msg string, close bool) {

	// Write the error as cleartext message

	data, _ := json.Marshal(map[string]interface{}{
		"id":   subID,
		"type": "subscription_fail",
		"payload": map[string]interface{}{
			"errors": []string{msg},
		},
	})

	conn.WriteMessage(websocket.TextMessage, data)

	if close {
		// Write error as closing control message

		conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(
				websocket.CloseUnsupportedData, msg), time.Now().Add(10*time.Second))

		conn.Close()
	}
}

/*
SwaggerDefs is used to describe the endpoint in swagger.
*/
func (e *graphQLSubscriptionsEndpoint) SwaggerDefs(s map[string]interface{}) {
	// No swagger definitions for this endpoint as it only handles websocket requests
}

// Callback Handler
// ================

/*
subscriptionCallbackHandler pushes new events to a subscription client via a websocket.
*/
type subscriptionCallbackHandler struct {
	finished bool
	publish  func(data map[string]interface{}, err error)
}

func (ch *subscriptionCallbackHandler) Publish(data map[string]interface{}, err error) {
	ch.publish(data, err)
}

func (ch *subscriptionCallbackHandler) IsFinished() bool {
	return ch.finished
}
