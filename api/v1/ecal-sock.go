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
	"io/ioutil"
	"net/http"
	"strings"

	"devt.de/krotik/common/cryptutil"
	"devt.de/krotik/common/errorutil"
	"devt.de/krotik/common/stringutil"
	"devt.de/krotik/ecal/engine"
	"devt.de/krotik/ecal/scope"
	"devt.de/krotik/eliasdb/api"
	"devt.de/krotik/eliasdb/ecal"
	"github.com/gorilla/websocket"
)

/*
EndpointECALSock is the ECAL endpoint URL (rooted) for websocket operations. Handles everything under sock/...
*/
const EndpointECALSock = api.APIRoot + "/sock/"

/*
upgrader can upgrade normal requests to websocket communications
*/
var sockUpgrader = websocket.Upgrader{
	Subprotocols:    []string{"ecal-sock"},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

var sockCallbackError error

/*
ECALSockEndpointInst creates a new endpoint handler.
*/
func ECALSockEndpointInst() api.RestEndpointHandler {
	return &ecalSockEndpoint{}
}

/*
Handler object for ECAL websocket operations.
*/
type ecalSockEndpoint struct {
	*api.DefaultEndpointHandler
}

/*
HandleGET handles ECAL websocket operations.
*/
func (e *ecalSockEndpoint) HandleGET(w http.ResponseWriter, r *http.Request, resources []string) {

	if api.SI != nil {
		var body []byte

		// Update the incomming connection to a websocket
		// If the upgrade fails then the client gets an HTTP error response.

		conn, err := sockUpgrader.Upgrade(w, r, nil)

		if err != nil {

			// We give details here on what went wrong

			w.Write([]byte(err.Error()))
			return
		}

		commID := fmt.Sprintf("%x", cryptutil.GenerateUUID())

		wc := ecal.NewWebsocketConnection(commID, conn)

		wc.Init()

		if body, err = ioutil.ReadAll(r.Body); err == nil {

			var data interface{}
			json.Unmarshal(body, &data)

			query := map[interface{}]interface{}{}
			for k, v := range r.URL.Query() {
				values := make([]interface{}, 0)
				for _, val := range v {
					values = append(values, val)
				}
				query[k] = values
			}

			header := map[interface{}]interface{}{}
			for k, v := range r.Header {
				header[k] = scope.ConvertJSONToECALObject(v)
			}

			proc := api.SI.Interpreter.RuntimeProvider.Processor
			event := engine.NewEvent(fmt.Sprintf("WebSocketRequest"), []string{"db", "web", "sock"},
				map[interface{}]interface{}{
					"commID":     commID,
					"path":       strings.Join(resources, "/"),
					"pathList":   resources,
					"bodyString": string(body),
					"bodyJSON":   scope.ConvertJSONToECALObject(data),
					"query":      query,
					"method":     r.Method,
					"header":     header,
				})

			// Add event that the websocket has been registered

			if _, err = proc.AddEventAndWait(event, nil); err == nil {
				api.SI.RegisterECALSock(wc)
				defer func() {
					api.SI.DeregisterECALSock(wc)
				}()

				for {
					var fatal bool
					var data map[string]interface{}

					// Read websocket message

					if data, fatal, err = wc.ReadData(); err != nil {

						wc.WriteData(map[string]interface{}{
							"error": err.Error(),
						})

						if fatal {
							break
						}

						continue
					}

					if val, ok := data["close"]; ok && stringutil.IsTrueValue(fmt.Sprint(val)) {
						wc.Close("")
						break
					}

					event = engine.NewEvent(fmt.Sprintf("WebSocketRequest"), []string{"db", "web", "sock", "data"},
						map[interface{}]interface{}{
							"commID":   commID,
							"path":     strings.Join(resources, "/"),
							"pathList": resources,
							"query":    query,
							"method":   r.Method,
							"header":   header,
							"data":     scope.ConvertJSONToECALObject(data),
						})

					_, err = proc.AddEvent(event, nil)
					errorutil.AssertOk(err)
				}
			}

		}

		if err != nil {
			wc.Close(err.Error())
			api.SI.Interpreter.RuntimeProvider.Logger.LogDebug(err)
		}

		return
	}

	http.Error(w, "Resource was not found", http.StatusNotFound)
}

/*
SwaggerDefs is used to describe the endpoint in swagger.
*/
func (e *ecalSockEndpoint) SwaggerDefs(s map[string]interface{}) {
	// No swagger definitions for this endpoint as it only handles websocket requests
}
