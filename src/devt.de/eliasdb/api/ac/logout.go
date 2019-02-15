/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package ac

import (
	"net/http"

	"devt.de/common/httputil/user"
	"devt.de/eliasdb/api"
)

/*
EndpointLogout is the logout endpoint URL (rooted). Handles logout/
*/
const EndpointLogout = api.APIRoot + "/logout/"

/*
LogoutEndpointInst creates a new endpoint handler.
*/
func LogoutEndpointInst() api.RestEndpointHandler {
	return &logoutEndpoint{}
}

/*
Handler object for logout operations.
*/
type logoutEndpoint struct {
	*api.DefaultEndpointHandler
}

/*
HandlePOST terminates the current user session.
*/
func (lo *logoutEndpoint) HandlePOST(w http.ResponseWriter, r *http.Request, resources []string) {

	// Remove all cookies - we don't check for a valid authentication so also
	// old (invalid) cookies are removed

	AuthHandler.InvalidateAuthCookie(r)
	AuthHandler.RemoveAuthCookie(w)
	user.UserSessionManager.RemoveSessionCookie(w)

	ct := r.Header.Get("Content-Type")

	if ct != "application/json" {

		// Do a redirect for non-REST clients

		http.Redirect(w, r, "/", http.StatusFound)
	}
}

/*
SwaggerDefs is used to describe the endpoint in swagger.
*/
func (lo *logoutEndpoint) SwaggerDefs(s map[string]interface{}) {

	s["paths"].(map[string]interface{})["/logout"] = map[string]interface{}{
		"post": map[string]interface{}{
			"summary":     "Logout the current user.",
			"description": "The logout endpoint terminates the current user session.",
			"consumes": []string{
				"application/json",
			},
			"produces": []string{
				"application/json",
			},
			"responses": map[string]interface{}{
				"302": map[string]interface{}{
					"description": "Redirect to /.",
				},
				"default": map[string]interface{}{
					"description": "Error response",
					"schema": map[string]interface{}{
						"$ref": "#/definitions/Error",
					},
				},
			},
		},
	}

	// Add generic error object to definition

	s["definitions"].(map[string]interface{})["Error"] = map[string]interface{}{
		"description": "A human readable error mesage.",
		"type":        "string",
	}
}
