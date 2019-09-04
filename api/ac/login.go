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
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"devt.de/krotik/common/datautil"
	"devt.de/krotik/common/errorutil"
	"devt.de/krotik/common/httputil"
	"devt.de/krotik/common/httputil/auth"
	"devt.de/krotik/eliasdb/api"
)

/*
EndpointLogin is the login endpoint definition (rooted). Handles login/
*/
const EndpointLogin = api.APIRoot + "/login/"

/*
DebounceTime default debounce time for each failed logins
*/
var DebounceTime = 5 * time.Second

/*
LoginEndpointInst creates a new endpoint handler. Requires a CookieAuthHandleFuncWrapper
object to verify login requests.
*/
func LoginEndpointInst() api.RestEndpointHandler {

	errorutil.AssertTrue(AuthHandler != nil, "AuthHandler not initialized")

	return &loginEndpoint{
		&api.DefaultEndpointHandler{},
		AuthHandler,
		3,
		20,
		datautil.NewMapCache(0, int64(20)),
		datautil.NewMapCache(0, int64(20)),
	}
}

/*
Handler object for cookie based login operations.
*/
type loginEndpoint struct {
	*api.DefaultEndpointHandler
	authHandler        *auth.CookieAuthHandleFuncWrapper // AuthHandler object to verify login requests
	allowedRetries     int                               // Number of retries a user has to enter the correct password
	bruteForceDebounce int                               // Time in seconds a user has to wait after too many failed attempts
	failedLogins       *datautil.MapCache                // Map of failed login attempts per user
	debounceUsers      *datautil.MapCache                // Map of users which have to wait after too many failed attempts
}

/*
HandlePOST tries to log a user in.
*/
func (le *loginEndpoint) HandlePOST(w http.ResponseWriter, r *http.Request, resources []string) {
	restClient := false

	data := make(map[string]interface{})

	ct := r.Header.Get("Content-Type")

	// Decode body either as application/json or application/x-www-form-urlencoded
	// This endpoint can be used by REST clients as well as pages using form submissions

	if ct == "application/json" {

		// The client is a REST client

		restClient = true

		dec := json.NewDecoder(r.Body)
		if err := dec.Decode(&data); err != nil {
			http.Error(w, fmt.Sprintf("Could not decode request body: %v",
				err.Error()), http.StatusBadRequest)
			return
		}

	} else if err := r.ParseForm(); err == nil {

		// Json decoding did not work out try normal form data decoding

		data["user"] = r.FormValue("user")
		data["pass"] = r.FormValue("pass")
		data["redirect_ok"] = r.FormValue("redirect_ok")
		data["redirect_notok"] = r.FormValue("redirect_notok")
	}

	// Handle query and ast requests

	user, ok1 := data["user"]
	pass, ok2 := data["pass"]

	redirectOk, ok3 := data["redirect_ok"]
	if !ok3 || redirectOk == "" {
		redirectOk = "/"
	}
	redirectNotOk, ok4 := data["redirect_notok"]
	if !ok4 || redirectNotOk == "" {
		redirectNotOk = "/"
		if u, err := url.Parse(r.Referer()); err == nil {
			redirectNotOk = u.Path
		}
	}

	if ok1 && ok2 && user != "" {

		redirect := redirectNotOk

		if aid := le.authHandler.AuthUser(fmt.Sprint(user), fmt.Sprint(pass), false); aid != "" {

			redirect = redirectOk
			le.authHandler.SetAuthCookie(aid, w)

		} else {

			LogAccess("Authentication for user ", user, " failed")

			// Add a time delay for negative answers to make dictionary attacks
			// more tedious

			time.Sleep(DebounceTime)
		}

		if !restClient {

			// Redirect if the other end is not a REST client

			redirectString := fmt.Sprint(redirect)

			// Make sure ok/notok redirect are local!

			if err := httputil.CheckLocalRedirect(redirectString); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			http.Redirect(w, r, redirectString, http.StatusFound)

		} else if redirect == redirectNotOk {

			// The other end is a REST client and failed the authentication

			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		}

		// REST clients will just get a 200 with the cookie

		return
	}

	http.Error(w, "Invalid authentication request", http.StatusBadRequest)
}

/*
SwaggerDefs is used to describe the endpoint in swagger.
*/
func (le *loginEndpoint) SwaggerDefs(s map[string]interface{}) {

	s["paths"].(map[string]interface{})["/login"] = map[string]interface{}{
		"post": map[string]interface{}{
			"summary":     "Login as a user and create a session.",
			"description": "The login endpoint can be used to log in and create a new user session.",
			"consumes": []string{
				"application/x-www-form-urlencoded",
				"application/json",
			},
			"produces": []string{
				"text/plain",
			},
			"parameters": []map[string]interface{}{
				{
					"name":        "user",
					"in":          "formData",
					"description": "Username to log in.",
					"required":    true,
					"type":        "string",
				},
				{
					"name":        "pass",
					"in":          "formData",
					"description": "Cleartext password of the username.",
					"required":    true,
					"type":        "string",
				},
				{
					"name":        "redirect_ok",
					"in":          "formData",
					"description": "Redirect URL if the log in is successful.",
					"required":    false,
					"type":        "string",
				},
				{
					"name":        "redirect_notok",
					"in":          "formData",
					"description": "Redirect URL if the log in is not successful.",
					"required":    false,
					"type":        "string",
				},
			},
			"responses": map[string]interface{}{
				"302": map[string]interface{}{
					"description": "Redirect depending on the log in result.",
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
