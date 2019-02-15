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
	"testing"

	"devt.de/common/httputil/user"
)

func TestLogoutEndpoint(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT

	authCookie := doAuth("johndoe", "doe")

	// Send request with auth cookie to the user endpoint

	res, resp := sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/", "GET", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != `[
  {
    "data": null,
    "groups": [
      "admin",
      "public"
    ],
    "username": "elias"
  },
  {
    "data": null,
    "groups": [],
    "username": "guest"
  },
  {
    "data": null,
    "groups": [
      "public"
    ],
    "username": "johndoe"
  }
]` {
		t.Error("Unexpected response:", res, resp)
	}

	// Do the logout but use a page submisssion

	res, resp = sendTestRequestResponse("application/x-www-form-urlencodedt", queryURL+EndpointLogout, "POST", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if resp.Request.URL.Path != "/" {
		t.Error("Unexpected request:", res, resp.Request.URL.Path)
		return
	}

	// Next request with auth cookie should fail since we are logged out

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser, "GET", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Valid credentials required" {
		t.Error("Unexpected response:", res, resp)
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+"/foo?abc=123", "GET", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if resp.Request.URL.Path != "/login.html" || resp.Request.URL.RawQuery != "ref=%2Ffoo%3Fabc%3D123" {
		t.Error("Unexpected response:", resp.Request.URL.Path, resp.Request.URL.RawQuery)
		return
	}
}

func TestSessionExpiry(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT

	authCookie := doAuth("johndoe", "doe")

	// Send request with auth cookie to the user endpoint

	_, resp := sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/?abc=123", "GET", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if resp.StatusCode != 200 {
		t.Error("Unexpected response:", resp)
		return
	}

	// Remove all underlying sessions

	sessions, _ := user.UserSessionManager.Provider.GetAll()
	for _, s := range sessions {
		user.UserSessionManager.Provider.Destroy(s.ID())
	}

	// Next request with auth cookie should fail

	_, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"?abc=123", "GET", nil,
		func(req *http.Request) {
			for _, c := range resp.Cookies() {

				// Add auth and session cookie otherwise the session will be recreated

				req.AddCookie(c)
			}
		})

	// The session is expired which causes the invalidation of the authentication cookie
	// and a redirect to login

	if resp.Request.URL.Path != "/login.html" {
		t.Error("Unexpected response:", resp)
		return
	}
}
