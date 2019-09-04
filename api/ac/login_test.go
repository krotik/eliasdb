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
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAuthorization(t *testing.T) {

	queryURL := "http://localhost" + TESTPORT

	authCookie := doAuth("johndoe", "doe")

	res, resp := sendTestRequestResponse("application/json", queryURL+EndpointUser, "POST", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Requested create access to /db/user/ was denied" {
		t.Error("Unexpected result:", res, resp)
		return
	}

	w := httptest.NewRecorder()
	ACL.CheckHTTPRequest(w, resp.Request, "hans")

	if strings.TrimSpace(w.Body.String()) != "Requested create access to /db/user/ was denied" {
		t.Error("Unexpected result: ", w.Body.String())
		return
	}
}

func TestLoginEndpoint(t *testing.T) {

	queryURL := "http://localhost" + TESTPORT

	// Send request with wrong method

	res := sendTestRequest("application/x-www-form-urlencoded", queryURL+EndpointLogin, "GET", nil, nil)

	if res != "Method Not Allowed" {
		t.Error("Unexpected response:", res)
		return
	}

	// Send request without body

	res = sendTestRequest("application/json", queryURL+EndpointLogin, "POST", nil, nil)

	if res != "Could not decode request body: EOF" {
		t.Error("Unexpected response:", res)
		return
	}

	// Send correct authentication with ok redirect missing

	res, resp := sendTestRequestResponse("application/x-www-form-urlencoded", queryURL+EndpointLogin, "POST",
		[]byte(`user=elias&pass=elias&redirect_notok=/bar`), nil)

	if res != "404 page not found" || resp.Request.URL.Path != "/" {
		t.Error("Unexpected response:", res)
		return
	}

	// Send malformed authentication request

	res = sendTestRequest("application/x-www-form-urlencoded", queryURL+EndpointLogin, "POST", []byte(`us=elias`), nil)

	if res != "Invalid authentication request" {
		t.Error("Unexpected response:", res)
		return
	}

	// Send authentication request with wrong credentials

	res, resp = sendTestRequestResponse("application/x-www-form-urlencoded", queryURL+EndpointLogin, "POST",
		[]byte(`user=elias&pass=elias1&redirect_ok=/foo&redirect_notok=/bar`), nil)

	if resp.Request.URL.Path != "/bar" {
		t.Error("Unexpected request:", res, resp.Request.URL.Path)
		return
	}

	// Send authentication request with wrong credentials and no not ok url

	res, resp = sendTestRequestResponse("application/x-www-form-urlencoded", queryURL+EndpointLogin, "POST",
		[]byte(`user=elias&pass=elias1&redirect_ok=/foo`), nil)

	if resp.Request.URL.Path != "/db/login/" {
		t.Error("Unexpected request:", res, resp.Request.URL.Path)
		return
	}

	// Send authentication request with correct credentials

	res, resp = sendTestRequestResponse("application/x-www-form-urlencoded", queryURL+EndpointLogin, "POST",
		[]byte(`user=elias&pass=elias&redirect_ok=/foobar&redirect_notok=/bar`), nil)

	if resp.Request.URL.Path != "/foobar" {
		t.Error("Unexpected request:", res, resp.Request.URL.Path)
		return
	}

	// Send authentication request with correct credentials but with bad redirect

	res, resp = sendTestRequestResponse("application/x-www-form-urlencoded", queryURL+EndpointLogin, "POST",
		[]byte(`user=elias&pass=elias&redirect_ok=http://foobar/foo&redirect_notok=/bar`), nil)

	if res != "Redirection URL must not be an absolute URL" {
		t.Error("Unexpected request:", resp, res)
		return
	}

	// Send authentication request with incorrect credentials as json

	res, resp = sendTestRequestResponse("application/json",
		queryURL+EndpointLogin, "POST", []byte(`
{
	"user"           : "elias",
	"pass"           : "elias123"
}
`), nil)

	if resp.StatusCode != 401 {
		t.Error("Unexpected response:", resp, res)
		return
	}

	// Send authentication request with correct credentials as json

	res, resp = sendTestRequestResponse("application/json",
		queryURL+EndpointLogin, "POST", []byte(`
{
	"user"           : "elias",
	"pass"           : "elias"
}
`), nil)

	if resp.StatusCode != 200 {
		t.Error("Unexpected response:", resp, res)
		return
	}
}
