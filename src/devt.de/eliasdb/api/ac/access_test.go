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
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"

	"devt.de/common/datautil"
	"devt.de/common/errorutil"
	"devt.de/common/httputil"
	"devt.de/common/httputil/access"
	"devt.de/common/httputil/auth"
	"devt.de/common/stringutil"
	"devt.de/eliasdb/api"
)

const TESTPORT = ":9090"

// Main function for all tests in this package

func TestMain(m *testing.M) {
	var err error

	flag.Parse()

	hs, wg := startServer()
	if hs == nil {
		return
	}

	// Disable access logging

	LogAccess = func(v ...interface{}) {}

	// Register public endpoints

	api.RegisterRestEndpoints(PublicAccessControlEndpointMap)

	// Initialise auth handler

	AuthHandler = auth.NewCookieAuthHandleFuncWrapper(http.HandleFunc)

	// Important statement! - all registered endpoints afterwards
	// are subject to access control

	api.HandleFunc = AuthHandler.HandleFunc

	// Register management endpoints

	api.RegisterRestEndpoints(AccessManagementEndpointMap)

	// Register dummy page

	api.HandleFunc("/foo", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("foobar!"))
	})

	// Initialise user DB

	UserDB, err = datautil.NewEnforcedUserDB("test_user.db", "")
	errorutil.AssertOk(err)

	// Put the UserDB in charge of verifying passwords

	AuthHandler.SetAuthFunc(UserDB.CheckUserPassword)

	// Initialise ACL's

	var conf map[string]interface{}

	errorutil.AssertOk(json.Unmarshal(stringutil.StripCStyleComments(DefaultAccessDB), &conf))
	at, err := access.NewMemoryACLTableFromConfig(conf)
	errorutil.AssertOk(err)
	InitACLs(at)

	// Connect the ACL object to the AuthHandler - this provides authorization for users

	AuthHandler.SetAccessFunc(ACL.CheckHTTPRequest)

	// Adding special handlers which redirect to the login page

	AuthHandler.CallbackSessionExpired = CallbackSessionExpired
	AuthHandler.CallbackUnauthorized = CallbackUnauthorized

	// Add users

	UserDB.UserDB.AddUserEntry("elias", "elias", nil)
	UserDB.UserDB.AddUserEntry("johndoe", "doe", nil)
	UserDB.UserDB.AddUserEntry("guest", "g", nil)

	// Disable debounce time for unit tests

	DebounceTime = 0

	// Run the tests

	res := m.Run()

	// Teardown

	stopServer(hs, wg)

	// Stop ACL monitoring

	ACL.Close()

	// Remove files

	os.Remove("test_user.db")

	os.Exit(res)
}

func TestSwaggerDefs(t *testing.T) {

	// Test we can build swagger defs from the endpoint

	data := map[string]interface{}{
		"paths":       map[string]interface{}{},
		"definitions": map[string]interface{}{},
	}

	for _, inst := range PublicAccessControlEndpointMap {
		inst().SwaggerDefs(data)
	}
	for _, inst := range AccessManagementEndpointMap {
		inst().SwaggerDefs(data)
	}
}

/*
Start a HTTP test server.
*/
func startServer() (*httputil.HTTPServer, *sync.WaitGroup) {
	hs := &httputil.HTTPServer{}

	var wg sync.WaitGroup
	wg.Add(1)

	go hs.RunHTTPServer(TESTPORT, &wg)

	wg.Wait()

	// Server is started

	if hs.LastError != nil {
		panic(hs.LastError)
	}

	return hs, &wg
}

/*
Stop a started HTTP test server.
*/
func stopServer(hs *httputil.HTTPServer, wg *sync.WaitGroup) {

	if hs.Running == true {

		wg.Add(1)

		// Server is shut down

		hs.Shutdown()

		wg.Wait()

	} else {

		panic("Server was not running as expected")
	}
}

/*
Send a request to a HTTP test server
*/
func sendTestRequest(contentType string, url string, method string, content []byte,
	reqMod func(*http.Request)) string {

	body, _ := sendTestRequestResponse(contentType, url, method, content, reqMod)

	return body
}

/*
Send a request to a HTTP test server
*/
func sendTestRequestResponse(contentType string, url string, method string,
	content []byte, reqMod func(*http.Request)) (string, *http.Response) {

	var req *http.Request
	var err error

	if content != nil {
		req, err = http.NewRequest(method, url, bytes.NewBuffer(content))
	} else {
		req, err = http.NewRequest(method, url, nil)
	}
	req.Header.Set("Content-Type", contentType)

	if reqMod != nil {
		reqMod(req)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	bodyStr := strings.Trim(string(body), " \n")

	// Try json decoding first

	out := bytes.Buffer{}
	err = json.Indent(&out, []byte(bodyStr), "", "  ")
	if err == nil {
		return out.String(), resp
	}

	// Just return the body

	return bodyStr, resp
}

/*
Perform authentication and retrieve an auth cookie
*/
func doAuth(user, pass string) *http.Cookie {
	queryURL := "http://localhost" + TESTPORT

	// Send authentication request with correct credentials

	res, resp := sendTestRequestResponse("application/json", queryURL+EndpointLogin, "POST", []byte(`
{
	"user" : "`+user+`",
	"pass" : "`+pass+`"
}
`), nil)

	errorutil.AssertTrue(len(resp.Cookies()) > 0, res)

	// Right after authentication we only have the authentication cookie - after
	// the first visit to a non-public page we will also have a session cookie

	authCookie := resp.Cookies()[0]

	errorutil.AssertTrue(authCookie.Name == "~aid",
		fmt.Sprint("Unexpected name for cookie:", authCookie))

	return authCookie
}
