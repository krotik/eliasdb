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
Package ac contains management code for access control.
*/
package ac

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"

	"devt.de/krotik/common/datautil"
	"devt.de/krotik/common/httputil/access"
	"devt.de/krotik/common/httputil/auth"
	"devt.de/krotik/common/httputil/user"
	"devt.de/krotik/eliasdb/api"
)

// Code and datastructures relating to access control
// ==================================================

/*
PublicAccessControlEndpointMap contains endpoints which should be publically
available when access control is used
*/
var PublicAccessControlEndpointMap = map[string]api.RestEndpointInst{
	EndpointLogin:  LoginEndpointInst,
	EndpointLogout: LogoutEndpointInst,
	EndpointWhoAmI: WhoAmIEndpointInst,
}

/*
AccessManagementEndpointMap contains endpoints which can manage access rights
*/
var AccessManagementEndpointMap = map[string]api.RestEndpointInst{
	EndpointUser: UserEndpointInst,
}

/*
LogAccess is used to log access requests
*/
var LogAccess = log.Print

/*
UserDB is the global user database which holds the password hashes and user
details.
*/
var UserDB *datautil.EnforcedUserDB

/*
ACL is the global AccessControlLists object which should be used to check
user access rights.
*/
var ACL *AccessControlLists

/*
AuthHandler is a wrapper object which has a HandleFunc similar to http.HandleFunc.
The HandleFunc of this object should be used for all endpoint which should check
for authentication and authorization.
*/
var AuthHandler *auth.CookieAuthHandleFuncWrapper

/*
DefaultAccessDB is the default access table for EliasDB
*/
var DefaultAccessDB = []byte(`
/*
Access control file for EliasDB. This file controls the access rights for each user.
Rights to resources are assigned to groups. Users are assigned to groups.

This file is monitored by the server - any changes to this file are picked up
by the server immediately. Equally, any change on the server side is immediately
written to this file.

The comments in this file are for initial comprehension only. They will be
removed as soon as the users, groups or permissions are modified from the
server side.
*/
{
  "groups": {
    "public": {

      // Page access
      // ===========

      "/": "-R--",          // Access to the root page

      // Resource access
      // ===============

      "/css/*": "-R--",    // Access to CSS rules
      "/js/*": "-R--",     // Access to JavaScript files
      "/img/*": "-R--",    // Access to image files
      "/vendor/*": "-R--", // Access to frontend libraries

      // REST API access
      // ===============

      "/db/*": "-R--"      // Access to database (read)
    },
    "admin": {

      // REST API access
      // ===============

      "/db/*": "CRUD"      // Access to database
    }
  },
  "users": {
    "elias": [    // Default EliasDB admin user
      "public",
      "admin"
    ],
	"johndoe" : [ // Default EliasDB public user
	  "public"
	]
  }
}
`[1:])

/*
InitACLs initializes the access control list object.
*/
func InitACLs(tab access.ACLTable) {
	ACL = &AccessControlLists{tab}
}

// Access request types
//
const (
	CREATE = "create"
	READ   = "read"
	UPDATE = "update"
	DELETE = "delete"
)

// Access request results
//
const (
	GRANTED = "granted"
	DENIED  = "denied"
)

// Mapping from http request method to access request type
//
var httpRequestMapping = map[string]string{
	"":       READ,
	"get":    READ,
	"put":    UPDATE,
	"post":   CREATE,
	"delete": DELETE,
}

/*
AccessControlLists store the access rights of groups and which users are
member of which groups.
*/
type AccessControlLists struct {
	access.ACLTable
}

/*
CheckHTTPRequest checks the request of a given user to a resource.
*/
func (a *AccessControlLists) CheckHTTPRequest(w http.ResponseWriter, r *http.Request, user string) bool {
	var result = DENIED
	var detail = "No rule which grants access was found"

	// Extract request details

	requestType := httpRequestMapping[strings.ToLower(r.Method)]
	requestResource := r.URL.Path

	// Build rights object

	requestRights := &access.Rights{
		Create: requestType == CREATE,
		Read:   requestType == READ,
		Update: requestType == UPDATE,
		Delete: requestType == DELETE,
	}

	// Check ACLTable

	if res, resDetail, err := a.IsPermitted(user, requestResource, requestRights); res && err == nil {
		result = GRANTED
		detail = resDetail
	} else if err != nil {
		detail = err.Error()
	}

	// Log the result

	text := fmt.Sprintf("User %v requested %v access to %v - %v (%v)",
		user, requestType, requestResource, result, detail)

	if result == GRANTED {
		LogAccess(text)
	} else {
		LogAccess(text)
		http.Error(w, fmt.Sprintf("Requested %v access to %v was denied",
			requestType, requestResource),
			http.StatusForbidden)
	}

	return result == GRANTED
}

// Default error handlers

/*
CallbackSessionExpired handles requests where the session has expired.
*/
var CallbackSessionExpired = func(w http.ResponseWriter, r *http.Request) {

	u, ok := AuthHandler.CheckAuth(r)

	// Remove all cookies

	AuthHandler.RemoveAuthCookie(w)
	user.UserSessionManager.RemoveSessionCookie(w)

	if ok {
		LogAccess("User ", u, " session expired")
	}

	origPath := r.URL.Path
	if r.URL.RawQuery != "" {
		origPath += "?" + r.URL.RawQuery
	}

	http.Redirect(w, r, fmt.Sprintf("/login.html?msg=Session+Expired&ref=%v",
		url.QueryEscape(origPath)), http.StatusFound)
}

/*
CallbackUnauthorized handles requests which are unauthorized.
*/
var CallbackUnauthorized = func(w http.ResponseWriter, r *http.Request) {

	LogAccess("Unauthorized request to ", r.URL.Path,
		" from ", r.RemoteAddr, " (", r.UserAgent(), " Cookies: ", r.Cookies(), ")")

	if strings.HasPrefix(r.URL.Path, api.APIRoot) {

		// No redirect for REST clients

		http.Error(w, "Valid credentials required", http.StatusForbidden)

	} else {
		origPath := r.URL.Path
		if r.URL.RawQuery != "" {
			origPath += "?" + r.URL.RawQuery
		}

		http.Redirect(w, r, fmt.Sprintf("/login.html?ref=%v",
			url.QueryEscape(origPath)), http.StatusFound)
	}
}

// Helper functions
// ================

/*
checkResources check given resources for a GET request.
*/
func checkResources(w http.ResponseWriter, resources []string, requiredMin int, requiredMax int, errorMsg string) bool {
	if len(resources) < requiredMin {
		http.Error(w, errorMsg, http.StatusBadRequest)
		return false
	} else if len(resources) > requiredMax {
		http.Error(w, "Invalid resource specification: "+strings.Join(resources[1:], "/"), http.StatusBadRequest)
		return false
	}
	return true
}
