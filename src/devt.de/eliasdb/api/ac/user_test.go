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
	"strings"
	"testing"
)

func TestUserEndpoint(t *testing.T) {
	queryURL := "http://localhost" + TESTPORT

	res, resp := sendTestRequestResponse("application/json", queryURL+EndpointWhoAmI, "GET", nil, nil)

	if res != `{
  "logged_in": false,
  "username": ""
}` {
		t.Error("Unexpected response:", res, resp)
	}

	authCookie := doAuth("johndoe", "doe")

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointWhoAmI, "GET", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != `{
  "logged_in": true,
  "username": "johndoe"
}` {
		t.Error("Unexpected response:", res, resp)
	}

	// Send request with auth cookie to the user endpoint

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/", "GET", nil,
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
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/elias", "GET", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"g/", "GET", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != `{
  "admin": {
    "/db/*": "CRUD"
  },
  "public": {
    "/": "-R--",
    "/css/*": "-R--",
    "/db/*": "-R--",
    "/img/*": "-R--",
    "/js/*": "-R--",
    "/vendor/*": "-R--"
  }
}` {
		t.Error("Unexpected response:", res, resp)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"g/public", "GET", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != `{
  "/": "-R--",
  "/css/*": "-R--",
  "/db/*": "-R--",
  "/img/*": "-R--",
  "/js/*": "-R--",
  "/vendor/*": "-R--"
}` {
		t.Error("Unexpected response:", res, resp)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"g/publi", "GET", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != `{}` {
		t.Error("Unexpected response:", res, resp)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/elias", "GET", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != `{
  "data": null,
  "groups": [
    "admin",
    "public"
  ],
  "username": "elias"
}` {
		t.Error("Unexpected response:", res, resp)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser, "GET", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Need u or g (user/group) and optionally a name" {
		t.Error("Unexpected response:", res, resp)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/foobar", "GET", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "User foobar does not exist" {
		t.Error("Unexpected response:", res, resp)
		return
	}

	// Create another account

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/hans", "POST",
		[]byte(`{
	"password": "123",
	"user_data": {
		"hobby": "fishing",
		"age":   35
	}
}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Requested create access to /db/user/u/hans was denied" {
		t.Error("Unexpected result:", res)
		return
	}

	authCookie = doAuth("elias", "elias")

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"x", "POST", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Need u or g (user/group) and a name" {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"x/bla", "POST", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Need u or g (user/group) as first path element" {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"x/bla/xxx", "POST", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Invalid resource specification: bla/xxx" {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/hans", "POST",
		[]byte(`{
	"password": "123"xxx
	"user_data": {
		"hobby": "fishing",
		"age":   35
	}
}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Could not decode request body as object: invalid character 'x' after object key:value pair" {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/hans", "POST",
		[]byte(`{
	"password": "123",
	"user_data": 123
}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "User data is not an object" {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/hans", "POST",
		[]byte(`{
	"password": "123",
	"user_data": {}
}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if !strings.HasPrefix(res, "Could not add user hans: Password matches a common dictionary password") {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/hans", "POST",
		[]byte(`{
	"user_data": {}
}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if !strings.HasPrefix(res, "Password is missing in body object") {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/hans", "POST",
		[]byte(`{
	"password"  : "SolidFoundat!0n",
	"user_data" : {
		"hobby" : "fishing",
		"age"   : 35
	},
	"group_list" : [ "public" ]
}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if resp.StatusCode != 200 {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/", "GET", nil,
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
    "data": {
      "age": 35,
      "hobby": "fishing"
    },
    "groups": [
      "public"
    ],
    "username": "hans"
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
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/hans", "POST",
		[]byte(`{
	"password"  : "SolidFoundat!0n",
	"user_data" : {
		"hobby" : "fishing",
		"age"   : 35
	},
	"group_list" : [ "public" ]
}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Could not add user hans: User hans already exists" {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/hans2", "POST",
		[]byte(`{
	"password"  : "SolidFoundat!0n",
	"user_data" : {
		"hobby" : "fishing",
		"age"   : 35
	},
	"group_list" : [ "public", "foo" ]
}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Group foo does not exist" {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/hans2", "POST",
		[]byte(`{
	"password"  : "SolidFoundat!0n",
	"user_data" : {
		"hobby" : "fishing",
		"age"   : 35
	},
	"group_list" : 1
}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Group list is not a list" {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"g/meyer", "POST", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if resp.StatusCode != 200 {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"g/", "GET", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != `{
  "admin": {
    "/db/*": "CRUD"
  },
  "meyer": {},
  "public": {
    "/": "-R--",
    "/css/*": "-R--",
    "/db/*": "-R--",
    "/img/*": "-R--",
    "/js/*": "-R--",
    "/vendor/*": "-R--"
  }
}` {
		t.Error("Unexpected response:", res, resp)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"g/meyer", "POST",
		[]byte(`{
	"password": "123",
	"user_data": 123
}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Could not add group meyer: Group meyer added twice" {
		t.Error("Unexpected result:", res)
		return
	}

	// Update an existing user

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/hans", "PUT",
		[]byte(`{
	"password"  : "xyzSolidFoundat!0n",
	"user_data" : {
		"hobby" : "riding",
		"age"   : 36
	},
	"group_list" : [ "public", "admin", "meyer" ]
}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if resp.StatusCode != 200 {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/", "GET", nil,
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
    "data": {
      "age": 36,
      "hobby": "riding"
    },
    "groups": [
      "admin",
      "meyer",
      "public"
    ],
    "username": "hans"
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
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/hans", "PUT",
		[]byte(`{
	"password"  : "xxx",
	"user_data" : {
		"hobby" : "riding",
		"age"   : 36
	},
	"group_list" : [ "public", "admin" ]
}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if !strings.HasPrefix(res, "Password must") {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/hans", "PUT",
		[]byte(`{
	"user_data" : 1,
	"group_list" : [ "public", "admin" ]
}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "User data is not an object" {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"g/hans", "PUT",
		[]byte(`{}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Group hans does not exist" {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"g/admin", "PUT",
		[]byte(`{xxx}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Could not decode request body as object: invalid character 'x' looking for beginning of object key string" {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"g/", "PUT",
		[]byte(`{}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Need u or g (user/group) and a name" {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"x/xxx", "PUT",
		[]byte(`{}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Need u or g (user/group) as first path element" {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/foo", "PUT",
		[]byte(`{}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "User foo does not exist" {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/hans", "PUT",
		[]byte(`{
	"group_list" : 1
}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Group list is not a list" {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/hans", "PUT",
		[]byte(`{
	"group_list" : [ "admin" ]
`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Could not decode request body as object: unexpected EOF" {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/hans", "PUT",
		[]byte(`{
	"password" : "66adm!nA",
	"user_data" : {
		"hobby" : "nothing"
	},
	"group_list" : [ "public", "admin", "foo" ]
}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Group foo does not exist" {
		t.Error("Unexpected result:", res)
		return
	}

	// Make sure non of the failed requests did a partial update

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/", "GET", nil,
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
    "data": {
      "age": 36,
      "hobby": "riding"
    },
    "groups": [
      "admin",
      "meyer",
      "public"
    ],
    "username": "hans"
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
		return
	}

	// ########################

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"g/meyer", "GET", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "{}" {
		t.Error("Unexpected response:", res, resp)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"g/meyer", "PUT",
		[]byte(`{
  "/": "-R--",
  "/css/*": "-RU-",
  "/styles/*": "-R--",
  "/db/*": "-R--",
  "/img/*": "-R--",
  "/js/*": "-R--",
  "/vendor/*": "-R--"
}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if resp.StatusCode != 200 {
		t.Error("Unexpected response:", res, resp)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"g/meyer", "GET", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != `{
  "/": "-R--",
  "/css/*": "-RU-",
  "/db/*": "-R--",
  "/img/*": "-R--",
  "/js/*": "-R--",
  "/styles/*": "-R--",
  "/vendor/*": "-R--"
}` {
		t.Error("Unexpected response:", res, resp)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"g/meyer", "PUT",
		[]byte(`{
  "/": "-R--",
  "/css/*": "-RU-",
  "/styles/*": "-W--",
  "/db/*": "-R--",
  "/img/*": "-R--",
  "/js/*": "-R--",
  "/vendor/*": "-R--"
}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Read permission in rights string must be either 'r' or '-'" {
		t.Error("Unexpected response:", res, resp)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"g/meyer", "PUT",
		[]byte(`{
  "/": "-R--"
}`),
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if resp.StatusCode != 200 {
		t.Error("Unexpected response:", res, resp)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"g/meyer", "GET", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != `{
  "/": "-R--"
}` {
		t.Error("Unexpected response:", res, resp)
		return
	}

	// ########################

	// Delete things

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"g/meyer", "DELETE", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if resp.StatusCode != 200 {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/", "GET", nil,
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
    "data": {
      "age": 36,
      "hobby": "riding"
    },
    "groups": [
      "admin",
      "public"
    ],
    "username": "hans"
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
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/hans", "DELETE", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if resp.StatusCode != 200 {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/", "GET", nil,
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
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"g/meyer", "DELETE", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Could not remove group meyer: Group meyer does not exist" {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"u/foo", "DELETE", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Could not remove user foo: Unknown user foo" {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"x/meyer", "DELETE", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Need u or g (user/group) as first path element" {
		t.Error("Unexpected result:", res)
		return
	}

	res, resp = sendTestRequestResponse("application/json", queryURL+EndpointUser+"x", "DELETE", nil,
		func(req *http.Request) {
			req.AddCookie(authCookie)
		})

	if res != "Need u or g (user/group) and a name" {
		t.Error("Unexpected result:", res)
		return
	}
}
