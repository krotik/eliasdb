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
	"sort"

	"devt.de/krotik/common/errorutil"
	"devt.de/krotik/common/httputil/access"
	"devt.de/krotik/eliasdb/api"
)

/*
EndpointUser is the user endpoint URL (rooted). Handles user/
*/
const EndpointUser = api.APIRoot + "/user/"

/*
EndpointWhoAmI is the current user endpoint URL (rooted). Handles whoami/
*/
const EndpointWhoAmI = api.APIRoot + "/whoami/"

/*
WhoAmIEndpointInst creates a new endpoint handler.
*/
func WhoAmIEndpointInst() api.RestEndpointHandler {
	return &whoAmIEndpoint{}
}

/*
Handler object for whoami operations.
*/
type whoAmIEndpoint struct {
	*api.DefaultEndpointHandler
}

/*
HandleGET handles user queries.
*/
func (we *whoAmIEndpoint) HandleGET(w http.ResponseWriter, r *http.Request, resources []string) {
	u, ok := AuthHandler.CheckAuth(r)
	w.Header().Set("content-type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"username":  u,
		"logged_in": ok,
	})
}

/*
SwaggerDefs is used to describe the endpoint in swagger.
*/
func (we *whoAmIEndpoint) SwaggerDefs(s map[string]interface{}) {

	s["paths"].(map[string]interface{})["/whoami"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "Return information about the current user.",
			"description": "Returns information about the current user.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "Current user information.",
					"schema": map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"username": map[string]interface{}{
								"description": "Name of the current user.",
								"type":        "string",
							},
							"logged_in": map[string]interface{}{
								"description": "Flag if the current user is logged in.",
								"type":        "boolean",
							},
						},
					},
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
}

/*
UserEndpointInst creates a new endpoint handler.
*/
func UserEndpointInst() api.RestEndpointHandler {
	return &userEndpoint{}
}

/*
Handler object for user operations.
*/
type userEndpoint struct {
	*api.DefaultEndpointHandler
}

/*
HandleGET handles user queries.
*/
func (ue *userEndpoint) HandleGET(w http.ResponseWriter, r *http.Request, resources []string) {
	var data interface{}

	// Check parameters

	if !checkResources(w, resources, 1, 2, "Need u or g (user/group) and optionally a name") {
		return
	}

	if resources[0] == "u" {
		var userData []map[string]interface{}

		dataItem := func(u string) (map[string]interface{}, error) {
			ud, ok := UserDB.UserData(u)

			if !ok {
				return nil, fmt.Errorf("User %s does not exist", u)
			}

			g, _ := ACL.GroupsOfUser(u)

			if g == nil {
				g = []string{}
			}

			return map[string]interface{}{
				"username": u,
				"groups":   g,
				"data":     ud,
			}, nil
		}

		if len(resources) > 1 {

			// Return only a single user

			item, err := dataItem(resources[1])

			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}

			w.Header().Set("content-type", "application/json; charset=utf-8")
			json.NewEncoder(w).Encode(item)
			return
		}

		users := UserDB.AllUsers()

		sort.Strings(users)

		for _, u := range users {
			item, _ := dataItem(u)
			userData = append(userData, item)
		}

		data = userData

	} else if resources[0] == "g" {

		groupData, _ := ACL.GetConfig()

		if len(resources) > 1 {
			var ok bool

			groupPerm := groupData["groups"].(map[string]map[string]string)

			if data, ok = groupPerm[resources[1]]; !ok {
				data = map[string]interface{}{}
			}

		} else {

			data = groupData["groups"]
		}
	}

	// Write data

	w.Header().Set("content-type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(data)
}

/*
HandlePOST handles a REST call to create new users and groups.
*/
func (ue *userEndpoint) HandlePOST(w http.ResponseWriter, r *http.Request, resources []string) {

	// Check parameters

	if !checkResources(w, resources, 2, 2, "Need u or g (user/group) and a name") {
		return
	}

	name := resources[1]

	if resources[0] == "u" {
		var userDataObject map[string]interface{}
		var groupDataObject []interface{}

		data := make(map[string]interface{})
		dec := json.NewDecoder(r.Body)

		if _, ok := UserDB.UserData(name); ok {

			// Shortcut the tests if the user already exists

			http.Error(w, fmt.Sprintf("Could not add user %s: User %s already exists", name, name),
				http.StatusBadRequest)
			return
		}

		if err := dec.Decode(&data); err != nil {
			http.Error(w, "Could not decode request body as object: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		password, ok := data["password"]
		if !ok {
			http.Error(w, "Password is missing in body object ", http.StatusBadRequest)
			return
		}

		if userData, ok := data["user_data"]; ok {
			if userDataObject, ok = userData.(map[string]interface{}); !ok {
				http.Error(w, "User data is not an object", http.StatusBadRequest)
				return
			}
		}

		if groupData, ok := data["group_list"]; ok {
			if groupDataObject, ok = groupData.([]interface{}); !ok {
				http.Error(w, "Group list is not a list", http.StatusBadRequest)
				return
			}

			names, _ := ACL.GroupNames()

			for _, g := range groupDataObject {
				group := fmt.Sprint(g)
				if i := sort.SearchStrings(names, group); !(i < len(names) && names[i] == group) {
					http.Error(w, fmt.Sprintf("Group %s does not exist", group), http.StatusBadRequest)
					return
				}
			}
		}

		if err := UserDB.AddUserEntry(name, fmt.Sprint(password), userDataObject); err != nil {
			http.Error(w, fmt.Sprintf("Could not add user %s: %s", name, err.Error()),
				http.StatusBadRequest)
			return
		}

		// Add user to various groups

		for _, g := range groupDataObject {
			ACL.AddUserToGroup(name, fmt.Sprint(g))
		}

	} else if resources[0] == "g" {

		if err := ACL.AddGroup(name); err != nil {
			http.Error(w, fmt.Sprintf("Could not add group %s: %s", name, err.Error()),
				http.StatusBadRequest)
			return
		}

	} else {
		http.Error(w, "Need u or g (user/group) as first path element", http.StatusBadRequest)
		return
	}
}

/*
HandlePUT handles a REST call to update an existing user or group.
*/
func (ue *userEndpoint) HandlePUT(w http.ResponseWriter, r *http.Request, resources []string) {
	var err error

	// Check parameters

	if !checkResources(w, resources, 2, 2, "Need u or g (user/group) and a name") {
		return
	}

	name := resources[1]

	if resources[0] == "u" {
		var updates []func() error
		var userDataObject map[string]interface{}
		var groupDataObject []interface{}

		if !UserDB.UserExists(name) {
			http.Error(w, fmt.Sprintf("User %s does not exist", name), http.StatusBadRequest)
			return
		}

		data := make(map[string]interface{})
		dec := json.NewDecoder(r.Body)

		if err = dec.Decode(&data); err != nil {
			http.Error(w, "Could not decode request body as object: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		if passwordObj, ok := data["password"]; ok {
			password := fmt.Sprint(passwordObj)

			if err = UserDB.IsAcceptablePassword(name, password); err == nil {
				updates = append(updates, func() error {
					return UserDB.UpdateUserPassword(name, password)
				})
			}
		}

		if err == nil {
			if userData, ok := data["user_data"]; ok {
				if userDataObject, ok = userData.(map[string]interface{}); !ok {
					http.Error(w, "User data is not an object", http.StatusBadRequest)
					return
				}
				updates = append(updates, func() error {
					return UserDB.UpdateUserData(name, userDataObject)
				})
			}

			if groupData, ok := data["group_list"]; ok {
				var userGroups []string

				if groupDataObject, ok = groupData.([]interface{}); !ok {
					http.Error(w, "Group list is not a list", http.StatusBadRequest)
					return
				}

				userGroups, _ = ACL.GroupsOfUser(name) // Ignore error here if the user does not exist

				var names []string

				names, err = ACL.GroupNames()
				if err == nil {

					for _, g := range groupDataObject {
						group := fmt.Sprint(g)
						if i := sort.SearchStrings(names, group); !(i < len(names) && names[i] == group) {
							http.Error(w, fmt.Sprintf("Group %s does not exist", group), http.StatusBadRequest)
							return
						}
					}

					// No errors are expected when executing the transaction

					for _, g := range userGroups {
						errorutil.AssertOk(ACL.RemoveUserFromGroup(name, fmt.Sprint(g)))
					}
					for _, g := range groupDataObject {
						errorutil.AssertOk(ACL.AddUserToGroup(name, fmt.Sprint(g)))
					}
				}
			}

			if err == nil {

				//  Execute the rest of the updates - no errors expected here

				for _, f := range updates {
					errorutil.AssertOk(f())
				}
			}
		}

	} else if resources[0] == "g" {

		// Replace all permissions for a given group

		if _, err = ACL.Permissions(name); err != nil {
			http.Error(w, fmt.Sprintf("Group %s does not exist", name), http.StatusBadRequest)
			return
		}

		data := make(map[string]interface{})
		dec := json.NewDecoder(r.Body)

		if err = dec.Decode(&data); err != nil {
			http.Error(w, "Could not decode request body as object: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		for _, perm := range data {
			if _, err = access.RightsFromString(fmt.Sprint(perm)); err != nil {
				break
			}
		}

		if err == nil {
			errorutil.AssertOk(ACL.ClearPermissions(name))

			for path, perm := range data {
				r, _ := access.RightsFromString(fmt.Sprint(perm))
				errorutil.AssertOk(ACL.AddPermission(name, path, r))
			}
		}

	} else {
		err = fmt.Errorf("Need u or g (user/group) as first path element")
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}

/*
HandleDELETE handles a REST call to remove an existing user or group.
*/
func (ue *userEndpoint) HandleDELETE(w http.ResponseWriter, r *http.Request, resources []string) {

	// Check parameters

	if !checkResources(w, resources, 2, 2, "Need u or g (user/group) and a name") {
		return
	}

	name := resources[1]

	if resources[0] == "u" {

		if err := UserDB.RemoveUserEntry(name); err != nil {
			http.Error(w, fmt.Sprintf("Could not remove user %s: %s", name, err.Error()),
				http.StatusBadRequest)
			return
		}

	} else if resources[0] == "g" {

		if err := ACL.RemoveGroup(name); err != nil {
			http.Error(w, fmt.Sprintf("Could not remove group %s: %s", name, err.Error()),
				http.StatusBadRequest)
			return
		}

	} else {
		http.Error(w, "Need u or g (user/group) as first path element", http.StatusBadRequest)
		return
	}
}

/*
SwaggerDefs is used to describe the endpoint in swagger.
*/
func (ue *userEndpoint) SwaggerDefs(s map[string]interface{}) {

	username := []map[string]interface{}{
		{
			"name":        "name",
			"in":          "path",
			"description": "Name of user.",
			"required":    true,
			"type":        "string",
		},
	}

	groupname := []map[string]interface{}{
		{
			"name":        "name",
			"in":          "path",
			"description": "Name of group.",
			"required":    true,
			"type":        "string",
		},
	}

	createParams := []map[string]interface{}{
		{
			"name":        "user_creation_data",
			"in":          "body",
			"description": "Additional data to create a user account",
			"required":    true,
			"schema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"password": map[string]interface{}{
						"description": "Password for the new user.",
						"type":        "string",
					},
					"user_data": map[string]interface{}{
						"description": "Additional user data.",
						"type":        "object",
					},
					"group_list": map[string]interface{}{
						"description": "List of groups.",
						"type":        "array",
						"items": map[string]interface{}{
							"type": "string",
						},
					},
				},
			},
		},
	}

	updateParams := []map[string]interface{}{
		{
			"name":        "user_update_data",
			"in":          "body",
			"description": "Additional data to update a user account",
			"required":    true,
			"schema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"password": map[string]interface{}{
						"description": "New password for the user.",
						"type":        "string",
					},
					"user_data": map[string]interface{}{
						"description": "New additional user data.",
						"type":        "object",
					},
					"group_list": map[string]interface{}{
						"description": "New list of groups.",
						"type":        "array",
						"items": map[string]interface{}{
							"type": "string",
						},
					},
				},
			},
		},
	}

	permParams := []map[string]interface{}{
		{
			"name":        "permission_data",
			"in":          "body",
			"description": "Resource paths and their permissions.",
			"required":    true,
			"schema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"resource_path": map[string]interface{}{
						"description": "Access rights to the resource path as CRUD (create, read, update and delete) string (e.g. '-RU-').",
						"type":        "string",
						"example":     "CRUD",
					},
				},
			},
		},
	}

	s["paths"].(map[string]interface{})["/user/u"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "Return information about all current known users.",
			"description": "Returns all registered users.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "List of known users.",
					"schema": map[string]interface{}{
						"type": "array",
						"items": map[string]interface{}{
							"type": "object",
							"properties": map[string]interface{}{
								"username": map[string]interface{}{
									"description": "Name of the user.",
									"type":        "string",
								},
								"groups": map[string]interface{}{
									"description": "Groups of the user.",
									"type":        "array",
									"items": map[string]interface{}{
										"type": "string",
									},
								},
								"data": map[string]interface{}{
									"description": "Extra data for the user.",
									"type":        "object",
								},
							},
						},
					},
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

	s["paths"].(map[string]interface{})["/user/g"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "Return information about all known groups and their permissions.",
			"description": "Returns all known groups.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "Known group.",
					"schema": map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"group_name": map[string]interface{}{
								"description": "Resource path.",
								"type":        "object",
								"properties": map[string]interface{}{
									"resource_path": map[string]interface{}{
										"description": "Access rights to the resource path as CRUD (create, read, update and delete) string (e.g. '-RU-').",
										"type":        "string",
										"example":     "CRUD",
									},
								},
							},
						},
					},
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

	s["paths"].(map[string]interface{})["/user/u/{name}"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "Return information about a current known user.",
			"description": "Returns a registered user.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": username,
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "Information about a single user.",
					"schema": map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"username": map[string]interface{}{
								"description": "Name of the user.",
								"type":        "string",
							},
							"groups": map[string]interface{}{
								"description": "Groups of the user.",
								"type":        "array",
								"items": map[string]interface{}{
									"type": "string",
								},
							},
							"data": map[string]interface{}{
								"description": "Extra data for the user.",
								"type":        "object",
							},
						},
					},
				},
				"default": map[string]interface{}{
					"description": "Error response",
					"schema": map[string]interface{}{
						"$ref": "#/definitions/Error",
					},
				},
			},
		},
		"post": map[string]interface{}{
			"summary":     "Create a new user.",
			"description": "Create a new user.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": append(username, createParams...),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "Request was successful.",
				},
				"default": map[string]interface{}{
					"description": "Error response",
					"schema": map[string]interface{}{
						"$ref": "#/definitions/Error",
					},
				},
			},
		},
		"put": map[string]interface{}{
			"summary":     "Update an existing user.",
			"description": "Update an existing user.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": append(username, updateParams...),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "Request was successful.",
				},
				"default": map[string]interface{}{
					"description": "Error response",
					"schema": map[string]interface{}{
						"$ref": "#/definitions/Error",
					},
				},
			},
		},
		"delete": map[string]interface{}{
			"summary":     "Delete an existing user.",
			"description": "Delete an existing user.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": username,
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "Request was successful.",
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

	s["paths"].(map[string]interface{})["/user/g/{name}"] = map[string]interface{}{
		"get": map[string]interface{}{
			"summary":     "Return information about a group's permissions.",
			"description": "Returns the permissions of a group.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": groupname,
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "Resource paths and their permissions.",
					"schema": map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"resource_path": map[string]interface{}{
								"description": "Access rights to the resource path as CRUD (create, read, update and delete) string (e.g. '-RU-').",
								"type":        "string",
								"example":     "CRUD",
							},
						},
					},
				},
				"default": map[string]interface{}{
					"description": "Error response",
					"schema": map[string]interface{}{
						"$ref": "#/definitions/Error",
					},
				},
			},
		},
		"post": map[string]interface{}{
			"summary":     "Create a new group.",
			"description": "Create a new group.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": groupname,
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "Request was successful.",
				},
				"default": map[string]interface{}{
					"description": "Error response",
					"schema": map[string]interface{}{
						"$ref": "#/definitions/Error",
					},
				},
			},
		},
		"put": map[string]interface{}{
			"summary":     "Set permissions of an existing group.",
			"description": "Set permissions of an existing group.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": append(groupname, permParams...),
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "Request was successful.",
				},
				"default": map[string]interface{}{
					"description": "Error response",
					"schema": map[string]interface{}{
						"$ref": "#/definitions/Error",
					},
				},
			},
		},
		"delete": map[string]interface{}{
			"summary":     "Delete an existing group.",
			"description": "Delete an existing group.",
			"produces": []string{
				"text/plain",
				"application/json",
			},
			"parameters": groupname,
			"responses": map[string]interface{}{
				"200": map[string]interface{}{
					"description": "Request was successful.",
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
