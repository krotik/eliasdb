/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package console

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"devt.de/common/stringutil"
	"devt.de/eliasdb/api/ac"
)

// Command: users
// ==============

/*
CommandUsers is a command name.
*/
const CommandUsers = "users"

/*
CmdUsers returns a list of all users.
*/
type CmdUsers struct {
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdUsers) Name() string {
	return CommandUsers
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdUsers) ShortDescription() string {
	return "Returns a list of all users."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdUsers) LongDescription() string {
	return "Returns a table of all users and their groups."
}

/*
Run executes the command.
*/
func (c *CmdUsers) Run(args []string, capi CommandConsoleAPI) error {
	res, err := capi.Req(ac.EndpointUser+"u/", "GET", nil)

	if err == nil {
		var data = res.([]interface{})
		var tab []string

		tab = append(tab, "Username")
		tab = append(tab, "Groups")

		for _, d := range data {
			u := d.(map[string]interface{})
			tab = append(tab, fmt.Sprint(u["username"]))

			var groups []string
			for _, g := range u["groups"].([]interface{}) {
				groups = append(groups, fmt.Sprint(g))
			}
			tab = append(tab, strings.Join(groups, "/"))
		}

		capi.ExportBuffer().WriteString(stringutil.PrintCSVTable(tab, 2))

		fmt.Fprint(capi.Out(), stringutil.PrintGraphicStringTable(tab, 2, 1,
			stringutil.SingleLineTable))
	}

	return err
}

// Command: groups
// ===============

/*
CommandGroups is a command name.
*/
const CommandGroups = "groups"

/*
CmdGroups returns a list of all groups and their permissions.
*/
type CmdGroups struct {
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdGroups) Name() string {
	return CommandGroups
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdGroups) ShortDescription() string {
	return "Returns a list of all groups and their permissions."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdGroups) LongDescription() string {
	return "Returns a list of all groups and their permissions."
}

/*
Run executes the command.
*/
func (c *CmdGroups) Run(args []string, capi CommandConsoleAPI) error {
	res, err := capi.Req(ac.EndpointUser+"g/", "GET", nil)

	if err == nil {
		var data = res.(map[string]interface{})
		var groups []string
		var tab []string

		tab = append(tab, "Group")
		tab = append(tab, "Path")
		tab = append(tab, "Permissions")

		for g := range data {
			groups = append(groups, g)
		}
		sort.Strings(groups)

		for _, g := range groups {
			var paths []string
			perms := data[g].(map[string]interface{})

			for p := range perms {
				paths = append(paths, p)
			}
			sort.Strings(paths)

			if len(paths) > 0 {
				for i, p := range paths {
					if i == 0 {
						tab = append(tab, g)
					} else {
						tab = append(tab, "")
					}
					tab = append(tab, p)
					tab = append(tab, fmt.Sprint(perms[p]))
				}
			} else {
				tab = append(tab, g)
				tab = append(tab, "")
				tab = append(tab, "")
			}
		}

		capi.ExportBuffer().WriteString(stringutil.PrintCSVTable(tab, 3))

		fmt.Fprint(capi.Out(), stringutil.PrintGraphicStringTable(tab, 3, 1,
			stringutil.SingleLineTable))
	}

	return err
}

// Command: useradd
// ================

/*
CommandUseradd is a command name.
*/
const CommandUseradd = "useradd"

/*
CmdUseradd adds a user.
*/
type CmdUseradd struct {
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdUseradd) Name() string {
	return CommandUseradd
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdUseradd) ShortDescription() string {
	return "Adds a user to the system."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdUseradd) LongDescription() string {
	return "Adds a user to the system."
}

/*
Run executes the command.
*/
func (c *CmdUseradd) Run(args []string, capi CommandConsoleAPI) error {

	if len(args) < 1 {
		return fmt.Errorf("Please specify a username")
	}

	user := args[0]
	pass := capi.AskPassword()

	data, err := json.Marshal(map[string]interface{}{
		"password":   pass,
		"user_data":  map[string]interface{}{},
		"group_list": []string{},
	})

	if err == nil {
		_, err = capi.Req(ac.EndpointUser+"u/"+user, "POST", data)

		if err == nil {
			fmt.Fprintln(capi.Out(), fmt.Sprintf("User %s was created", user))
		}
	}

	return err
}

// Command: newpass
// ================

/*
CommandNewpass is a command name.
*/
const CommandNewpass = "newpass"

/*
CmdNewpass changes the password of a user.
*/
type CmdNewpass struct {
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdNewpass) Name() string {
	return CommandNewpass
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdNewpass) ShortDescription() string {
	return "Changes the password of a user."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdNewpass) LongDescription() string {
	return "Changes the password of a user."
}

/*
Run executes the command.
*/
func (c *CmdNewpass) Run(args []string, capi CommandConsoleAPI) error {

	if len(args) < 1 {
		return fmt.Errorf("Please specify a username")
	}

	user := args[0]
	pass := capi.AskPassword()

	data, err := json.Marshal(map[string]interface{}{
		"password": pass,
	})

	if err == nil {
		_, err = capi.Req(ac.EndpointUser+"u/"+user, "PUT", data)

		if err == nil {
			fmt.Fprintln(capi.Out(), fmt.Sprintf("Password for user %s was changed", user))
		}
	}

	return err
}

// Command: joingroup
// ==================

/*
CommandJoingroup is a command name.
*/
const CommandJoingroup = "joingroup"

/*
CmdJoingroup joins a user to a group.
*/
type CmdJoingroup struct {
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdJoingroup) Name() string {
	return CommandJoingroup
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdJoingroup) ShortDescription() string {
	return "Joins a user to a group."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdJoingroup) LongDescription() string {
	return "Joins a user to a group."
}

/*
Run executes the command.
*/
func (c *CmdJoingroup) Run(args []string, capi CommandConsoleAPI) error {

	if len(args) < 2 {
		return fmt.Errorf("Please specify a username and a group")
	}

	user := args[0]
	group := args[1]

	res, err := capi.Req(ac.EndpointUser+"u/"+user, "GET", nil)

	if err == nil {
		groups := res.(map[string]interface{})["groups"].([]interface{})

		for _, g := range groups {
			if g == group {
				err = fmt.Errorf("User %s is already member of group %s", user, group)
				break
			}
		}

		if err == nil {
			var data []byte

			data, err = json.Marshal(map[string]interface{}{
				"group_list": append(groups, group),
			})

			if err == nil {
				_, err = capi.Req(ac.EndpointUser+"u/"+user, "PUT", data)

				if err == nil {
					fmt.Fprintln(capi.Out(), fmt.Sprintf("User %s has joined group %s", user, group))
				}
			}
		}
	}

	return err
}

// Command: leavegroup
// ===================

/*
CommandLeavegroup is a command name.
*/
const CommandLeavegroup = "leavegroup"

/*
CmdLeavegroup removes a user from a group.
*/
type CmdLeavegroup struct {
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdLeavegroup) Name() string {
	return CommandLeavegroup
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdLeavegroup) ShortDescription() string {
	return "Removes a user from a group."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdLeavegroup) LongDescription() string {
	return "Removes a user from a group."
}

/*
Run executes the command.
*/
func (c *CmdLeavegroup) Run(args []string, capi CommandConsoleAPI) error {

	if len(args) < 2 {
		return fmt.Errorf("Please specify a username and a group")
	}

	user := args[0]
	group := args[1]

	res, err := capi.Req(ac.EndpointUser+"u/"+user, "GET", nil)

	if err == nil {
		var newgroups []interface{}
		groups := res.(map[string]interface{})["groups"].([]interface{})

		for i, g := range groups {
			if g == group {
				newgroups = append(groups[:i], groups[i+1:]...)
				break
			}
		}

		if newgroups != nil {
			var data []byte

			data, err = json.Marshal(map[string]interface{}{
				"group_list": newgroups,
			})

			if err == nil {
				_, err = capi.Req(ac.EndpointUser+"u/"+user, "PUT", data)

				if err == nil {
					fmt.Fprintln(capi.Out(), fmt.Sprintf("User %s has left group %s", user, group))
				}
			}
		} else {
			err = fmt.Errorf("User %s is not in group %s", user, group)
		}
	}

	return err
}

// Command: userdel
// ================

/*
CommandUserdel is a command name.
*/
const CommandUserdel = "userdel"

/*
CmdUserdel deletes a user.
*/
type CmdUserdel struct {
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdUserdel) Name() string {
	return CommandUserdel
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdUserdel) ShortDescription() string {
	return "Removes a user from the system."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdUserdel) LongDescription() string {
	return "Removes a user from the system."
}

/*
Run executes the command.
*/
func (c *CmdUserdel) Run(args []string, capi CommandConsoleAPI) error {

	if len(args) < 1 {
		return fmt.Errorf("Please specify a username")
	}

	user := args[0]

	_, err := capi.Req(ac.EndpointUser+"u/"+user, "DELETE", nil)

	if err == nil {
		fmt.Fprintln(capi.Out(), fmt.Sprintf("User %s was deleted", user))
	}

	return err
}

// Command: groupadd
// =================

/*
CommandGroupadd is a command name.
*/
const CommandGroupadd = "groupadd"

/*
CmdGroupadd adds a new group.
*/
type CmdGroupadd struct {
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdGroupadd) Name() string {
	return CommandGroupadd
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdGroupadd) ShortDescription() string {
	return "Adds a group to the system."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdGroupadd) LongDescription() string {
	return "Adds a group to the system."
}

/*
Run executes the command.
*/
func (c *CmdGroupadd) Run(args []string, capi CommandConsoleAPI) error {

	if len(args) < 1 {
		return fmt.Errorf("Please specify a groupname")
	}

	group := args[0]

	_, err := capi.Req(ac.EndpointUser+"g/"+group, "POST", nil)

	if err == nil {
		fmt.Fprintln(capi.Out(), fmt.Sprintf("Group %s was created", group))
	}

	return err
}

// Command: groupadd
// =================

/*
CommandGroupdel is a command name.
*/
const CommandGroupdel = "groupdel"

/*
CmdGroupdel deletes a group.
*/
type CmdGroupdel struct {
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdGroupdel) Name() string {
	return CommandGroupdel
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdGroupdel) ShortDescription() string {
	return "Removes a group from the system."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdGroupdel) LongDescription() string {
	return "Removes a group from the system."
}

/*
Run executes the command.
*/
func (c *CmdGroupdel) Run(args []string, capi CommandConsoleAPI) error {

	if len(args) < 1 {
		return fmt.Errorf("Please specify a groupname")
	}

	group := args[0]

	_, err := capi.Req(ac.EndpointUser+"g/"+group, "DELETE", nil)

	if err == nil {
		fmt.Fprintln(capi.Out(), fmt.Sprintf("Group %s was deleted", group))
	}

	return err
}

// Command: grantperm
// ==================

/*
CommandGrantperm is a command name.
*/
const CommandGrantperm = "grantperm"

/*
CmdGrantperm grants a new permission to a group.
*/
type CmdGrantperm struct {
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdGrantperm) Name() string {
	return CommandGrantperm
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdGrantperm) ShortDescription() string {
	return "Grants a new permission to a group."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdGrantperm) LongDescription() string {
	return "Grants a new permission to a group. Specify first the permission " +
		"in CRUD format (Create, Read, Update or Delete), then a resource path and " +
		"then a group name."
}

/*
Run executes the command.
*/
func (c *CmdGrantperm) Run(args []string, capi CommandConsoleAPI) error {

	if len(args) < 3 {
		return fmt.Errorf("Please specify a permission, a resource path and a groupname")
	}

	perm := args[0]
	path := args[1]
	group := args[2]

	res, err := capi.Req(ac.EndpointUser+"g/"+group, "GET", nil)

	if err == nil {
		var data []byte

		perms := res.(map[string]interface{})

		// Merge in new permission

		perms[path] = perm

		if data, err = json.Marshal(perms); err == nil {

			if _, err = capi.Req(ac.EndpointUser+"g/"+group, "PUT", data); err == nil {
				fmt.Fprintln(capi.Out(), fmt.Sprintf("Permission %s on %s was granted to %s", perm, path, group))
			}
		}
	}

	return err
}

// Command: revokeperm
// ===================

/*
CommandRevokeperm is a command name.
*/
const CommandRevokeperm = "revokeperm"

/*
CmdRevokeperm revokes permissions to a resource for a group.
*/
type CmdRevokeperm struct {
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdRevokeperm) Name() string {
	return CommandRevokeperm
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdRevokeperm) ShortDescription() string {
	return "Revokes permissions to a resource for a group."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdRevokeperm) LongDescription() string {
	return "Revokes permissions to a resource for a group."
}

/*
Run executes the command.
*/
func (c *CmdRevokeperm) Run(args []string, capi CommandConsoleAPI) error {

	if len(args) < 2 {
		return fmt.Errorf("Please specify a resource path and a groupname")
	}

	path := args[0]
	group := args[1]

	res, err := capi.Req(ac.EndpointUser+"g/"+group, "GET", nil)

	if err == nil {
		var data []byte

		perms := res.(map[string]interface{})

		// Merge in new permission

		delete(perms, path)

		if data, err = json.Marshal(perms); err == nil {

			if _, err = capi.Req(ac.EndpointUser+"g/"+group, "PUT", data); err == nil {
				fmt.Fprintln(capi.Out(), fmt.Sprintf("All permissions on %s were revoked for %s", path, group))
			}
		}
	}

	return err
}
