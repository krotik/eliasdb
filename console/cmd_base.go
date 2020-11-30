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
	"bytes"
	"fmt"

	"devt.de/krotik/common/stringutil"
	"devt.de/krotik/eliasdb/api"
	"devt.de/krotik/eliasdb/api/ac"
)

// Command: ver
// ============

/*
CommandVer is a command name.
*/
const CommandVer = "ver"

/*
CmdVer displays descriptions of other commands.
*/
type CmdVer struct {
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdVer) Name() string {
	return CommandVer
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdVer) ShortDescription() string {
	return "Displays server version information."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdVer) LongDescription() string {
	return "Displays server version information."
}

/*
Run executes the command.
*/
func (c *CmdVer) Run(args []string, capi CommandConsoleAPI) error {

	fmt.Fprintln(capi.Out(), fmt.Sprintf("Connected to: %v", capi.URL()))

	res, err := capi.Req(api.EndpointAbout, "GET", nil)

	if err == nil {
		data := res.(map[string]interface{})

		fmt.Fprintln(capi.Out(), fmt.Sprintf("%v %v (REST versions: %v)",
			data["product"], data["version"], data["api_versions"]))
	}

	return err
}

// Command: whoami
// ===============

/*
CommandWhoAmI is a command name.
*/
const CommandWhoAmI = "whoami"

/*
CmdWhoAmI returns the current login status.
*/
type CmdWhoAmI struct {
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdWhoAmI) Name() string {
	return CommandWhoAmI
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdWhoAmI) ShortDescription() string {
	return "Returns the current login status."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdWhoAmI) LongDescription() string {
	return "Returns the current login status."
}

/*
Run executes the command.
*/
func (c *CmdWhoAmI) Run(args []string, capi CommandConsoleAPI) error {

	res, err := capi.Req(ac.EndpointWhoAmI, "GET", nil)

	if err == nil {
		var out string

		o := res.(map[string]interface{})

		if o["logged_in"].(bool) {
			out = fmt.Sprintf("%s", o["username"])
		} else {
			out = "Nobody - not logged in"
		}

		fmt.Fprintln(capi.Out(), out)
	}

	return err
}

// Command: export
// ===============

/*
CommandExport is a command name.
*/
const CommandExport = "export"

/*
CmdExport exports the data which is currently in the export buffer.
*/
type CmdExport struct {
	exportFunc func([]string, *bytes.Buffer) error
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdExport) Name() string {
	return CommandExport
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdExport) ShortDescription() string {
	return "Exports the last output."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdExport) LongDescription() string {
	return "Exports the data which is currently in the export buffer. The export " +
		"buffer is filled with the previous command output in a machine readable form."
}

/*
Run executes the command.
*/
func (c *CmdExport) Run(args []string, capi CommandConsoleAPI) error {
	return c.exportFunc(args, capi.ExportBuffer())
}

// Command: login
// ==============

/*
CommandLogin is a command name.
*/
const CommandLogin = "login"

/*
CmdLogin placeholder for the login command.
*/
type CmdLogin struct {
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdLogin) Name() string {
	return CommandLogin
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdLogin) ShortDescription() string {
	return "Log in as a user."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdLogin) LongDescription() string {
	return "Log in as a user."
}

/*
Run executes the command.
*/
func (c *CmdLogin) Run(args []string, capi CommandConsoleAPI) error {
	return nil // Functionality is implemented in the command processor
}

// Command: logout
// ===============

/*
CommandLogout is a command name.
*/
const CommandLogout = "logout"

/*
CmdLogout placeholder for the logout command.
*/
type CmdLogout struct {
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdLogout) Name() string {
	return CommandLogout
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdLogout) ShortDescription() string {
	return "Log out the current user."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdLogout) LongDescription() string {
	return "Log out the current user."
}

/*
Run executes the command.
*/
func (c *CmdLogout) Run(args []string, capi CommandConsoleAPI) error {
	return nil // Functionality is implemented in the command processor
}

// Command: help
// =============

/*
CommandHelp is a command name.
*/
const CommandHelp = "help"

/*
CmdHelp displays descriptions of other commands.
*/
type CmdHelp struct {
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdHelp) Name() string {
	return CommandHelp
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdHelp) ShortDescription() string {
	return "Display descriptions for all available commands."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdHelp) LongDescription() string {
	return "Display descriptions for all available commands."
}

/*
Run executes the command.
*/
func (c *CmdHelp) Run(args []string, capi CommandConsoleAPI) error {

	cmds := capi.Commands()

	if len(args) > 0 {
		name := args[0]

		for _, cmd := range cmds {
			if cmd.Name() == name {
				capi.ExportBuffer().WriteString(cmd.LongDescription())
				fmt.Fprintln(capi.Out(), cmd.LongDescription())
				return nil
			}
		}

		return fmt.Errorf("Unknown command: %s", name)
	}

	var tab []string

	tab = append(tab, "Command")
	tab = append(tab, "Description")

	for _, cmd := range cmds {
		tab = append(tab, cmd.Name())
		tab = append(tab, cmd.ShortDescription())
	}

	capi.ExportBuffer().WriteString(stringutil.PrintCSVTable(tab, 2))

	fmt.Fprint(capi.Out(), stringutil.PrintStringTable(tab, 2))

	return nil
}
