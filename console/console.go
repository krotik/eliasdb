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
Package console contains the console command processor for EliasDB.
*/
package console

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"

	"devt.de/krotik/common/errorutil"
	"devt.de/krotik/eliasdb/api/ac"
	"devt.de/krotik/eliasdb/config"
)

/*
NewConsole creates a new Console object which can parse and execute given
commands from the given Reader and outputs the result to the Writer. It
optionally exports data with the given export function via the save command.
Export is disabled if no export function is defined.
*/
func NewConsole(url string, out io.Writer, getCredentials func() (string, string),
	getPassword func() string, exportFunc func([]string, *bytes.Buffer) error) CommandConsole {

	cmdMap := make(map[string]Command)

	cmdMap[CommandHelp] = &CmdHelp{}
	cmdMap[CommandVer] = &CmdVer{}

	// Adding commands specific to access control

	if config.Bool(config.EnableAccessControl) {
		cmdMap[CommandLogin] = &CmdLogin{}
		cmdMap[CommandLogout] = &CmdLogout{}
		cmdMap[CommandWhoAmI] = &CmdWhoAmI{}
		cmdMap[CommandUsers] = &CmdUsers{}
		cmdMap[CommandGroups] = &CmdGroups{}
		cmdMap[CommandUseradd] = &CmdUseradd{}
		cmdMap[CommandGroupadd] = &CmdGroupadd{}
		cmdMap[CommandUserdel] = &CmdUserdel{}
		cmdMap[CommandGroupdel] = &CmdGroupdel{}
		cmdMap[CommandNewpass] = &CmdNewpass{}
		cmdMap[CommandJoingroup] = &CmdJoingroup{}
		cmdMap[CommandLeavegroup] = &CmdLeavegroup{}
		cmdMap[CommandGrantperm] = &CmdGrantperm{}
		cmdMap[CommandRevokeperm] = &CmdRevokeperm{}
	}

	cmdMap[CommandInfo] = &CmdInfo{}
	cmdMap[CommandPart] = &CmdPart{}
	cmdMap[CommandFind] = &CmdFind{}

	// Add export if we got an export function

	if exportFunc != nil {
		cmdMap[CommandExport] = &CmdExport{exportFunc}
	}

	c := &EliasDBConsole{url, "main", out, bytes.NewBuffer(nil), nil,
		nil, false, cmdMap, getCredentials, getPassword}

	c.childConsoles = []CommandConsole{&EQLConsole{c}, &GraphQLConsole{c}}

	return c
}

/*
CommandConsole is the main interface for command processors.
*/
type CommandConsole interface {

	/*
		Run executes one or more commands. It returns an error if the command
		had an unexpected result and a flag if the command was handled.
	*/
	Run(cmd string) (bool, error)

	/*
	   Commands returns a sorted list of all available commands.
	*/
	Commands() []Command
}

/*
CommandConsoleAPI is the console interface which commands can use to send communicate to the server.
*/
type CommandConsoleAPI interface {
	CommandConsole

	/*
	   Authenticate authenticates the user if necessary.
	*/
	Authenticate(force bool)

	/*
	   Url returns the current connection URL.
	*/
	Url() string

	/*
	   Partition returns the current partition.
	*/
	Partition() string

	/*
	   Sets the current partition.
	*/
	SetPartition(string)

	/*
	   AskPassword asks the user for a password.
	*/
	AskPassword() string

	/*
	   Req is a convenience function to send common requests.
	*/
	Req(endpoint string, method string, content []byte) (interface{}, error)

	/*
	   SendRequest sends a request to the connected server. The calling code of the
	   function can specify the contentType (e.g. application/json), the method
	   (e.g. GET), the content (for POST, PUT and DELETE requests) and a request
	   modifier function which can be used to modify the request object before the
	   request to the server is being made.
	*/
	SendRequest(endpoint string, contentType string, method string,
		content []byte, reqMod func(*http.Request)) (string, *http.Response, error)

	/*
		Out returns a writer which can be used to write to the console.
	*/
	Out() io.Writer

	/*
	   ExportBuffer returns a buffer which can be used to write exportable data.
	*/
	ExportBuffer() *bytes.Buffer
}

/*
CommError is a communication error from the ConsoleAPI.
*/
type CommError struct {
	err  error          // Nice error message
	Resp *http.Response // Error response from the REST API
}

/*
Error returns a textual representation of this error.
*/
func (c *CommError) Error() string {
	return c.err.Error()
}

/*
Command describes an available command.
*/
type Command interface {
	/*
	   Name returns the command name (as it should be typed).
	*/
	Name() string

	/*
	   ShortDescription returns a short description of the command (single line).
	*/
	ShortDescription() string

	/*
	   LongDescription returns an extensive description of the command (can be multiple lines).
	*/
	LongDescription() string

	/*
		Run executes the command.
	*/
	Run(args []string, capi CommandConsoleAPI) error
}

// EliasDB Console
// ===============

/*
EliasDBConsole implements the basic console functionality like login and version.
*/
type EliasDBConsole struct {
	url string // Current server url (e.g. http://localhost:9090)

	part          string           // Current partition
	out           io.Writer        // Output for this console
	export        *bytes.Buffer    // Export buffer
	childConsoles []CommandConsole // List of child consoles

	authCookie *http.Cookie // User token
	credsAsked bool         // Flag if the credentials have been asked

	CommandMap     map[string]Command      // Map of registered commands
	GetCredentials func() (string, string) // Ask the user for credentials
	GetPassword    func() string           // Ask the user for a password
}

/*
Url returns the current connected server URL.
*/
func (c *EliasDBConsole) Url() string {
	return c.url
}

/*
Out returns a writer which can be used to write to the console.
*/
func (c *EliasDBConsole) Out() io.Writer {
	return c.out
}

/*
Partition returns the current partition.
*/
func (c *EliasDBConsole) Partition() string {
	return c.part
}

/*
SetPartition sets the current partition.
*/
func (c *EliasDBConsole) SetPartition(part string) {
	c.part = part
}

/*
AskPassword asks the user for a password.
*/
func (c *EliasDBConsole) AskPassword() string {
	return c.GetPassword()
}

/*
ExportBuffer returns a buffer which can be used to write exportable data.
*/
func (c *EliasDBConsole) ExportBuffer() *bytes.Buffer {
	return c.export
}

/*
Run executes one or more commands. It returns an error if the command
had an unexpected result and a flag if the command was handled.
*/
func (c *EliasDBConsole) Run(cmd string) (bool, error) {

	// First split a line with multiple commands

	cmds := strings.Split(cmd, ";")

	for _, cmd := range cmds {

		// Run the command and return if there is an error

		if ok, err := c.RunCommand(cmd); err != nil {

			// Return if there was an unexpected error

			return false, err

		} else if !ok {

			// Try child consoles

			for _, c := range c.childConsoles {

				if ok, err := c.Run(cmd); err != nil || ok {
					return ok, err
				}
			}

			return false, fmt.Errorf("Unknown command")
		}
	}

	// Everything was handled

	return true, nil
}

/*
RunCommand executes a single command. It returns an error for unexpected results and
a flag if the command was handled.
*/
func (c *EliasDBConsole) RunCommand(cmdString string) (bool, error) {
	cmdSplit := strings.Fields(cmdString)

	if len(cmdSplit) > 0 {
		cmd := cmdSplit[0]
		args := cmdSplit[1:]

		// Reset the export buffer if we are not exporting

		if cmd != CommandExport {
			c.export.Reset()
		}

		if config.Bool(config.EnableAccessControl) {

			// Extra commands when access control is enabled

			if cmd == "logout" {

				// Special command "logout" to remove the current auth token

				c.authCookie = nil

				fmt.Fprintln(c.out, "Current user logged out.")

			} else if cmd != "ver" && cmd != "whoami" && cmd != "help" &&
				cmd != "?" && cmd != "export" {

				// Do not authenticate if running local commands

				// Authenticate user this is a NOP if the user is authenticated unless
				// the command "login" is given. Then the user is reauthenticated.

				c.Authenticate(cmd == "login")
			}
		}

		if cmdObj, ok := c.CommandMap[cmd]; ok {
			return true, cmdObj.Run(args, c)
		} else if cmd == "?" {
			return true, c.CommandMap["help"].Run(args, c)
		}
	}

	return false, nil
}

/*
Commands returns a sorted list of all available commands.
*/
func (c *EliasDBConsole) Commands() []Command {
	var res []Command

	for _, c := range c.CommandMap {
		res = append(res, c)
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i].Name() < res[j].Name()
	})

	return res
}

/*
Authenticate authenticates the user if necessary.
*/
func (c *EliasDBConsole) Authenticate(force bool) {

	// Only do the authentication if we haven't asked yet or it is
	// explicitly desired

	if !c.credsAsked || force {

		c.credsAsked = false
		for !c.credsAsked {

			// Ask for credentials

			user, pass := c.GetCredentials()

			if user == "" {

				// User doesn't want to authenticate - do nothing

				fmt.Fprintln(c.out, "Skipping authentication")
				c.credsAsked = true

				return
			}

			content, err := json.Marshal(map[string]interface{}{
				"user": user,
				"pass": pass,
			})

			errorutil.AssertOk(err) // Json marshall should never fail

			res, resp, err := c.SendRequest(ac.EndpointLogin, "application/json", "POST", content, nil)

			if err == nil {
				if resp.StatusCode == http.StatusOK && len(resp.Cookies()) > 0 {
					fmt.Fprintln(c.out, "Login as user", user)
					c.authCookie = resp.Cookies()[0]
					c.credsAsked = true
					return
				}
			}

			fmt.Fprintln(c.out, fmt.Sprintf("Login failed for user %s: %s (error=%v)", user, res, err))
		}
	}
}

/*
Req is a convenience function to send common requests.
*/
func (c *EliasDBConsole) Req(endpoint string, method string, content []byte) (interface{}, error) {
	var res interface{}

	bodyStr, resp, err := c.SendRequest(endpoint, "application/json", method, content,
		func(r *http.Request) {})

	if err == nil {

		// Try json decoding

		if jerr := json.Unmarshal([]byte(bodyStr), &res); jerr != nil {
			res = bodyStr

			// Check if we got an error back

			if resp.StatusCode != http.StatusOK {
				return nil, &CommError{
					fmt.Errorf("%s request to %s failed: %s", method, endpoint, bodyStr),
					resp,
				}
			}
		}
	}

	return res, err
}

/*
SendRequest sends a request to the connected server. The calling code of the
function can specify the contentType (e.g. application/json), the method
(e.g. GET), the content (for POST, PUT and DELETE requests) and a request
modifier function which can be used to modify the request object before the
request to the server is being made.
*/
func (c *EliasDBConsole) SendRequest(endpoint string, contentType string, method string,
	content []byte, reqMod func(*http.Request)) (string, *http.Response, error) {

	var bodyStr string
	var req *http.Request
	var resp *http.Response
	var err error

	if content != nil {
		req, err = http.NewRequest(method, c.url+endpoint, bytes.NewBuffer(content))
	} else {
		req, err = http.NewRequest(method, c.url+endpoint, nil)
	}

	if err == nil {

		req.Header.Set("Content-Type", contentType)

		// Set auth cookie

		if c.authCookie != nil {
			req.AddCookie(c.authCookie)
		}

		if reqMod != nil {
			reqMod(req)
		}

		// Console client does not verify the SSL keys

		tlsConfig := &tls.Config{
			InsecureSkipVerify: true,
		}
		transport := &http.Transport{TLSClientConfig: tlsConfig}

		client := &http.Client{
			Transport: transport,
		}

		resp, err = client.Do(req)

		if err == nil {
			defer resp.Body.Close()

			body, _ := ioutil.ReadAll(resp.Body)
			bodyStr = strings.Trim(string(body), " \n")
		}
	}

	// Just return the body

	return bodyStr, resp, err
}

// Util functions
// ==============

/*
cmdStartsWithKeyword checks if a given command line starts with a given list
of keywords.
*/
func cmdStartsWithKeyword(cmd string, keywords []string) bool {
	ss := strings.Fields(strings.ToLower(cmd))

	if len(ss) > 0 {
		firstCmd := strings.ToLower(ss[0])

		for _, k := range keywords {
			if k == firstCmd || strings.HasPrefix(firstCmd, k) {
				return true
			}
		}
	}

	return false
}
