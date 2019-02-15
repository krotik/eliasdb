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
	"fmt"
	"net/url"
	"sort"
	"strings"

	"devt.de/common/stringutil"
	"devt.de/eliasdb/api/v1"
)

// Command: info
// =============

/*
CommandInfo is a command name.
*/
const CommandInfo = "info"

/*
CmdInfo returns general database information.
*/
type CmdInfo struct {
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdInfo) Name() string {
	return CommandInfo
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdInfo) ShortDescription() string {
	return "Returns general database information."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdInfo) LongDescription() string {
	return "Returns general database information such as known node kinds, known attributes, etc ..."
}

/*
Run executes the command.
*/
func (c *CmdInfo) Run(args []string, capi CommandConsoleAPI) error {

	res, err := capi.Req(v1.EndpointInfoQuery, "GET", nil)

	if err == nil {
		var data = res.(map[string]interface{})
		var keys, tab []string

		tab = append(tab, "Kind")
		tab = append(tab, "Count")

		counts := data["node_counts"].(map[string]interface{})

		for k := range counts {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		for _, k := range keys {
			c := counts[k]
			tab = append(tab, k)
			tab = append(tab, fmt.Sprintf("%10v", c))
		}

		capi.ExportBuffer().WriteString(stringutil.PrintCSVTable(tab, 2))

		fmt.Fprint(capi.Out(), stringutil.PrintGraphicStringTable(tab, 2, 1,
			stringutil.SingleLineTable))
	}

	return err
}

// Command: part
// =============

/*
CommandPart is a command name.
*/
const CommandPart = "part"

/*
CmdPart displays or sets the current partition.
*/
type CmdPart struct {
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdPart) Name() string {
	return CommandPart
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdPart) ShortDescription() string {
	return "Displays or sets the current partition."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdPart) LongDescription() string {
	return "Displays or sets the current partition."
}

/*
Run executes the command.
*/
func (c *CmdPart) Run(args []string, capi CommandConsoleAPI) error {

	if len(args) == 0 {
		fmt.Fprintln(capi.Out(), capi.Partition())
	} else {
		capi.SetPartition(args[0])
		fmt.Fprintln(capi.Out(),
			fmt.Sprintf("Current partition is: %s", args[0]))
	}

	return nil
}

// Command: find
// =============

/*
CommandFind is a command name.
*/
const CommandFind = "find"

/*
CmdFind does a full-text search of the database.
*/
type CmdFind struct {
}

/*
Name returns the command name (as it should be typed)
*/
func (c *CmdFind) Name() string {
	return CommandFind
}

/*
ShortDescription returns a short description of the command (single line)
*/
func (c *CmdFind) ShortDescription() string {
	return "Do a full-text search of the database."
}

/*
LongDescription returns an extensive description of the command (can be multiple lines)
*/
func (c *CmdFind) LongDescription() string {
	return "Do a full-text search of the database."
}

/*
Run executes the command.
*/
func (c *CmdFind) Run(args []string, capi CommandConsoleAPI) error {

	if len(args) < 1 {
		return fmt.Errorf("Please specify a search phrase")
	}

	phrase := url.QueryEscape(strings.Join(args, " "))

	res, err := capi.Req(fmt.Sprintf("%s?lookup=1&text=%s", v1.EndpointFindQuery, phrase), "GET", nil)

	if err == nil {
		partitions := res.(map[string]interface{})

		for _, p := range stringutil.MapKeys(partitions) {
			kinds := partitions[p].(map[string]interface{})

			for _, k := range stringutil.MapKeys(kinds) {
				nodes := kinds[k].([]interface{})

				// Construct table header

				header := []string{"Partition", p, "Kind", k}

				capi.ExportBuffer().WriteString(stringutil.PrintCSVTable(header, 2))

				fmt.Fprint(capi.Out(), stringutil.PrintStringTable(header, 2))

				// Construct table

				node := nodes[0].(map[string]interface{})
				attrs := stringutil.MapKeys(node)

				var tab []string

				tab = append(tab, attrs...)
				for _, n := range nodes {
					node := n.(map[string]interface{})

					for _, attr := range attrs {
						tab = append(tab, fmt.Sprint(node[attr]))
					}
				}

				capi.ExportBuffer().WriteString(stringutil.PrintCSVTable(tab, len(attrs)))

				fmt.Fprint(capi.Out(), stringutil.PrintGraphicStringTable(tab, len(attrs), 1,
					stringutil.SingleLineTable))

				fmt.Fprintln(capi.Out(), "")
			}
		}
	}

	return nil
}
