/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package eql

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/krotik/eliasdb/graph"
	"github.com/krotik/eliasdb/graph/graphstorage"
)

/*
runTestQuery runs a query against a given graph manager and checks against an expected result.
*/
func runTestQuery(gm *graph.Manager, query string, expectedResult string) error {

	sr, err := RunQuery("", "main", query, gm)

	if err != nil {
		return err
	}
	sr.(*queryResult).SearchResult.StableSort()

	if sr.String() != expectedResult {
		return fmt.Errorf("Unexpected result: %s", sr)
	}

	return nil
}

/*
Issue 43 Query not returning expected result
(https://github.com/krotik/eliasdb/issues/43)

I have a graph where 4 nodes (T1 through T4) are connected like this:

[T1] <-(E2)- [T2] <-(E3)- [T3] -(E4)-> [T4]

The goal of my query is:

    Given a T1, find the T4 items.

The kind for T1, T2 and T3 are known but the type for T4 is not. For some reason the follwing query does not work (traversing from the left to right: T1 to T2 to T3 to T4).

get T1 where key = "%s"
    traverse in:E2:out:T2
        traverse in:E3:out:T3
            traverse out:E4:in:
            end
        end
    end
*/

var issue43data = `
{
    "nodes" : [
        {
			"key"       : "t1",
			"kind"      : "T1",
			"name"      : "T1 node"
        },
        {
			"key"       : "t2",
			"kind"      : "T2",
			"name"      : "T2 node"
        },
        {
			"key"       : "t3",
			"kind"      : "T3",
			"name"      : "T3 node"
        },
        {
			"key"       : "t4",
			"kind"      : "T4",
			"name"      : "T4 node"
        }
    ],
    "edges" : [
        {
			"key"       : "e2",
			"kind"      : "E2",
			"end1key"   : "t1",
			"end1cascading": false,
			"end1kind": "T1",
			"end1role": "in",
			"end2key"   : "t2",
			"end2cascading": false,
			"end2kind": "T2",
			"end2role": "out"
        },
        {
			"key"       : "e3",
			"kind"      : "E3",
			"end1key"   : "t2",
			"end1cascading": false,
			"end1kind": "T2",
			"end1role": "in",
			"end2cascading": false,
			"end2key"   : "t3",
			"end2cascading": false,
			"end2kind": "T3",
			"end2role": "out"
        },
        {
			"key"       : "e4",
			"kind"      : "E4",
			"end1key"   : "t3",
			"end1cascading": false,
			"end1kind": "T3",
			"end1role": "out",
			"end2key"   : "t4",
			"end2cascading": false,
			"end2kind": "T4",
			"end2role": "in"
        }
    ]
}
`

func TestIssue43Regression(t *testing.T) {
	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	err := graph.ImportPartition(bytes.NewBufferString(issue43data), "main", gm)
	if err != nil {
		t.Error(err)
		return
	}

	if err := runTestQuery(gm, `
get T1 where key = "t1"
    traverse in:E2:out:T2
        traverse in:E3:out:T3
            traverse out:E4:in:
            end
        end
    end
`, `
Labels: T1 Key, T1 Name, T2 Key, T2 Name, T3 Key, T3 Name, Key, Kind, Name
Format: auto, auto, auto, auto, auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:name, 2:n:key, 2:n:name, 3:n:key, 3:n:name, 4:n:key, 4:n:kind, 4:n:name
t1, T1 node, t2, T2 node, t3, T3 node, t4, T4, T4 node
`[1:]); err != nil {
		t.Error(err)
		return
	}
}
