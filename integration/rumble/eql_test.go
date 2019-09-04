/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package rumble

import (
	"fmt"
	"testing"

	"devt.de/krotik/eliasdb/api"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/graph/graphstorage"
)

func TestQuery(t *testing.T) {

	mr := &mockRuntime{}
	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	api.GM = gm

	gm.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "a",
		"kind": "b",
	}))
	gm.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "c",
		"kind": "d",
	}))

	q := &QueryFunc{}

	if q.Name() != "db.query" {
		t.Error("Unexpected result:", q.Name())
		return
	}

	if err := q.Validate(2, mr); err != nil {
		t.Error(err)
		return
	}

	if err := q.Validate(1, mr); err == nil || err.Error() != "Invalid construct Function query requires 2 parameters: partition and a query string" {
		t.Error(err)
		return
	}

	res, err := q.Execute([]interface{}{"main", "get b"}, nil, mr)

	if err != nil {
		t.Error(err)
		return
	}

	if res := res.(map[interface{}]interface{})["rows"]; fmt.Sprint(res) != "[[a]]" {
		t.Error("Unexpected result:", res)
		return
	}

	if res := res.(map[interface{}]interface{})["cols"]; fmt.Sprint(res) != "[B Key]" {
		t.Error("Unexpected result:", res)
		return
	}

	_, err = q.Execute([]interface{}{"main", "got b"}, nil, mr)

	if err == nil || err.Error() != "Invalid state EQL error in db.query: Invalid construct (Unknown query type: got) (Line:1 Pos:1)" {
		t.Error(err)
		return
	}

}
