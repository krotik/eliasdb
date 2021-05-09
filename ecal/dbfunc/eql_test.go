/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dbfunc

import (
	"fmt"
	"testing"

	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/graph/graphstorage"
)

func TestQuery(t *testing.T) {
	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	gm.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "a",
		"kind": "b",
	}))
	gm.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "c",
		"kind": "d",
	}))

	q := &QueryFunc{gm}

	if _, err := q.DocString(); err != nil {
		t.Error(err)
		return
	}

	if _, err := q.Run("", nil, nil, 0, []interface{}{""}); err == nil ||
		err.Error() != "Function requires 2 parameters: partition and a query string" {
		t.Error(err)
		return
	}

	res, err := q.Run("", nil, nil, 0, []interface{}{"main", "get b"})

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

	_, err = q.Run("", nil, nil, 0, []interface{}{"main", "got b"})

	if err == nil || err.Error() != "EQL error in db.query: Invalid construct (Unknown query type: got) (Line:1 Pos:1)" {
		t.Error(err)
		return
	}
}
