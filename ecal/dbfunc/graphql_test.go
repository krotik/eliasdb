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

func TestGraphQL(t *testing.T) {
	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	gm.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "a",
		"kind": "b",
		"foo":  "bar1",
	}))
	gm.StoreNode("main", data.NewGraphNodeFromMap(map[string]interface{}{
		"key":  "c",
		"kind": "b",
		"foo":  "bar2",
	}))

	q := &GraphQLFunc{gm}

	if _, err := q.DocString(); err != nil {
		t.Error(err)
		return
	}

	if _, err := q.Run("", nil, nil, 0, []interface{}{""}); err == nil ||
		err.Error() != "Function requires at least 2 parameters: partition and query with optionally a map of variables and an operation name" {
		t.Error(err)
		return
	}

	if _, err := q.Run("", nil, nil, 0, []interface{}{"", "", ""}); err == nil ||
		err.Error() != "Third parameter must be a map" {
		t.Error(err)
		return
	}

	res, err := q.Run("", nil, nil, 0, []interface{}{"main",
		`query foo($x: string) { b(key:$x) { foo }}`, map[interface{}]interface{}{
			"x": "c",
		}, "foo"})

	if err != nil {
		t.Error(err)
		return
	}

	if fmt.Sprint(res) != "map[data:map[b:[map[foo:bar2]]]]" {
		t.Error("Unexpected result:", res)
		return
	}

	_, err = q.Run("", nil, nil, 0, []interface{}{"main", "aaaaa"})

	if err == nil || err.Error() != "Fatal GraphQL operation error in db.query: Missing operation (No executable expression found) (Line:1 Pos:1)" {
		t.Error(err)
		return
	}
}
