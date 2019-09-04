package eql

import (
	"testing"
)

func TestRefineQuery(t *testing.T) {
	gm, _ := songGraph()

	res, _ := RunQuery("test", "main", "get Author with ordering(ascending key)", gm)
	if res.String() != `
Labels: Author Key, Author Name
Format: auto, auto
Data: 1:n:key, 1:n:name
000, John
123, Mike
456, Hans
`[1:] {
		t.Error("Unexpected result: ", res)
		return
	}

	refres, err := RefineQueryByResultRows(res, []int{0, 2})
	if err != nil || refres != `lookup Author "000", "456" 
with
  ordering(ascending key)` {
		t.Error("Unexpected result: ", refres, err)
		return
	}

	res, _ = RunQuery("test", "main", "lookup Author '000', '123', '456' with ordering(ascending key)", gm)
	if res.String() != `
Labels: Author Key, Author Name
Format: auto, auto
Data: 1:n:key, 1:n:name
000, John
123, Mike
456, Hans
`[1:] {
		t.Error("Unexpected result: ", res)
		return
	}

	refres, err = RefineQueryByResultRows(res, []int{0, 2})
	if err != nil || refres != `lookup Author "000", "456" 
with
  ordering(ascending key)` {
		t.Error("Unexpected result: ", refres, err)
		return
	}

	res, _ = RunQuery("test", "main", "get Author", gm)

	refres, err = RefineQueryByResultRows(res, []int{0, 1, 2, 3})
	if err != nil || refres != `lookup Author "000", "123", "456"` {
		t.Error("Unexpected result: ", refres, err)
		return
	}

	refres, err = RefineQueryByResultRows(res, []int{3})
	if err == nil || err.Error() != "Could not find requested row" {
		t.Error("Unexpected result: ", refres, err)
		return
	}
}
