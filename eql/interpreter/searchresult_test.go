/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package interpreter

import (
	"errors"
	"fmt"
	"testing"

	"devt.de/krotik/eliasdb/eql/parser"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/graph/graphstorage"
	"devt.de/krotik/eliasdb/storage"
)

func TestSources(t *testing.T) {
	gm, _ := filterGraph()

	rt := NewGetRuntimeProvider("test", "main", gm, NewDefaultNodeInfo(gm))

	res, err := getResult("get filtertest with ordering(ascending key)", `
Labels: Filtertest Key, Val1, Val2, Val3
Format: auto, auto, auto, auto
Data: 1:n:key, 1:n:val1, 1:n:val2, 1:n:val3
1, test, Hans, foo
2, test1, Hans, foo
3, test2, Hans, foo
4, test3, Peter, foo
5, test4, Peter, foo
6, test5, Peter, foo
7, test6, Anna, foo
8, test7, Anna, foo
9, test8, Steve, foo
10, test9, Steve, foo
11, test10, Franz, foo
12, test11, Kevin, foo
13, test12, Kevin, foo
14, test13, Kevin, foo
15, test14, X1, foo
16, test15, X2, foo
17, test16, X3, foo
18, test17, X4, foo
19, test17, X5, foo
`[1:], rt, false)

	if err != nil {
		t.Error(err)
		return
	}

	if q := res.Query(); q != `
get filtertest 
with
  ordering(ascending key)`[1:] {
		t.Error("Unexpected result: ", q)
		return
	}

	if r := fmt.Sprint(res.Rows()); r != "[[1 test Hans foo] [2 test1 Hans foo] [3 test2 Hans foo] [4 test3 Peter foo] [5 test4 Peter foo] [6 test5 Peter foo] [7 test6 Anna foo] [8 test7 Anna foo] [9 test8 Steve foo] [10 test9 Steve foo] [11 test10 Franz foo] [12 test11 Kevin foo] [13 test12 Kevin foo] [14 test13 Kevin foo] [15 test14 X1 foo] [16 test15 X2 foo] [17 test16 X3 foo] [18 test17 X4 foo] [19 test17 X5 foo]]" {
		t.Error("Unexpected result: ", r)
	}

	if r := fmt.Sprint(res.RowSources()); r != "[[n:filtertest:1 n:filtertest:1 n:filtertest:1 n:filtertest:1] [n:filtertest:2 n:filtertest:2 n:filtertest:2 n:filtertest:2] [n:filtertest:3 n:filtertest:3 n:filtertest:3 n:filtertest:3] [n:filtertest:4 n:filtertest:4 n:filtertest:4 n:filtertest:4] [n:filtertest:5 n:filtertest:5 n:filtertest:5 n:filtertest:5] [n:filtertest:6 n:filtertest:6 n:filtertest:6 n:filtertest:6] [n:filtertest:7 n:filtertest:7 n:filtertest:7 n:filtertest:7] [n:filtertest:8 n:filtertest:8 n:filtertest:8 n:filtertest:8] [n:filtertest:9 n:filtertest:9 n:filtertest:9 n:filtertest:9] [n:filtertest:10 n:filtertest:10 n:filtertest:10 n:filtertest:10] [n:filtertest:11 n:filtertest:11 n:filtertest:11 n:filtertest:11] [n:filtertest:12 n:filtertest:12 n:filtertest:12 n:filtertest:12] [n:filtertest:13 n:filtertest:13 n:filtertest:13 n:filtertest:13] [n:filtertest:14 n:filtertest:14 n:filtertest:14 n:filtertest:14] [n:filtertest:15 n:filtertest:15 n:filtertest:15 n:filtertest:15] [n:filtertest:16 n:filtertest:16 n:filtertest:16 n:filtertest:16] [n:filtertest:17 n:filtertest:17 n:filtertest:17 n:filtertest:17] [n:filtertest:18 n:filtertest:18 n:filtertest:18 n:filtertest:18] [n:filtertest:19 n:filtertest:19 n:filtertest:19 n:filtertest:19]]" {
		t.Error("Unexpected result: ", r)
	}

	if res := res.CSV(); res != `
Filtertest Key,Val1,Val2,Val3
1,test,Hans,foo
2,test1,Hans,foo
3,test2,Hans,foo
4,test3,Peter,foo
5,test4,Peter,foo
6,test5,Peter,foo
7,test6,Anna,foo
8,test7,Anna,foo
9,test8,Steve,foo
10,test9,Steve,foo
11,test10,Franz,foo
12,test11,Kevin,foo
13,test12,Kevin,foo
14,test13,Kevin,foo
15,test14,X1,foo
16,test15,X2,foo
17,test16,X3,foo
18,test17,X4,foo
19,test17,X5,foo
`[1:] {
		t.Error("Unexpected result: ", res)
	}
}

func TestGrouping(t *testing.T) {
	gm, mgs := songGraphGroups()
	rt := NewGetRuntimeProvider("test", "main", gm, NewDefaultNodeInfo(gm))
	rt2 := NewLookupRuntimeProvider("test", "main", gm, NewDefaultNodeInfo(gm))

	res, err := getResult("get group where key = 'Best' traverse :::", `
Labels: Group Key, Key, Kind, Name
Format: auto, auto, auto, auto
Data: 1:n:key, 2:n:key, 2:n:kind, 2:n:name
Best, Aria3, Song, Aria3
Best, LoveSong3, Song, LoveSong3
Best, MyOnlySong3, Song, MyOnlySong3
Best, StrangeSong1, Song, StrangeSong1
`[1:], rt, true)

	if err != nil {
		t.Error(err)
		return
	}

	if r := res.Rows(); r == nil || fmt.Sprint(r) != "[[Best Aria3 Song Aria3] [Best LoveSong3 Song LoveSong3] [Best MyOnlySong3 Song MyOnlySong3] [Best StrangeSong1 Song StrangeSong1]]" {
		t.Error("Unexpected result: ", r)
		return
	}

	if r := res.RowSources(); r == nil || fmt.Sprint(r) != "[[n:group:Best n:Song:Aria3 n:Song:Aria3 n:Song:Aria3] [n:group:Best n:Song:LoveSong3 n:Song:LoveSong3 n:Song:LoveSong3] [n:group:Best n:Song:MyOnlySong3 n:Song:MyOnlySong3 n:Song:MyOnlySong3] [n:group:Best n:Song:StrangeSong1 n:Song:StrangeSong1 n:Song:StrangeSong1]]" {
		t.Error("Unexpected result: ", r)
		return
	}

	// Inspect result

	if res.Header() != &res.SearchHeader {
		t.Error("Unexpected result")
		return
	}

	if res.Header().Data()[0] != res.SearchHeader.ColData[0] {
		t.Error("Unexpected result")
		return
	}

	if res.Header().Format()[0] != res.SearchHeader.ColFormat[0] {
		t.Error("Unexpected result")
		return
	}

	if res.Header().Labels()[0] != res.SearchHeader.ColLabels[0] {
		t.Error("Unexpected result")
		return
	}

	if res.Header().PrimaryKind() != res.SearchHeader.ResPrimaryKind {
		t.Error("Unexpected result")
		return
	}

	if res.Header().Partition() != res.SearchHeader.ResPartition {
		t.Error("Unexpected result")
		return
	}

	if res.RowCount() != 4 {
		t.Error("Unexpected result")
		return
	}

	if res.Row(2)[1] != res.Data[2][1] {
		t.Error("Unexpected result")
		return
	}

	if res.RowSource(2)[1] != res.Source[2][1] {
		t.Error("Unexpected result")
		return
	}

	if _, err := getResult("get Song from group Best", `
Labels: Song Key, Song Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
Aria3, Aria3, 4
LoveSong3, LoveSong3, 1
MyOnlySong3, MyOnlySong3, 19
StrangeSong1, StrangeSong1, 5
`[1:], rt, true); err != nil {
		t.Error(err)
		return
	}

	if _, err := getResult("get Song from group bbest", `
Labels: Song Key, Song Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
`[1:], rt, true); err != nil {
		t.Error(err)
		return
	}

	if _, err := getResult("get Song from group Best1", `
Labels: Song Key, Song Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
`[1:], rt, true); err != nil {
		t.Error(err)
		return
	}

	// Test special error case with groups

	msm := mgs.StorageManager("main"+"group"+graph.StorageSuffixNodes, false).(*storage.MemoryStorageManager)

	msm.AccessMap[1] = storage.AccessCacheAndFetchError

	if _, err := getResult("get Song from group Best", "", rt, true); err.Error() !=
		"GraphError: Failed to access graph storage component (Slot not found (mystorage/maingroup.nodes - Location:1))" {
		t.Error(err)
		return
	}

	if _, err := getResult("lookup Song '1' from group Best", "", rt2, true); err.Error() !=
		"GraphError: Failed to access graph storage component (Slot not found (mystorage/maingroup.nodes - Location:1))" {
		t.Error(err)
		return
	}

	delete(msm.AccessMap, 1)

	if _, err := getResult("lookup Song 'non', 'Aria1', 'MyOnlySong3' from group Best", `
Labels: Song Key, Song Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
MyOnlySong3, MyOnlySong3, 19
`[1:], rt2, true); err != nil {
		t.Error(err)
		return
	}

	if _, err := getResult("lookup Song 'non', 'Aria1', 'MyOnlySong3' from group bbest", `
Labels: Song Key, Song Name, Ranking
Format: auto, auto, auto
Data: 1:n:key, 1:n:name, 1:n:ranking
`[1:], rt2, true); err != nil {
		t.Error(err)
		return
	}
}

func TestWithFlags(t *testing.T) {
	gm, _ := songGraph()
	rt := NewGetRuntimeProvider("test", "main", gm, NewDefaultNodeInfo(gm))

	// Test simple query to get everything

	if _, err := getResult("get Author traverse :::", `
Labels: Author Key, Author Name, Key, Kind, Name
Format: auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:name, 2:n:key, 2:n:kind, 2:n:name
000, John, Aria1, Song, Aria1
000, John, Aria2, Song, Aria2
000, John, Aria3, Song, Aria3
000, John, Aria4, Song, Aria4
123, Mike, DeadSong2, Song, DeadSong2
123, Mike, FightSong4, Song, FightSong4
123, Mike, LoveSong3, Song, LoveSong3
123, Mike, StrangeSong1, Song, StrangeSong1
456, Hans, MyOnlySong3, Song, MyOnlySong3
`[1:], rt, true); err != nil {
		t.Error(err)
		return
	}

	// Test ordering

	if _, err := getResult("get Author traverse :::Song end with ordering(ascending Song:name)", `
Labels: Author Key, Author Name, Song Key, Song Name, Ranking
Format: auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:name, 2:n:key, 2:n:name, 2:n:ranking
000, John, Aria1, Aria1, 8
000, John, Aria2, Aria2, 2
000, John, Aria3, Aria3, 4
000, John, Aria4, Aria4, 18
123, Mike, DeadSong2, DeadSong2, 6
123, Mike, FightSong4, FightSong4, 3
123, Mike, LoveSong3, LoveSong3, 1
456, Hans, MyOnlySong3, MyOnlySong3, 19
123, Mike, StrangeSong1, StrangeSong1, 5
`[1:], rt, false); err != nil {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author traverse :::Song end with ordering(descending ranking)", `
Labels: Author Key, Author Name, Song Key, Song Name, Ranking
Format: auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:name, 2:n:key, 2:n:name, 2:n:ranking
456, Hans, MyOnlySong3, MyOnlySong3, 19
000, John, Aria4, Aria4, 18
000, John, Aria1, Aria1, 8
123, Mike, DeadSong2, DeadSong2, 6
123, Mike, StrangeSong1, StrangeSong1, 5
000, John, Aria3, Aria3, 4
123, Mike, FightSong4, FightSong4, 3
000, John, Aria2, Aria2, 2
123, Mike, LoveSong3, LoveSong3, 1
`[1:], rt, false); err != nil {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author traverse :::Song end with ordering(ascending Song:ranking)", `
Labels: Author Key, Author Name, Song Key, Song Name, Ranking
Format: auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:name, 2:n:key, 2:n:name, 2:n:ranking
123, Mike, LoveSong3, LoveSong3, 1
000, John, Aria2, Aria2, 2
123, Mike, FightSong4, FightSong4, 3
000, John, Aria3, Aria3, 4
123, Mike, StrangeSong1, StrangeSong1, 5
123, Mike, DeadSong2, DeadSong2, 6
000, John, Aria1, Aria1, 8
000, John, Aria4, Aria4, 18
456, Hans, MyOnlySong3, MyOnlySong3, 19
`[1:], rt, false); err != nil {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author traverse :::Song end with ordering(ascending 2:n:ranking)", `
Labels: Author Key, Author Name, Song Key, Song Name, Ranking
Format: auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:name, 2:n:key, 2:n:name, 2:n:ranking
123, Mike, LoveSong3, LoveSong3, 1
000, John, Aria2, Aria2, 2
123, Mike, FightSong4, FightSong4, 3
000, John, Aria3, Aria3, 4
123, Mike, StrangeSong1, StrangeSong1, 5
123, Mike, DeadSong2, DeadSong2, 6
000, John, Aria1, Aria1, 8
000, John, Aria4, Aria4, 18
456, Hans, MyOnlySong3, MyOnlySong3, 19
`[1:], rt, false); err != nil {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author traverse :Wrote::Song end show 1:n:name, 2:n:name, 2:e:number with ordering(descending Song:name, ascending Wrote:number)", `
Labels: Name, Name, Number
Format: auto, auto, auto
Data: 1:n:name, 2:n:name, 2:e:number
Mike, StrangeSong1, 1
John, Aria1, 1
Mike, DeadSong2, 2
John, Aria2, 2
Hans, MyOnlySong3, 3
Mike, LoveSong3, 3
John, Aria3, 3
Mike, FightSong4, 4
John, Aria4, 4
`[1:], rt, false); err != nil {
		t.Error(err)
		return
	}

	// Test empty traversal flag

	if _, err := getResult("get Author traverse :::Song where name = '123' end with nulltraversal(true)", `
Labels: Author Key, Author Name, Song Key, Song Name, Ranking
Format: auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:name, 2:n:key, 2:n:name, 2:n:ranking
123, Mike, <not set>, <not set>, <not set>
456, Hans, <not set>, <not set>, <not set>
000, John, <not set>, <not set>, <not set>
`[1:], rt, false); err != nil {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author traverse :::Song where name = '123' end", `
Labels: Author Key, Author Name, Song Key, Song Name, Ranking
Format: auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:name, 2:n:key, 2:n:name, 2:n:ranking
`[1:], rt, false); err != nil {
		t.Error(err)
		return
	}

	// Test filtering

	if _, err := getResult("get Author traverse :::Song where name = 'DeadSong2' end with nulltraversal(true), filtering(isnotnull Song:name)", `
Labels: Author Key, Author Name, Song Key, Song Name, Ranking
Format: auto, auto, auto, auto, auto
Data: 1:n:key, 1:n:name, 2:n:key, 2:n:name, 2:n:ranking
123, Mike, DeadSong2, DeadSong2, 6
`[1:], rt, false); err != nil {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author traverse :::Song end show Author:name with filtering(unique Author:name)", `
Labels: Author Name
Format: auto
Data: 1:n:name
Mike
Hans
John
`[1:], rt, false); err != nil {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author traverse :::Song end show Author:name with filtering(uniquecount Author:name)", `
Labels: Author Name
Format: auto
Data: 1:n:name
Mike (4)
Hans (1)
John (4)
`[1:], rt, false); err != nil {
		t.Error(err)
		return
	}
}

func TestWithFlagsErrors(t *testing.T) {
	gm, _ := songGraph()
	rt := NewGetRuntimeProvider("test", "main", gm, NewDefaultNodeInfo(gm))

	if _, err := getResult("get Author traverse ::: end with filtering(unique 1:p:bla)", "", rt, false); err.Error() !=
		"EQL error in test: Invalid construct (Cannot determine column for with term: 1:p:bla) (Line:1 Pos:44)" {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author traverse ::: end with ordering(ascending p:bla)", "", rt, false); err.Error() !=
		"EQL error in test: Invalid construct (Cannot determine column for with term: p:bla) (Line:1 Pos:43)" {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author traverse ::: end with filtering(ascending p:bla)", "", rt, false); err.Error() !=
		"EQL error in test: Invalid construct (ascending) (Line:1 Pos:44)" {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author traverse ::: end with ordering(unique p:bla)", "", rt, false); err.Error() !=
		"EQL error in test: Invalid construct (unique) (Line:1 Pos:43)" {
		t.Error(err)
		return
	}

	if _, err := getResult("get Author traverse ::: end with ascending(ascending p:bla)", "", rt, false); err.Error() !=
		"EQL error in test: Invalid construct (ascending) (Line:1 Pos:34)" {
		t.Error(err)
		return
	}
}

/*
Helper function to run a search and check against a result.
*/
func getResult(query string, expectedResult string, rt parser.RuntimeProvider, sort bool) (*SearchResult, error) {
	ast, err := parser.ParseWithRuntime("test", query, rt)
	if err != nil {
		return nil, err
	}

	res, err := ast.Runtime.Eval()
	if err != nil {
		return nil, err
	}

	if sort {
		res.(*SearchResult).StableSort()
	}

	if fmt.Sprint(res) != expectedResult {
		return nil, errors.New(fmt.Sprint("Unexpected search result:", res, err))
	}

	return res.(*SearchResult), nil
}

func songGraph() (*graph.Manager, *graphstorage.MemoryGraphStorage) {

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	constructEdge := func(key string, node1 data.Node, node2 data.Node, number int) data.Edge {
		edge := data.NewGraphEdge()

		edge.SetAttr("key", key)
		edge.SetAttr("kind", "Wrote")

		edge.SetAttr(data.EdgeEnd1Key, node1.Key())
		edge.SetAttr(data.EdgeEnd1Kind, node1.Kind())
		edge.SetAttr(data.EdgeEnd1Role, "Author")
		edge.SetAttr(data.EdgeEnd1Cascading, true)

		edge.SetAttr(data.EdgeEnd2Key, node2.Key())
		edge.SetAttr(data.EdgeEnd2Kind, node2.Kind())
		edge.SetAttr(data.EdgeEnd2Role, "Song")
		edge.SetAttr(data.EdgeEnd2Cascading, false)

		edge.SetAttr("number", number)

		return edge
	}

	storeSong := func(node data.Node, name string, ranking int, number int) {
		node3 := data.NewGraphNode()
		node3.SetAttr("key", name)
		node3.SetAttr("kind", "Song")
		node3.SetAttr("name", name)
		node3.SetAttr("ranking", ranking)
		gm.StoreNode("main", node3)
		gm.StoreEdge("main", constructEdge(name, node, node3, number))
	}

	node0 := data.NewGraphNode()
	node0.SetAttr("key", "000")
	node0.SetAttr("kind", "Author")
	node0.SetAttr("name", "John")
	gm.StoreNode("main", node0)

	storeSong(node0, "Aria1", 8, 1)
	storeSong(node0, "Aria2", 2, 2)
	storeSong(node0, "Aria3", 4, 3)
	storeSong(node0, "Aria4", 18, 4)

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "Author")
	node1.SetAttr("name", "Mike")
	gm.StoreNode("main", node1)

	storeSong(node1, "LoveSong3", 1, 3)
	storeSong(node1, "FightSong4", 3, 4)
	storeSong(node1, "DeadSong2", 6, 2)
	storeSong(node1, "StrangeSong1", 5, 1)

	node2 := data.NewGraphNode()
	node2.SetAttr("key", "456")
	node2.SetAttr("kind", "Author")
	node2.SetAttr("name", "Hans")
	gm.StoreNode("main", node2)

	storeSong(node2, "MyOnlySong3", 19, 3)

	return gm, mgs.(*graphstorage.MemoryGraphStorage)
}

func songGraphGroups() (*graph.Manager, *graphstorage.MemoryGraphStorage) {
	gm, mgs := songGraph()

	node0 := data.NewGraphNode()
	node0.SetAttr("key", "Best")
	node0.SetAttr("kind", GroupNodeKind)
	gm.StoreNode("main", node0)

	constructEdge := func(songkey string) data.Edge {
		edge := data.NewGraphEdge()

		edge.SetAttr("key", songkey)
		edge.SetAttr("kind", "Contains")

		edge.SetAttr(data.EdgeEnd1Key, node0.Key())
		edge.SetAttr(data.EdgeEnd1Kind, node0.Kind())
		edge.SetAttr(data.EdgeEnd1Role, "group")
		edge.SetAttr(data.EdgeEnd1Cascading, false)

		edge.SetAttr(data.EdgeEnd2Key, songkey)
		edge.SetAttr(data.EdgeEnd2Kind, "Song")
		edge.SetAttr(data.EdgeEnd2Role, "Song")
		edge.SetAttr(data.EdgeEnd2Cascading, false)

		return edge
	}

	gm.StoreEdge("main", constructEdge("LoveSong3"))
	gm.StoreEdge("main", constructEdge("Aria3"))
	gm.StoreEdge("main", constructEdge("MyOnlySong3"))
	gm.StoreEdge("main", constructEdge("StrangeSong1"))

	return gm, mgs
}

func dateGraph() (*graph.Manager, *graphstorage.MemoryGraphStorage) {

	mgs := graphstorage.NewMemoryGraphStorage("mystorage")
	gm := graph.NewGraphManager(mgs)

	node0 := data.NewGraphNode()
	node0.SetAttr("key", "000")
	node0.SetAttr("kind", "datetest")
	node0.SetAttr("name", "date1")
	node0.SetAttr("unix", 1349809255)
	node0.SetAttr("RFC3339_value", "2012-10-09T19:00:55Z")
	node0.SetAttr("naive_value", "2012-10-09")
	gm.StoreNode("main", node0)

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "001")
	node1.SetAttr("kind", "datetest")
	node1.SetAttr("name", "date2")
	node1.SetAttr("unix", 1350061255)
	node1.SetAttr("RFC3339_value", "2012-10-12T19:00:55+02:00")
	node1.SetAttr("naive_value", "2012-10-12")
	gm.StoreNode("main", node1)

	return gm, mgs.(*graphstorage.MemoryGraphStorage)
}

func filterGraph() (*graph.Manager, *graphstorage.MemoryGraphStorage) {
	gm, mgs := songGraph()

	constructNode := func(key, val1, val2, val3 string) data.Node {
		node0 := data.NewGraphNode()
		node0.SetAttr("key", key)
		node0.SetAttr("kind", "filtertest")
		node0.SetAttr("val1", val1)
		node0.SetAttr("val2", val2)
		node0.SetAttr("val3", val3)

		return node0
	}

	gm.StoreNode("main", constructNode("1", "test", "Hans", "foo"))
	gm.StoreNode("main", constructNode("2", "test1", "Hans", "foo"))
	gm.StoreNode("main", constructNode("3", "test2", "Hans", "foo"))
	gm.StoreNode("main", constructNode("4", "test3", "Peter", "foo"))
	gm.StoreNode("main", constructNode("5", "test4", "Peter", "foo"))
	gm.StoreNode("main", constructNode("6", "test5", "Peter", "foo"))
	gm.StoreNode("main", constructNode("7", "test6", "Anna", "foo"))
	gm.StoreNode("main", constructNode("8", "test7", "Anna", "foo"))
	gm.StoreNode("main", constructNode("9", "test8", "Steve", "foo"))
	gm.StoreNode("main", constructNode("10", "test9", "Steve", "foo"))
	gm.StoreNode("main", constructNode("11", "test10", "Franz", "foo"))
	gm.StoreNode("main", constructNode("12", "test11", "Kevin", "foo"))
	gm.StoreNode("main", constructNode("13", "test12", "Kevin", "foo"))
	gm.StoreNode("main", constructNode("14", "test13", "Kevin", "foo"))
	gm.StoreNode("main", constructNode("15", "test14", "X1", "foo"))
	gm.StoreNode("main", constructNode("16", "test15", "X2", "foo"))
	gm.StoreNode("main", constructNode("17", "test16", "X3", "foo"))
	gm.StoreNode("main", constructNode("18", "test17", "X4", "foo"))
	gm.StoreNode("main", constructNode("19", "test17", "X5", "foo"))

	return gm, mgs
}
