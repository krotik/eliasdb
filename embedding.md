EliasDB Code Tutorial
=====================
The following text will give you an introduction on how to embed EliasDB in another Go project.

Prerequisites
-------------
You have a `go modules` (see [here](https://golang.org/cmd/go/#hdr-Modules__module_versions__and_more)) based go project.

You can create a simple one by running:
```
go mod init example.com/test
```
and creating a file called `main.go` with the following content:
```
package main

import "fmt"

func main() {
	fmt.Println("Test")
}
```
Running `go build` should create a `test` executable in the current folder. Running `./test` will just output `Test`.

Simple graph database setup
---------------------------
The first step is to create a graph storage which will store the data. The following code will create a disk storage in the db/ subdirectory (the false flag opens the store in read / write mode):
```
import (
	...
		"devt.de/krotik/eliasdb/graph/graphstorage"
)

func main() {
...
	// Create a graph storage

	gs, err := graphstorage.NewDiskGraphStorage("db", false)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer gs.Close()
...
```
Running `go build` again should now download eliasdb as additional dependency (the actual versions might be different):
```
go: finding devt.de/krotik/eliasdb/graph/graphstorage latest
go: finding devt.de/krotik/eliasdb/graph latest
go: finding devt.de/krotik/eliasdb v1.0.0
go: downloading devt.de/krotik/eliasdb v1.0.0
go: extracting devt.de/krotik/eliasdb v1.0.0
go: finding github.com/gorilla/websocket v1.4.1
go: finding devt.de/krotik/common v1.0.0
go: downloading devt.de/krotik/common v1.0.0
go: extracting devt.de/krotik/common v1.0.0
```
The `go build` command will have modified the `go.mod` file and created a `go.sum` file.

It is important to close a disk storage before shutdown. It is also possible to create a memory-only storage with:
```
	gs = graphstorage.NewMemoryGraphStorage("memdb")
```

After creating a storage we can now create a GraphManager object which provides the graph API:
```
	gm := graph.NewGraphManager(gs)

```

Storing and retrieving data
---------------------------
The main storage element in a graph database are nodes. All nodes stored in EliasDB are identified by a combination of key and kind. The node kind is basically the node type (e.g. Person) while the key is a node unique identifier.

To store a single node in the datastore we can write the following code:
```
	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "mynode")
	node1.SetAttr("name", "Node1")
	node1.SetAttr("text", "The first stored node")

	gm.StoreNode("main", node1)
```
The attributes key and kind are compulsory. Storing a node with the same key and kind will overwrite any existing node. Each node should have a name which should be a human-readable label for the node. The StoreNode call gets a partition as the first argument. Nodes stored in separate partitions can not be linked by an edge. Search queries are scoped to a single partition.

Nodes can be linked together via an edge:
```
	node2 := data.NewGraphNode()
	node2.SetAttr(data.NodeKey, "456")
	node2.SetAttr(data.NodeKind, "mynode")
	node2.SetAttr(data.NodeName, "Node2")

	gm.StoreNode("main", node2)

	edge := data.NewGraphEdge()

	edge.SetAttr(data.NodeKey, "abc")
	edge.SetAttr(data.NodeKind, "myedge")

	edge.SetAttr(data.EdgeEnd1Key, node1.Key())
	edge.SetAttr(data.EdgeEnd1Kind, node1.Kind())
	edge.SetAttr(data.EdgeEnd1Role, "node1")
	edge.SetAttr(data.EdgeEnd1Cascading, true)

	edge.SetAttr(data.EdgeEnd2Key, node2.Key())
	edge.SetAttr(data.EdgeEnd2Kind, node2.Kind())
	edge.SetAttr(data.EdgeEnd2Role, "node2")
	edge.SetAttr(data.EdgeEnd2Cascading, false)

	edge.SetAttr(data.NodeName, "Edge1")

	gm.StoreEdge("main", edge)
```
Edges have more compulsory attributes than nodes. As well as key and kind for the edge itself, you also need to define for each end the key, kind, a role and a cascading flag. The cascading flag defines if delete actions to an end should be propagated to the other end. The role is a name which defines one end's relationship to the other. It is only used for traversals. An example relationship of nodes through an edge could be described like this:

(Hans/Person) Father -- Family -- Child (Klaus/Person)

We could traverse this relationship by writing:
```
    gm.Traverse("main", node1.Key(), node1.Kind(), "Father:Family:Child:Person", true)
```
The last boolean flag indicates if all data from the target node should be received. If set to false only the key and kind will be populated. If multiple edge kinds or roles should be traversed it is possible to use gm.TraverseMulti. Omitting a traversal component is like using a wildcard (e.g. :Family:: will traverse all family edges to any node kind).

The storage of nodes and edges can be combined in a transaction. The transaction either inserts all items or none.
```
	trans := graph.NewGraphTrans(gm)
	trans.StoreNode(...)
	trans.StoreEdge(...)
	trans.Commit()
```
Now that the datastore has some data we can use the graph API to query the data. To query a node you can use a lookup:
```
	n, err := gm.FetchNode("main", "123", "mynode")
	fmt.Println(n, err)
```
To iterate over all nodes of a specific kind you can use a node iterator:
```
it, err := gm.NodeKeyIterator("main", "mynode")
for it.HasNext() {
	key := it.Next()

	if it.LastError != nil {
		break
	}

	n, err := gm.FetchNode("main", key, "mynode")
	fmt.Println(n, err)
}
```

Querying the datastore
----------------------
Besides direct lookups and iterators the datastore also supports higher search functionality such as phrase searching and a query language.

All data in the datastore is indexed. To query for a certain phrase you can run a phrase search:
```
idx, idxerr := gm.NodeIndexQuery("main", "mynode")
if idxerr == nil {

	keys, err := idx.LookupPhrase("text", "first stored")
	if err == nil {

		for _, key := range keys {
			n, err := gm.FetchNode("main", key, "mynode")
			fmt.Println(n, err)
		}
	}
}
```
For even more complex searches you can use EQL (see also the EQL manual  [here](https://devt.de/krotik/eliasdb/src/master/eql.md)):
```
res, err := eql.RunQuery("myquery", "main", "get mynode where name = 'Node2'", gm)

fmt.Println(res, err)
```

Adding REST API endpoints
-------------------------
EliasDB's REST API can be added easily when using Go's default webserver and router:
```
api.RegisterRestEndpoints(v1.V1EndpointMap)
api.RegisterRestEndpoints(api.GeneralEndpointMap)
```

Example source
--------------
An example demo.go could look like this:
```
package main

import (
	"fmt"
	"log"

	"devt.de/krotik/eliasdb/eql"
	"devt.de/krotik/eliasdb/graph"
	"devt.de/krotik/eliasdb/graph/data"
	"devt.de/krotik/eliasdb/graph/graphstorage"
)

func main() {

	// Create a graph storage

	//gs, err := graphstorage.NewDiskGraphStorage("db", false)
	//if err != nil {
	//		log.Fatal(err)
	//		return
	//	}
	//defer gs.Close()

	// For memory only storage do:

	gs := graphstorage.NewMemoryGraphStorage("memdb")

	gm := graph.NewGraphManager(gs)

	// Create transaction

	trans := graph.NewGraphTrans(gm)

	// Store node1

	node1 := data.NewGraphNode()
	node1.SetAttr("key", "123")
	node1.SetAttr("kind", "mynode")
	node1.SetAttr("name", "Node1")
	node1.SetAttr("text", "The first stored node")

	if err := trans.StoreNode("main", node1); err != nil {
		log.Fatal(err)
	}

	// Store node 2

	node2 := data.NewGraphNode()
	node2.SetAttr(data.NodeKey, "456")
	node2.SetAttr(data.NodeKind, "mynode")
	node2.SetAttr(data.NodeName, "Node2")

	if err := trans.StoreNode("main", node2); err != nil {
		log.Fatal(err)
	}

	if err := trans.Commit(); err != nil {
		log.Fatal(err)
	}

	trans = graph.NewGraphTrans(gm)

	// Store edge between nodes

	edge := data.NewGraphEdge()

	edge.SetAttr(data.NodeKey, "abc")
	edge.SetAttr(data.NodeKind, "myedge")

	edge.SetAttr(data.EdgeEnd1Key, node1.Key())
	edge.SetAttr(data.EdgeEnd1Kind, node1.Kind())
	edge.SetAttr(data.EdgeEnd1Role, "node1")
	edge.SetAttr(data.EdgeEnd1Cascading, true)

	edge.SetAttr(data.EdgeEnd2Key, node2.Key())
	edge.SetAttr(data.EdgeEnd2Kind, node2.Kind())
	edge.SetAttr(data.EdgeEnd2Role, "node2")
	edge.SetAttr(data.EdgeEnd2Cascading, false)

	edge.SetAttr(data.NodeName, "Edge1")

	if err := gm.StoreEdge("main", edge); err != nil {
		log.Fatal(err)
	}

	// Commit transaction

	if err := trans.Commit(); err != nil {
		log.Fatal(err)
	}

	// Demo traversal:

	nodes, edges, err := gm.TraverseMulti("main", "123", "mynode", ":::", false)
	fmt.Println("out1:", nodes, edges, err)

	// Demo key iterator:

	it, err := gm.NodeKeyIterator("main", "mynode")
	for it.HasNext() {
		key := it.Next()

		if it.LastError != nil {
			break
		}

		n, err := gm.FetchNode("main", key, "mynode")
		fmt.Println("out2:", n, err)
	}

	// Demo full text search

	idx, idxerr := gm.NodeIndexQuery("main", "mynode")
	if idxerr == nil {

		keys, err := idx.LookupPhrase("text", "first stored")
		if err == nil {

			for _, key := range keys {
				n, err := gm.FetchNode("main", key, "mynode")
				fmt.Println("out3:", n, err)
			}
		}
	}

	// Demo eql query

	res, err := eql.RunQuery("myquery", "main", "get mynode where name = 'Node2'", gm)

	fmt.Println("out4:", res, err)
}
```
