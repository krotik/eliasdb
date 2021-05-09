EliasDB Event Condition Action Language
=======================================

EliasDB supports a scripting language called [Event Condition Action Language (ECAL)](ecal-lang.md) to enable rule based scripting functionality. ECAL provides [database trigger](https://en.wikipedia.org/wiki/Database_trigger) functionality for EliasDB.

ECAL was added for the following use-cases:
- Providing a way to manipulate data in response to events
- Enforce certain aspects of a database schema
- Providing back-end logic for web applications using EliasDB

The source of EliasDB comes with a [chat example](examples/chat/doc/chat.md) containing a simple ECAL script which adds a timestamp to nodes and a [game example](examples/game/doc/game.md) which demonstrates a more complex application of ECAL.

ECAL related config values:
--
These ECAL related config options are available in `eliasdb.config.json`:

| Configuration Option | Description |
| --- | --- |
| EnableECALScripts | Enable ECAL scripting. |
| ECALScriptFolder | Scripting folder for ECAL scripts. |
| ECALEntryScript | Entry script in the script folder. |
| ECALLogFile | File in which the logs should be written (use an empty string for stdout). |
| ECALLogLevel | Log level for the printed logs. |
| EnableECALDebugServer | Enable debugging and start the ECAL debug server. Note: Activating debugging will slow down the interpreter speed significantly! |
| ECALDebugServerHost | Host for the debug server. |
| ECALDebugServerPort | Port for the debug server. |
| ECALWorkerCount | Number of worker threads in the ECA engine's thread pool. |

ECAL Debugging
--
If the debug server is enabled in the config file then it is possible to debug ECAL scripts with [VSCode](https://devt.de/krotik/ecal/src/master/ecal-support/README.md). The debugger supports break points and thread state inspection. It is also possible to restart and reload the scripts.

Using the `-ecal-console` parameter it is possible to open an interactive console into the server process. If used together with the debug server additional debug commands are available also there. Enter `?` to see the build-in documentation.

EliasDB specific events which can be handled:
--
The ECAL interpreter in EliasDB receives the following events:

| Web Request | ECAL event kind | Event state contents | Description |
| --- | ---| --- | --- |
| /db/api/|`db.web.api`| bodyJSON, bodyString, header, method, path, pathList, query | Any web request to /db/api/... These endpoints are public and never require authentication. |
| /db/ecal/|`db.web.ecal`| bodyJSON, bodyString, header, method, path, pathList, query | Any web request to /db/ecal/... These endpoints are considered internal and require authentication if access control is enabled. |
| /db/sock/|`db.web.sock`| bodyJSON, bodyString, commID, header, method, path, pathList, query | Any web request to /db/sock/... These endpoints are used to initiate websocket connections. |
| - |`db.web.sock.data`| commID, data, header, method, path, pathList, query | An existing websocket connection received some JSON object data. If the close attribute of the object is set to true then the websocket connection is closed. |

| EliasDB Graph Event | ECAL event kind | Event state contents | Description |
| --- | --- | --- | --- |
| graph.EventNodeCreated | `db.node.created` | part, trans, node | A node was created. |
| graph.EventNodeUpdated | `db.node.updated` | part, trans, node, old_node | A node was updated. |
| graph.EventNodeDeleted | `db.node.deleted` | part, trans, node | A node was deleted. |
| graph.EventEdgeCreated | `db.edge.created` | part, trans, edge | An edge was created. |
| graph.EventEdgeUpdated | `db.edge.updated` | part, trans, edge, old_edge | An edge was updated. |
| graph.EventEdgeDeleted | `db.edge.deleted` | part, trans, edge | An edge was deleted. |
| graph.EventNodeStore | `db.node.store` | part, trans, node | A node is about to be stored (always overwriting existing values). |
| graph.EventNodeUpdate | `db.node.update` | part, trans, node | A node is about to be updated. |
| graph.EventNodeDelete | `db.node.delete` | part, trans, key, kind | A node is about to be deleted. |
| graph.EventEdgeStore | `db.edge.store` | part, trans, edge | An edge is about to be stored. |
| graph.EventEdgeDelete | `db.edge.delete` | part, trans, key, kind | An edge is about to be deleted. |

Note: EliasDB will wait for the event cascade to be finished before performing the actual operation (e.g. inserting a node). If the event handling requires a time consuming operation then a new parallel event cascade can be started using `addEvent` with a scope:

EliasDB can receive the following events from the ECAL interpreter:

| ECAL event kind | Event state contents | Description |
| --- | --- | --- |
| db.web.sock.msg | commID, payload, close | The payload is send to a client with an open websocket identified by the commID. |
```
addEvent("request", "foo.bar.xxx", {
   "payload" : 123
}, {
   "": true  # This scope allows all events
})
```

EliasDB specific functions:
--
The ECAL interpreter in EliasDB supports the following EliasDB specific functions:

#### `db.storeNode(partition, nodeMap, [transaction])`
Inserts or updates a node in EliasDB.

Parameter | Description
-|-
partition | Partition of the node
nodeMap | Node object as a map with at least a key and a kind attribute
transaction | Optional a transaction to group a set of changes

Example:
```
db.storeNode("main", {
  "key" : "foo",
  "kind" : "bar",
  "data" : 123,
})
```

#### `db.updateNode(partition, nodeMap, [transaction])`
Updates a node in EliasDB (only update the given values of the node).

Parameter | Description
-|-
partition | Partition of the node
nodeMap | Node object as a map with at least a key and a kind attribute
transaction | Optional a transaction to group a set of changes

Example:
```
db.updateNode("main", {
  "key" : "foo",
  "kind" : "bar",
  "data" : 123,
})
```

#### `db.removeNode(partition, nodeKey, nodeKind, [transaction])`
Removes a node in EliasDB.

Parameter | Description
-|-
partition | Partition of the node
nodeKey | Key attribute of the node to remove
nodeKind | Kind attribute of the node to remove
transaction | Optional a transaction to group a set of changes

Example:
```
db.removeNode("main", "foo", "bar")
```

#### `db.fetchNode(partition, nodeKey, nodeKind)`
Fetches a node in EliasDB.

Parameter | Description
-|-
partition | Partition of the node
nodeKey | Key attribute of the node to fetch
nodeKind | Kind attribute of the node to fetch

Example:
```
db.fetchNode("main", "foo", "bar")
```

#### `db.storeEdge(partition, edgeMap, [transaction])`
Inserts or updates an edge in EliasDB.

Parameter | Description
-|-
partition | Partition of the edge
edgeMap | Edge object as a map with at least the main attributes: key, kind, end1cascading, end1key, end1kind, end1role, end2cascading, end2key, end2kind, end2role
transaction | Optional a transaction to group a set of changes

Example:
```
db.storeEdge("main", {
  "key":           "123",
  "kind":          "myedges",
  "end1cascading": true,
  "end1key":       "foo",
  "end1kind":      "bar",
  "end1role":      "role1",
  "end2cascading": false,
  "end2key":       "key2",
  "end2kind":      "kind2",
  "end2role":      "role2",
})
```

#### `db.removeEdge(partition, edgeKey, edgeKind, [transaction])`
Removes an edge in EliasDB.

Parameter | Description
-|-
partition | Partition of the edge
edgeKey | Key attribute of the edge to remove
edgeKind | Kind attribute of the edge to remove
transaction | Optional a transaction to group a set of changes

Example:
```
db.removeEdge("main", "123", "myedges")
```

#### `db.fetchEdge(partition, edgeKey, edgeKind)`
Fetches an edge in EliasDB.

Parameter | Description
-|-
partition | Partition of the edge
edgeKey | Key attribute of the edge to fetch
edgeKind | Kind attribute of the edge to fetch

Example:
```
db.fetchEdge("main", "123", "myedges")
```

#### `db.traverse(partition, nodeKey, nodeKind, traversalSpec)`
Traverses an edge in EliasDB from a given node. Returns a list of nodes which were
reached and a list of edges which were followed.

Parameter | Description
-|-
partition | Partition of the node
nodeKey | Key attribute of the node to traverse from
nodeKind | Kind attribute of the node to traverse from
traversalSpec | Traversal spec

Example:
```
[nodes, edges] := db.traverse("main", "foo", "bar", "role1:myedges:role2:kind2")
```

#### `db.newTrans()`
Creates a new transaction for EliasDB.

Example:
```
trans := db.newTrans()
```

#### `db.newRollingTrans(n)`
Creates a new rolling transaction for EliasDB. A rolling transaction commits after n entries.

Parameter | Description
-|-
n | Rolling threshold (number of operations before rolling)

Example:
```
trans := db.newRollingTrans(5)
```

#### `db.commit(transaction)`
Commits an existing transaction for EliasDB.

Parameter | Description
-|-
transaction | Transaction to execute

Example:
```
db.commit(trans)
```

#### `db.query(partition, query)`
Run an EQL query.

Parameter | Description
-|-
partition | Partition to query
query | Query to execute

Example:
```
db.commit("main", "get bar")
```

#### `db.graphQL(partition, query, [variables], [operationName])`
Run a GraphQL query.

Parameter | Description
-|-
partition | Partition to query
query | Query to execute
variables | Map of variables for the query
operationName | Operation to execute (useful if the query defines more than a single operation)

Example:
```
db.graphQL("main", "query myquery($x: string) { bar(key:$x) { data }}", {
  "x": "foo",  
}, "myquery")
```

#### `db.raiseGraphEventHandled()`
When handling a graph event, notify the GraphManager of EliasDB that no further action is necessary. This creates a special error object and should not be used inside a `try` block. When using a `try` block this can be used inside an `except` or `otherwise` block.

Example:
```
sink mysink
  kindmatch [ "db.*.*" ],
{
  db.raiseGraphEventHandled()
}
```

#### `db.raiseWebEventHandled()`
When handling a web event, notify the web API of EliasDB that the web request was handled. This creates a special error object and should not be used inside a `try` block. When using a `try` block this can be used inside an `except` or `otherwise` block.

Example:
```
sink mysink
  kindmatch [ "web.*.*" ],
{
  db.raiseWebEventHandled({
    "status" : 200,
    "headers" : {
      "Date": "today"
    },
    "body" : {
      "mydata" : [1,2,3]
    }
  })
}
```
