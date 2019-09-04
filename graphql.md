EliasDB GraphQL
===============

EliasDB supports GraphQL to search nodes in a partition of the graph database. A simple GraphQL query has the following structure:
```
{
  <node kind> : {
    <attr1>
    <attr2>
    <attr3>
  }
}
```
It reads: "Get all graph nodes of a certain node kind and fetch attr1, attr2 and attr3 of every node".

Filtering results
-----------------

It is possible to reduce the number of resulting nodes by defining a condition using the `matches` argument. For example to get all `Person` nodes which start with the letters `Jo` you could write:
```
query {
  Person(matches: {
    name : "^Jo.*"
  }) {
    name
  }
}
```
The condition can be inverted by prefixing with `not_` so to get all `Person` nodes which do *NOT* start with `Jo` you could write:
```
query {
  Person(matches: {
    not_name : "^Jo.*"
  }) {
    name
  }
}
```
To retrieve a specific node with a known key it is possible to do a direct lookup by key:
```
query {
  Person(key: "john") {
    name
  }
}
```

Sorting and limiting
--------------------
To manage potentially large results and avoid overwhelming a client with data it is possible to sort and limit the result.

To sort a result in ascending or descending order use the arguments `ascending` or `descending` with the ordering attribute. To order all Person nodes by ascending name write:
```
query {
  Person(ascending: "name") {
    name
  }
}
```
To limit the result to the last `n` entries use the argument `last`. To limit the result to a range use `from` to define the start position (default is 0) and `items` to define how many entries should be returned:
```
query {
  Person(ascending: "name", last:10) {
    name
  }
}
```

Traversal
---------
To traverse the graph you need to add the `traverse` argument on a field of the selection set. For example to get the friends of a Person write:
```
query {
  Person(ascending: "name") {
    name
    friends(traverse: ":Friend::Person") {
      name
    }
  }
}
```

Data modification
-----------------
If the endpoint support `mutation` of data then you can store and remove nodes and edges. Node and edge storage (create or update) requires all attributes to be specified (nodes and edges are overwritten):
```
mutation {
  Person(storeNode: {
      key: "hans"
      name: "Hans"
    }) {
    name
    friends(traverse: ":Friend::Person") {
      name
    }
  }
}
```
Possible arguments are `storeNode, storeEdge, removeNode and removeEdge`. Removal of nodes and edges requires only the `key` and `kind` to be specified. The operation allows retrieval of nodes as well (i.e. the single operation will insert *AND* retrieve data).

Subscription to updates
-----------------------
EliasDB's implementation of GraphQL supports also subscriptions which involve using Websockets for bidirectional communication. See the examples for further details (in code). A subscription is basically a normal query for example:
```
subscription {
  Person(ascending: "name") {
    name
  }
}
```
which receives continuous from the server as the underlying data changes.
