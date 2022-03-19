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

It is possible to reduce the number of resulting nodes by defining a condition using the `matches` argument.

The simplest case is to retrieve a node with a specific value:
```
query {
  Person(matches: {
    name : "John"
  }) {
    name
  }
}
```
Multiple values can be matched by specifying a list:
```
query {
  Person(matches: {
    name : ["John", "Frank"]
  }) {
    name
  }
}
```
For more complex cases it is also possible to use a Regex. For example to get all `Person` nodes where the `name` starts with the letters `Jo` you could write:
```
query {
  Person(matches: {
    name : "^Jo.*"
  }) {
    name
  }
}
```
The condition can be inverted by prefixing with `not_`. To get all `Person` nodes where the `name` does *NOT* start with `Jo`:
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

To sort a result in ascending or descending order use the arguments `ascending` or `descending` with the ordering attribute. Ordering is only possible for attributes which are part of the query. To order all Person nodes in ascending name write:
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
To traverse the graph you can add the `traverse` argument on a field of the selection set. For example to get the friends of a Person write:
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
If the traversal route does not matter (e.g. a traversal wildcard would be used above :::Person) then a shortcut is available:
```
query {
  Person(ascending: "name") {
    name
    friends: Person {
      name
    }
  }
}
```

Fragments
---------
Fragments allow repeated selections to be defined once and be reused via a label:
```
{
  Station(ascending:key) {
    ...stationFields
    Station(ascending:key) {
      ...stationFields
    }
  }
}
fragment stationFields on Station {
  key
  name
  zone
}
```

Fragments can also be used as type conditions to query different attributes dependent on the encountered node kind:
```
{
  Station(ascending:key) {
    ...stationFields
    StationAndLines(traverse: ":::", ascending:key) {
      ...stationFields
      ... on Line {
        key
        name
      }
    }
  }
}
fragment stationFields on Station {
  key
  name
  zone
}
```
The example above shows a combination of a separate fragment definition and an inline fragment.


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
Possible arguments are `storeNode, storeEdge, removeNode and removeEdge`. The operation allows retrieval of nodes as well (i.e. the single operation will insert *AND* retrieve data). Removal of edges requires only the `key` and `kind` to be specified. Removal of nodes requires only the `kind` to be specified. Using `removeNodes` with a missing `key` will remove all nodes of the kind.


Variables
---------
To avoid parsing issues and possible security risks it is advisable to always use variables to pass data to EliasDB especially if it is a user-provided value. EliasDB supports all GraphQL default types: string, integer, float
```
mutation($name: string) {
    Person(storeNode: {
        key: "hans"
        name: $name
    }) {
        key
        name
    }
}
```
The type name (in the example `string`) is not evaluated in EliasDB's GraphQL interpreter. The values should be send in a separate variables datastructure:
```
{
  name: "Hans"
}
```
Variables can be used in combination with fragments and the directives `@skip` and `@include` to modify queries:
```
query Stations($expandedInfo: boolean=true){
  Station(ascending:key) {
    ...stationFields
    Station(ascending:key) {
      ...stationFields
      ... on Station @include(if: $expandedInfo) {
        zone
      }
    }
  }
}
fragment stationFields on Station {
  key
  name
}
```

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
