EliasDB query language
======================

EliasDB query language (EQL) is a query langugage to search nodes in a partition of the graph database. Its syntax is designed to follow natural language supporting complex graph queries while keeping simple queries simple. A simple EQL query has the following structure:
```
get <node kind> where <condition>
```
It reads: "Get all graph nodes of a certain node kind which match a certain condition". The condition is evaluated for each node from the specified kind. For example to get all "Person" nodes with the name "John" you could write:
```
get Person where name = John
```
The result of this query is a table listing all data store nodes which have a node attribute name with the value John.

Where clause
------------

A where clause supports the following operators:

- Standard boolean operators: and, or, not

- Standard condition operators: =, !=, >, <, >=, <=, in, notin, contains, beginswith, endswith, containsnot

- Standard arithmetic operators: +, -, *, /

- Integer operations: // (integer division), % (modulo)

- Regular expression operator: like

Operators can be combined. Expressions can be segregated using parentheses. Each where condition should end in a boolean value. List operators such as “in” and “notin” operate on sequences of values which can be declared with square brackets e.g. [1,2,3].

- Where clauses also support the following constants: true, false, null

To explicitly define if a value represents a literal or a name of a node or edge attribute it is possible to prefix it with either 'attr:' for a node attribute name, 'eattr:' for an edge attribute name or 'val:' for a literal. In the majority of cases however the query interpreter will determine the right meaning. The precedence is: node attribute, edge attribute, literal value.

EQL supports nested object structures on node attributes. A node value of { l1 : { l2 : { l3 : 123 } } } can be queried as:

<attr name>.l1.l2.l3 = 123

If the actual attribute name contins a dot then the 'attr:' prefix must be used.


Traversal blocks
----------------

For the majority of useful queries it will be necessary to traverse the graph. Relationships between nodes can be matched with traversal specifications. A traversal specification has the following form:
```
<source role>:<relationship kind>:<destination role>:<destination kind>
```
All components of a traversal specification are optional. A traversal of all relationships can be expressed with:
```
:::
```
Traversal expressions in a query are defined as block expressions:
```
get <node kind> where <condition>
 traverse <traversal spec> where <condition>
    traverse <traversal spec> where <condition>
        <Further traversals>
    end
end
```
Traversal expressions define which parts of the graph should be collected for the query. Reading from top to bottom each traversal expression defines a traversal step. Each traversal step will add several columns to the result if no explicit show clause is defined.

Show clause
-----------

To control which data will be displayed in the final result it is possible to define a show clause. A show clause explicitly states which columns should be displayed in the result table.
```
get <node kind> where <condition>
 traverse <traversal spec> where <condition>
    traverse <traversal spec> where <condition>
        <Further traversals>
    end
end
show <show clause for column 1>, <show clause for column 2>, ...
```
The data can be defined by traversal position, as attribute name or as node/edge kind with attribute name.

Examples:
```
1:n:key  - Display the key of the start nodes
2:e:name - Display the name of the relationship from the 1 traversal step
Person:name - Display the name of the first defined Person node from the query
name – Display the name of the first defined node which has a name attribute
```
With clause
-----------

Operation which need to be applied once all rows of the result have been fetched can be defined in the with clause. If a with clause is defined it is always the last clause in a query.
```
get <node kind> show <show clauses> with <with operation>, <with operation>, ...
```
The following operations are possible:

- ordering - Order a column (e.g. ordering(ascending Person:name) )
             Available directives: ascending, descending
 
- filtering - Filter a column (e.g. filtering(unique 2:e:name) )
              Available directives: unique (column will only have unique values),
                                    unique count (column will show unique values
                                                  and a count of how many values were  
                                                  encountered),
                                    isnotnull (column will only contain not null 
                                               values)
- nulltraversal – Only includes rows in the result where all traversals steps
                  where executed (i.e. do not include partial traversals)
                  Available directives: true, false

Functions
---------

Functions can be used to construct result values. A function can be used inside a where clause and inside a show clause. All function start with an “@” sign.

Functions for conditions:
```
@count(<traversal spec>) - Counts how many nodes can be reached via a given spec from the traversal step of the condition.
```

```
@parseDate(<date string>, <opt. layout>) - Converts a given date string into an unix time integer. The optional second parameter is the parsing layout stated as reference time (Mon Jan 2 15:04:05 -0700 MST 2006) - e.g. '2006-01-02' interprets <year>-<month>-<day> strings. The default layout is RFC3339.
```

Functions for the show clause:
```
@count(<traversal step>, <traversal spec>) - Counts how many nodes can be reached via a given spec from a given traversal step.
```

```
@objget(<traversal step>, <attribute name>, <path to value>) - Extracts a value from a nested object structure.
```
