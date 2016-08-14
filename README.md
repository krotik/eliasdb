EliasDB
=======

EliasDB is a graph based database which aims to provide a lightweight solution for projects which want to store their data as graph.

Status
------
Swgger Definition: <a href="http://online.swagger.io/validator/debug?url=https://raw.githubusercontent.com/krotik/eliasdb/master/doc/swagger.json"><img src="http://online.swagger.io/validator?url=https://raw.githubusercontent.com/krotik/eliasdb/master/doc/swagger.json" id="validator"></a>

Features
--------
- Build on top of a fast key-value store which supports transactions and memory-only storage.
- Data is stored in nodes (key-value objects) which are connected via edges.
- Stored graphs can be separated via partitions.
- Stored graphs support cascading deletions - delete one node and all its "children".
- All stored data is indexed and can be quickly searched via a full text phrase search.
- For more complex queries EliasDB has an own query language called EQL with an sql-like syntax.
- Written in Go from scratch. No third party libraries were used apart from Go's standard library.
- The database can be embedded or used as a standalone application.
- When used as a standalone application it comes with an internal HTTPS webserver which
  provides a REST API and a basic file server.
- When used as an embedded database it supports transactions with rollbacks, iteration of data
  and rule based consistency management.


Getting Started (standalone application)
----------------------------------------
You can download a precompiled package for Win64 [here](https://raw.githubusercontent.com/krotik/eliasdb/master/res/eliasdb_v0_8.zip).

Extract it and execute the executable. The executable should automatically create 3 subfolders and a configuration file. Point your webbrowser to:
```
https://localhost:9090/db/term.html
```
After accepting the self-signed certificate from the server you should see a web terminal. 

### Using as a standalone application:

The terminal uses a REST API to communicate with the backend. The REST API can be browsed using a dynamically generated swagger.json definition (https://localhost:9090/db/swagger.json). You can browse the API of the EliasDB's latest version [here](http://petstore.swagger.io/?url=https://raw.githubusercontent.com/krotik/eliasdb/master/doc/swagger.json#/default).

Further Reading
---------------
- A design document which describes the different components of the graph database. [Link](/doc/elias_db_design.md)
- A reference for the EliasDB query language EQL. [Link](/doc/eql.md)

License
-------
EliasDB source code is available under the [Mozilla Public License](/LICENSE).
