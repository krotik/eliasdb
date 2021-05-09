EliasDB
=======

<p align="center">
  <img height="300px" style="height:300px;" src="eliasdb_logo.png">
</p>

EliasDB is a graph-based database which aims to provide a lightweight solution for projects which want to store their data as a graph.

[![Code coverage](https://void.devt.de/pub/eliasdb/test_result.svg)](https://void.devt.de/pub/eliasdb/coverage.txt)
[![Go Report Card](https://goreportcard.com/badge/devt.de/krotik/eliasdb?style=flat-square)](https://goreportcard.com/report/devt.de/krotik/eliasdb)
[![Go Reference](https://pkg.go.dev/badge/krotik/eliasdb.svg)](https://pkg.go.dev/devt.de/krotik/eliasdb)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge-flat.svg)](https://github.com/avelino/awesome-go)

Features
--------
- Build on top of a custom key-value store which supports transactions and memory-only storage.
- Data is stored in nodes (key-value objects) which are connected via edges.
- Stored graphs can be separated via partitions.
- Stored graphs support cascading deletions - delete one node and all its "children".
- All stored data is indexed and can be quickly searched via a full text phrase search.
- EliasDB has a GraphQL interface which can be used to store and retrieve data.
- For more complex queries EliasDB has an own query language called EQL with an sql-like syntax.
- Includes a scripting interpreter to define alternative actions for database operations or writing backend logic.
- Written in Go from scratch. Only uses gorilla/websocket to support websockets for GraphQL subscriptions.
- The database can be embedded or used as a standalone application.
- When used as a standalone application it comes with an internal HTTPS webserver which provides user management, a REST API and a basic file server.
- When used as an embedded database it supports transactions with rollbacks, iteration of data and rule based consistency management.

Getting Started (standalone application)
----------------------------------------
You can download a pre-compiled package for Windows (win64) or Linux (amd64) [here](https://void.devt.de/pub/eliasdb).

Extract it and execute the executable with:
```
eliasdb server
```
The executable should automatically create 3 subfolders and a configuration file. It should start an HTTPS server on port 9090. To see a terminal point your webbrowser to:
```
https://localhost:9090/db/term.html
```
After accepting the self-signed certificate from the server you should see a web terminal. EliasDB can be stopped with a simple CTRL+C or by overwriting the content in eliasdb.lck with a single character.

Getting Started (docker image)
------------------------------
You can pull the latest docker image of EliasDB from [Dockerhub](https://hub.docker.com/r/krotik/eliasdb):
```
docker pull krotik/eliasdb
```

Create an empty directory, change into it and run the following to start the server:
```
docker run --user $(id -u):$(id -g) -v $PWD:/data -p 9090:9090 krotik/eliasdb server
```
This exposes port 9090 from the container on the local machine. All runtime related files are written to the current directory as the current user/group.

Connect to the running server with a console by running:
```
docker run --rm --network="host" -it -v $PWD:/data --user $(id -u):$(id -g) -v $PWD:/data krotik/eliasdb console
```

### Tutorial:

To get an idea of what EliasDB is about have a look at the [tutorial](examples/tutorial/doc/tutorial.md). This tutorial will cover the basics of EQL and show how data is organized.

There is a separate [tutorial](examples/tutorial/doc/tutorial_graphql.md) on using ELiasDB with GraphQL.

### REST API:

The terminal uses a REST API to communicate with the backend. The REST API can be browsed using a dynamically generated swagger.json definition (https://localhost:9090/db/swagger.json). You can browse the API of EliasDB's latest version [here](http://petstore.swagger.io/?url=https://devt.de/krotik/eliasdb/raw/master/swagger.json).

### Scripting

EliasDB supports a scripting language called [ECAL](ecal.md) to define alternative actions for database operations such as store, update or delete. The actions can be taken before, instead (by calling `db.raiseGraphEventHandled()`) or after the normal database operation. The language is powerful enough to write backend logic for applications.

There is a [VSCode integration](https://devt.de/krotik/ecal/src/master/ecal-support/README.md) available which supports syntax highlighting and debugging via the debug server. More information can be found in the [code repository](https://devt.de/krotik/ecal) of the interpreter.

### Clustering:

EliasDB supports to be run in a cluster by joining multiple instances of EliasDB together. You can read more about it [here](cluster.md).

### Command line options
The main EliasDB executable has two main tools:
```
Usage of ./eliasdb <tool>

EliasDB graph based database

Available commands:

    console   EliasDB server console
    server    Start EliasDB server
```
The most important one is server which starts the database server. The server has several options:
```
Usage of ./eliasdb server [options]

  -export string
    	Export the current database to a zip file
  -help
    	Show this help message
  -import string
    	Import a database from a zip file
  -no-serv
    	Do not start the server after initialization
```
If the `EnableECALScripts` configuration option is set the following additional option is available:
```
-ecal-console
    Start an interactive interpreter console for ECAL
```
The interactive console can be used to inspect and modify the runtime state of the ECAL interpreter.

Once the server is started the console tool can be used to interact with the server. The options of the console tool are:
```
Usage of ./eliasdb console [options]

  -exec string
    	Execute a single line and exit
  -file string
    	Read commands from a file and exit
  -help
    	Show this help message
  -host string
    	Host of the EliasDB server (default "localhost")
  -port string
    	Port of the EliasDB server (default "9090")
```
On the console type 'q' to exit and 'help' to get an overview of available commands:
```
Command Description
export  Exports the last output.
find    Do a full-text search of the database.
help    Display descriptions for all available commands.
info    Returns general database information.
part    Displays or sets the current partition.
ver     Displays server version information.
```
It is also possible to directly run EQL and GraphQL queries on the console. Use the arrow keys to cycle through the command history.

### Configuration
EliasDB uses a single configuration file called eliasdb.config.json. After starting EliasDB for the first time it should create a default configuration file. Available configurations are:

| Configuration Option | Description |
| --- | --- |
| ClusterConfigFile | Cluster configuration file. |
| ClusterLogHistory | File which is used to store the console history. |
| ClusterStateInfoFile | File which is used to store the cluster state. |
| CookieMaxAgeSeconds | Lifetime for cookies used by EliasDB. |
| ECALDebugServerHost | Hostname the ECAL debug server should listen to. |
| ECALDebugServerPort | Port on which the debug server should listen on. |
| ECALEntryScript | Entry script for ECAL interpreter. |
| ECALLogFile | Logfile for ECAL interpreter. An empty string will cause the logger to write to the console. |
| ECALLogLevel | Log level for ECAL interpreter. Can be debug, info or error. |
| ECALScriptFolder | Directory for ECAL scripts. |
| ECALWorkerCount | Number of worker threads in the ECA engine's thread pool. |
| EnableAccessControl | Flag if access control for EliasDB should be enabled. This provides user authentication and authorization features. |
| EnableCluster | Flag if EliasDB clustering support should be enabled. EXPERIMENTAL! |
| EnableClusterTerminal | Flag if the cluster terminal file /web/db/cluster.html should be created. |
| EnableECALDebugServer | Flag if the ECAL debug server should be started. Note: This will slow ECAL performance significantly. |
| EnableECALScripts | Flag if ECAL scripts should be executed on startup. |
| EnableReadOnly | Flag if the datastore should be open read-only. |
| EnableWebFolder | Flag if the files in the webfolder /web should be served up by the webserver. If false only the REST API is accessible. |
| EnableWebTerminal | Flag if the web terminal file /web/db/term.html should be created. |
| HTTPSCertificate | Name of the webserver certificate which should be used. A new one is created if it does not exist. |
| HTTPSHost | Hostname the webserver should listen to. This host is also used in the dynamically generated swagger definition. |
| HTTPSKey | Name of the webserver private key which should be used. A new one is created if it does not exist. |
| HTTPSPort | Port on which the webserver should listen on. |
| LocationAccessDB | File which is used to store access control information. This file can be edited while the server is running and changes will be picked up immediately. |
| LocationDatastore | Directory for datastore files. |
| LocationHTTPS | Directory for the webserver's SSL related files. |
| LocationUserDB | File which is used to store (hashed) user passwords. |
| LocationWebFolder | Directory of the webserver's webfolder. |
| LockFile | Lockfile for the webserver which will be watched duing runtime. Replacing the content of this file with a single character will shutdown the webserver gracefully. |
| MemoryOnlyStorage | Flag if the datastore should only be kept in memory. |
| ResultCacheMaxAgeSeconds | EQL queries create result sets which are cached. The value describes the amount of time in seconds a result is kept in the cache. |
| ResultCacheMaxSize | EQL queries create result sets which are cached. The value describes the number of results which can be kept in the cache. |

Note: It is not (and will never be) possible to access the REST API via HTTP.

Enabling Access Control
-----------------------
It is possible to enforce access control by enabling the `EnableAccessControl` configuration option. When started with enabled access control EliasDB will only allow known users to connect. Users must authenticate with a password before connecting to the web interface or the REST API. On the first start with the flag enabled the following users are created by default:

|Username|Default Password|Groups|Description|
|---|---|---|---|
|elias|elias|admin/public|Default Admin|
|johndoe|doe|public|Default unprivileged user|

Users can be managed from the console. Please do either delete the default users or change their password after starting EliasDB.

Users are organized in groups and permissions are assigned to groups. Permissions are given to endpoints of the REST API. The following permissions are available:

|Type|Allowed HTTP Request Type|Description|
|---|---|---|
|Create|Post|Creating new data|
|Read|Get|Read data|
|Update|Put|Modify existing data|
|Delete|Delete|Delete data|

The default group permissions are:

|Group|Path|Permissions|
|---|---|---|
|admin|/db/*|`CRUD`|
|public|/|`-R--`|
||/css/*|`-R--`|
||/db/*|`-R--`|
||/img/*|`-R--`|
||/js/*|`-R--`|
||/vendor/*|`-R--`|


Building EliasDB
----------------
To build EliasDB from source you need to have Go installed (go >= 1.12):

- Create a directory, change into it and run:
```
git clone https://devt.de/krotik/eliasdb/ .
```

- You can build EliasDB's executable with:
```
go build cli/eliasdb.go
```

Building EliasDB as Docker image
--------------------------------
EliasDB can be build as a secure and compact Docker image.

- Create a directory, change into it and run:
```
git clone https://devt.de/krotik/eliasdb/ .
```

- You can now build the Docker image with:
```
docker build --tag krotik/eliasdb .
```

Example Applications
--------------------
- [Chat](examples/chat/doc/chat.md) - A simple chat application showing node modification via ECAL script, user management and subscriptions.
- [Data-mining](examples/data-mining/doc/data-mining.md) - A more complex application which uses the cluster feature of EliasDB and GraphQL for data queries.
- [Game](examples/game/doc/game.md) - A multiplayer game example using ECAL for simulating the game scene in the backend.

Further Reading
---------------
- A design document which describes the different components of the graph database. [Link](eliasdb_design.md)
- A reference for EliasDB's custom query language EQL. [Link](eql.md)
- A reference for EliasDB's support for GraphQL. [Link](graphql.md)
- A quick overview of what you can do when you embed EliasDB in your own Go project. [Link](embedding.md)

License
-------
EliasDB source code is available under the [Mozilla Public License](/LICENSE).
