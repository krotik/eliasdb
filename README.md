EliasDB
=======
EliasDB is a graph-based database which aims to provide a lightweight solution for projects which want to store their data as a graph. EliasDB does not require any third-party libraries.

<p>
<a href="https://devt.de/build_status.html"><img src="https://devt.de/nightly/build.eliasdb.svg" alt="Build status"></a>
<a href="https://devt.de/nightly/test.eliasdb.html"><img src="https://devt.de/nightly/test.eliasdb.svg" alt="Code coverage"></a>
<a href="https://goreportcard.com/report/github.com/krotik/eliasdb">
<img src="https://goreportcard.com/badge/github.com/krotik/eliasdb?style=flat-square" alt="Go Report Card"></a>
<a href="http://devt.de/docs/pkg/devt.de/eliasdb/">
<img src="https://devt.de/nightly/godoc_badge.svg" alt="Go Doc"></a>
<a href="https://gitter.im/eliasdb/Lobby">
<img src="https://badges.gitter.im/gitterHQ/gitter.svg" alt="Gitter Chat"></a>
</p>

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
You can download a precompiled package for Windows (win64) or Linux (amd64) [here](https://devt.de/build_status.html).

Extract it and execute the executable. The executable should automatically create 3 subfolders and a configuration file. It should start an HTTPS server on port 9090. To see a terminal point your webbrowser to:
```
https://localhost:9090/db/term.html
```
After accepting the self-signed certificate from the server you should see a web terminal. EliasDB can be stopped with a simple CTRL+C or by overwriting the content in eliasdb.lck with a single character.

### Tutorial:

To get an idea of what EliasDB is about have a look at the [tutorial](/examples/tutorial/doc/tutorial.md).

### REST API:

The terminal uses a REST API to communicate with the backend. The REST API can be browsed using a dynamically generated swagger.json definition (https://localhost:9090/db/swagger.json). You can browse the API of EliasDB's latest version [here](http://petstore.swagger.io/?url=https://raw.githubusercontent.com/krotik/eliasdb/master/doc/swagger.json#/default).

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
It is also possible to directly run EQL queries on the console. Use the arrow keys to cycle through the command history.

### Configuration
EliasDB uses a single configuration file called eliasdb.config.json. After starting EliasDB for the first time it should create a default configuration file. Available configurations are:

| Configuration Option | Description |
| --- | --- |
| ClusterConfigFile | Cluster configuration file. |
| ClusterLogHistory | File which is used to store the console history. |
| ClusterStateInfoFile | File which is used to store the cluster state. |
| CookieMaxAgeSeconds | Lifetime for cookies used by EliasDB. |
| EnableAccessControl | Flag if access control for EliasDB should be enabled. This provides user authentication and authorization features. |
| EnableCluster | Flag if EliasDB clustering support should be enabled. EXPERIMENTAL! |
| EnableClusterTerminal | Flag if the cluster terminal file /web/db/cluster.html should be created. |
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

Building EliasDB
----------------
To build EliasDB from source you need to have Go installed. There a are two options:

### Checkout from github (use this method if you want code + documentation and tutorials):

Create a directory, change into it and run:
```
git clone https://github.com/krotik/eliasdb/ .
```

Assuming your GOPATH is set to the new directory you should be able to build the binary with:
```
go install devt.de/eliasdb/cli
```

### Using go get (use this method if you want to embed EliasDB in your project):

Create a directory, change into it and run:
```
go get devt.de/common/... devt.de/eliasdb/...
```

Assuming your GOPATH is set to the new directory you should be able to build the binary with:
```
go build devt.de/eliasdb/cli
```

Further Reading
---------------
- A design document which describes the different components of the graph database. [Link](/doc/elias_db_design.md)
- A reference for the EliasDB query language EQL. [Link](/doc/eql.md)
- A quick overview of what you can do when you embed EliasDB in your own Go project. [Link](/doc/embedding.md)

License
-------
EliasDB source code is available under the [Mozilla Public License](/LICENSE).
