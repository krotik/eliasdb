/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
EliasDB is a graph based database which aims to provide a lightweight solution
for projects which want to store their data as a graph.

Features:

- Build on top of a fast key-value store which supports transactions and memory-only storage.

- Data is stored in nodes (key-value objects) which are connected via edges.

- Stored graphs can be separated via partitions.

- Stored graphs support cascading deletions - delete one node and all its "children".

- All stored data is indexed and can be quickly searched via a full text phrase search.

- For more complex queries EliasDB has an own query language called EQL with an sql-like syntax.

- Written in Go from scratch. No third party libraries were used apart from Go's standard library.

- The database can be embedded or used as a standalone application.

- When used as a standalone application it comes with an internal HTTPS webserver which provides a REST API and a basic file server.

- When used as an embedded database it supports transactions with rollbacks, iteration of data and rule based consistency management.
*/

package main

import (
	"archive/zip"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"devt.de/common/errorutil"
	"devt.de/common/fileutil"
	"devt.de/common/termutil"
	"devt.de/eliasdb/config"
	"devt.de/eliasdb/console"
	"devt.de/eliasdb/graph"
	"devt.de/eliasdb/server"
	"devt.de/eliasdb/version"
)

func main() {

	// Initialize the default command line parser

	flag.CommandLine.Init(os.Args[0], flag.ContinueOnError)

	// Define default usage message

	flag.Usage = func() {

		// Print usage for tool selection

		fmt.Println(fmt.Sprintf("Usage of %s <tool>", os.Args[0]))
		fmt.Println()
		fmt.Println("EliasDB graph based database")
		fmt.Println()
		fmt.Println("Available commands:")
		fmt.Println()
		fmt.Println("    console   EliasDB server console")
		fmt.Println("    server    Start EliasDB server")
		fmt.Println()
		fmt.Println(fmt.Sprintf("Use %s <command> -help for more information about a given command.", os.Args[0]))
		fmt.Println()
	}

	// Parse the command bit

	err := flag.CommandLine.Parse(os.Args[1:])

	if len(flag.Args()) > 0 {

		arg := flag.Args()[0]

		if arg == "server" {
			config.LoadConfigFile(config.DefaultConfigFile)
			server.StartServerWithSingleOp(handleServerCommandLine)
		} else if arg == "console" {
			config.LoadConfigFile(config.DefaultConfigFile)
			RunCliConsole()
		} else {
			flag.Usage()
		}

	} else if err == nil {

		flag.Usage()
	}
}

/*
RunCliConsole runs the server console on the commandline.
*/
func RunCliConsole() {
	var err error

	// Try to get the server host and port from the config file

	chost, cport := getHostPortFromConfig()

	host := flag.String("host", chost, "Host of the EliasDB server")
	port := flag.String("port", cport, "Port of the EliasDB server")

	cmdfile := flag.String("file", "", "Read commands from a file and exit")
	cmdline := flag.String("exec", "", "Execute a single line and exit")

	showHelp := flag.Bool("help", false, "Show this help message")

	flag.Usage = func() {
		fmt.Println()
		fmt.Println(fmt.Sprintf("Usage of %s console [options]", os.Args[0]))
		fmt.Println()
		flag.PrintDefaults()
		fmt.Println()
	}

	flag.CommandLine.Parse(os.Args[2:])

	if *showHelp {
		flag.Usage()
		return
	}

	if *cmdfile == "" && *cmdline == "" {
		fmt.Println(fmt.Sprintf("EliasDB %v.%v - Console",
			version.VERSION, version.REV))
	}

	var clt termutil.ConsoleLineTerminal

	isExitLine := func(s string) bool {
		return s == "exit" || s == "q" || s == "quit" || s == "bye" || s == "\x04"
	}

	clt, err = termutil.NewConsoleLineTerminal(os.Stdout)

	if *cmdfile != "" {
		var file *os.File

		// Read commands from a file

		file, err = os.Open(*cmdfile)
		if err == nil {
			defer file.Close()

			clt, err = termutil.AddFileReadingWrapper(clt, file, true)
		}

	} else if *cmdline != "" {
		var buf bytes.Buffer

		buf.WriteString(fmt.Sprintln(*cmdline))

		// Read commands from a single line

		clt, err = termutil.AddFileReadingWrapper(clt, &buf, true)

	} else {

		// Add history functionality

		histfile := filepath.Join(filepath.Dir(os.Args[0]), ".eliasdb_console_history")
		clt, err = termutil.AddHistoryMixin(clt, histfile,
			func(s string) bool {
				return isExitLine(s)
			})
	}

	if err == nil {

		// Create the console object

		con := console.NewConsole(fmt.Sprintf("https://%s:%s", *host, *port), os.Stdout,
			func() (string, string) {

				//  Login function

				line, err := clt.NextLinePrompt("Login username: ", 0x0)
				user := strings.TrimRight(line, "\r\n")
				errorutil.AssertOk(err)
				pass, err := clt.NextLinePrompt("Password: ", '*')
				errorutil.AssertOk(err)
				return user, pass
			},
			func() string {

				// Enter password function

				var err error
				var pass, pass2 string
				pass2 = "x"
				for pass != pass2 {
					pass, err = clt.NextLinePrompt("Password: ", '*')
					errorutil.AssertOk(err)
					pass2, err = clt.NextLinePrompt("Re-type password: ", '*')
					errorutil.AssertOk(err)
					if pass != pass2 {
						clt.WriteString(fmt.Sprintln("Passwords don't match"))
					}
				}
				return pass
			},
			func(args []string, exportBuf *bytes.Buffer) error {

				// Export data to a chosen file

				filename := "export.out"

				if len(args) > 0 {
					filename = args[0]
				}

				return ioutil.WriteFile(filename, exportBuf.Bytes(), 0666)
			})

		// Start the console

		if err = clt.StartTerm(); err == nil {
			var line string

			defer clt.StopTerm()

			if *cmdfile == "" && *cmdline == "" {
				fmt.Println("Type 'q' or 'quit' to exit the shell and '?' to get help")
			}

			line, err = clt.NextLine()
			for err == nil && !isExitLine(line) {

				_, cerr := con.Run(line)

				if cerr != nil {

					// Output any error

					fmt.Fprintln(clt, cerr.Error())
				}

				line, err = clt.NextLine()
			}
		}
	}

	if err != nil {
		fmt.Println(err.Error())
	}
}

/*
getHostPortFromConfig gets the host and port from the config file or the
default config.
*/
func getHostPortFromConfig() (string, string) {
	host := fileutil.ConfStr(config.DefaultConfig, config.HTTPSHost)
	port := fileutil.ConfStr(config.DefaultConfig, config.HTTPSPort)

	configFile := filepath.Join(filepath.Dir(os.Args[0]), config.DefaultConfigFile)
	if ok, _ := fileutil.PathExists(configFile); ok {
		cfg, _ := fileutil.LoadConfig(configFile, config.DefaultConfig)
		if cfg != nil {

			host = fileutil.ConfStr(cfg, config.HTTPSHost)
			port = fileutil.ConfStr(cfg, config.HTTPSPort)
		}
	}

	return host, port
}

/*
handleServerCommandLine handles all command line options for the server
*/
func handleServerCommandLine(gm *graph.Manager) bool {
	var err error

	importDb := flag.String("import", "", "Import a database from a zip file")
	exportDb := flag.String("export", "", "Export the current database to a zip file")

	noServ := flag.Bool("no-serv", false, "Do not start the server after initialization")

	showHelp := flag.Bool("help", false, "Show this help message")

	flag.Usage = func() {
		fmt.Println()
		fmt.Println(fmt.Sprintf("Usage of %s server [options]", os.Args[0]))
		fmt.Println()
		flag.PrintDefaults()
		fmt.Println()
	}

	flag.CommandLine.Parse(os.Args[2:])

	if *showHelp {
		flag.Usage()
		return true
	}

	if *importDb != "" {
		var zipFile *zip.ReadCloser

		fmt.Println("Importing from:", *importDb)

		if zipFile, err = zip.OpenReader(*importDb); err == nil {
			defer zipFile.Close()

			for _, file := range zipFile.File {
				var in io.Reader

				if !file.FileInfo().IsDir() {
					part := strings.TrimSuffix(filepath.Base(file.Name), filepath.Ext(file.Name))
					fmt.Println(fmt.Sprintf("Importing %s to partition %s", file.Name, part))

					if in, err = file.Open(); err == nil {
						err = graph.ImportPartition(in, part, gm)
					}

					if err != nil {
						break
					}
				}
			}
		}
	}

	if *exportDb != "" {
		var zipFile *os.File

		fmt.Println("Exporting to:", *exportDb)

		if zipFile, err = os.Create(*exportDb); err == nil {
			defer zipFile.Close()

			zipWriter := zip.NewWriter(zipFile)
			defer zipWriter.Close()

			for _, part := range gm.Partitions() {
				var exportFile io.Writer

				name := fmt.Sprintf("%s.json", part)

				fmt.Println(fmt.Sprintf("Exporting partition %s to %s", part, name))

				if exportFile, err = zipWriter.Create(name); err == nil {
					err = graph.ExportPartition(exportFile, part, gm)
				}

				if err != nil {
					break
				}
			}
		}
	}

	if err != nil {
		fmt.Println(err.Error())
		return true
	}

	return *noServ
}
