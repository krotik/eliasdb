/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package ecal

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	"devt.de/krotik/common/datautil"
	"devt.de/krotik/common/fileutil"
	"devt.de/krotik/common/stringutil"
	"devt.de/krotik/ecal/cli/tool"
	ecalconfig "devt.de/krotik/ecal/config"
	"devt.de/krotik/ecal/engine"
	"devt.de/krotik/ecal/scope"
	"devt.de/krotik/ecal/stdlib"
	"devt.de/krotik/ecal/util"
	"devt.de/krotik/eliasdb/config"
	"devt.de/krotik/eliasdb/ecal/dbfunc"
	"devt.de/krotik/eliasdb/graph"
)

/*
ScriptingInterpreter models a ECAL script interpreter instance.
*/
type ScriptingInterpreter struct {
	GM          *graph.Manager       // GraphManager for the interpreter
	Interpreter *tool.CLIInterpreter // ECAL Interpreter object

	Dir       string // Root dir for interpreter
	EntryFile string // Entry file for the program
	LogLevel  string // Log level string (Debug, Info, Error)
	LogFile   string // Logfile (blank for stdout)

	RunDebugServer  bool   // Run a debug server
	DebugServerHost string // Debug server host
	DebugServerPort string // Debug server port

	WebsocketConnections *datautil.MapCache
}

/*
NewScriptingInterpreter returns a new ECAL scripting interpreter.
*/
func NewScriptingInterpreter(scriptFolder string, gm *graph.Manager) *ScriptingInterpreter {
	return &ScriptingInterpreter{
		GM:                   gm,
		Dir:                  scriptFolder,
		EntryFile:            filepath.Join(scriptFolder, config.Str(config.ECALEntryScript)),
		LogLevel:             config.Str(config.ECALLogLevel),
		LogFile:              config.Str(config.ECALLogFile),
		RunDebugServer:       config.Bool(config.EnableECALDebugServer),
		DebugServerHost:      config.Str(config.ECALDebugServerHost),
		DebugServerPort:      config.Str(config.ECALDebugServerPort),
		WebsocketConnections: datautil.NewMapCache(5000, 0),
	}
}

/*
dummyEntryFile is a small valid ECAL which does not do anything. It is used
as the default entry file if no entry file exists.
*/
const dummyEntryFile = `0 # Write your ECAL code here
`

/*
Run runs the ECAL scripting interpreter.

After this function completes:
- EntryScript in config and all related scripts in the interpreter root dir have been executed
- ECAL Interpreter object is fully initialized
- A debug server might be running which can reload the entry script
- ECAL's event processor has been started
- GraphManager events are being forwarded to ECAL
*/
func (si *ScriptingInterpreter) Run() error {
	var err error

	// Ensure we have a dummy entry point

	if ok, _ := fileutil.PathExists(si.EntryFile); !ok {
		err = ioutil.WriteFile(si.EntryFile, []byte(dummyEntryFile), 0600)
	}

	if err == nil {
		i := tool.NewCLIInterpreter()
		si.Interpreter = i

		// Set worker count in ecal config

		ecalconfig.Config[ecalconfig.WorkerCount] = config.Config[config.ECALWorkerCount]

		i.Dir = &si.Dir
		i.LogFile = &si.LogFile
		i.LogLevel = &si.LogLevel

		i.EntryFile = si.EntryFile
		i.LoadPlugins = true

		i.CreateRuntimeProvider("eliasdb-runtime")

		// Adding functions

		AddEliasDBStdlibFunctions(si.GM)

		// Adding rules

		sockRule := &engine.Rule{
			Name:            "EliasDB-websocket-communication-rule", // Name
			Desc:            "Handles a websocket communication",    // Description
			KindMatch:       []string{"db.web.sock.msg"},            // Kind match
			ScopeMatch:      []string{},
			StateMatch:      nil,
			Priority:        0,
			SuppressionList: nil,
			Action:          si.HandleECALSockEvent,
		}

		si.Interpreter.CustomRules = append(si.Interpreter.CustomRules, sockRule)

		if err == nil {

			if si.RunDebugServer {
				di := tool.NewCLIDebugInterpreter(i)

				addr := fmt.Sprintf("%v:%v", si.DebugServerHost, si.DebugServerPort)
				di.DebugServerAddr = &addr
				di.RunDebugServer = &si.RunDebugServer
				falseFlag := false
				di.EchoDebugServer = &falseFlag
				di.Interactive = &falseFlag
				di.BreakOnStart = &falseFlag
				di.BreakOnError = &falseFlag

				err = di.Interpret()

			} else {

				err = i.Interpret(false)
			}

			// EliasDB graph events are now forwarded to ECAL via the eventbridge.

			si.GM.SetGraphRule(&EventBridge{
				Processor: i.RuntimeProvider.Processor,
				Logger:    i.RuntimeProvider.Logger,
			})
		}
	}

	// Include a traceback if possible

	if ss, ok := err.(util.TraceableRuntimeError); ok {
		err = fmt.Errorf("%v\n  %v", err.Error(), strings.Join(ss.GetTraceString(), "\n  "))
	}

	return err
}

/*
RegisterECALSock registers a websocket which should be connected to ECAL events.
*/
func (si *ScriptingInterpreter) RegisterECALSock(conn *WebsocketConnection) {
	si.WebsocketConnections.Put(conn.CommID, conn)
}

/*
DeregisterECALSock removes a registered websocket.
*/
func (si *ScriptingInterpreter) DeregisterECALSock(conn *WebsocketConnection) {
	si.WebsocketConnections.Remove(conn.CommID)
}

/*
HandleECALSockEvent handles websocket events from the ECAL interpreter (db.web.sock.msg events).
*/
func (si *ScriptingInterpreter) HandleECALSockEvent(p engine.Processor, m engine.Monitor, e *engine.Event, tid uint64) error {
	state := e.State()
	payload := scope.ConvertECALToJSONObject(state["payload"])
	shouldClose := stringutil.IsTrueValue(fmt.Sprint(state["close"]))

	id := "null"
	if commID, ok := state["commID"]; ok {
		id = fmt.Sprint(commID)
	}

	err := fmt.Errorf("Could not send data to unknown websocket - commID: %v", id)

	if conn, ok := si.WebsocketConnections.Get(id); ok {
		err = nil
		wconn := conn.(*WebsocketConnection)
		wconn.WriteData(map[string]interface{}{
			"commID":  id,
			"payload": payload,
			"close":   shouldClose,
		})

		if shouldClose {
			wconn.Close("")
		}
	}

	return err
}

/*
AddEliasDBStdlibFunctions adds EliasDB related ECAL stdlib functions.
*/
func AddEliasDBStdlibFunctions(gm *graph.Manager) {
	stdlib.AddStdlibPkg("db", "EliasDB related functions")

	stdlib.AddStdlibFunc("db", "storeNode", &dbfunc.StoreNodeFunc{GM: gm})
	stdlib.AddStdlibFunc("db", "updateNode", &dbfunc.UpdateNodeFunc{GM: gm})
	stdlib.AddStdlibFunc("db", "removeNode", &dbfunc.RemoveNodeFunc{GM: gm})
	stdlib.AddStdlibFunc("db", "fetchNode", &dbfunc.FetchNodeFunc{GM: gm})
	stdlib.AddStdlibFunc("db", "storeEdge", &dbfunc.StoreEdgeFunc{GM: gm})
	stdlib.AddStdlibFunc("db", "removeEdge", &dbfunc.RemoveEdgeFunc{GM: gm})
	stdlib.AddStdlibFunc("db", "fetchEdge", &dbfunc.FetchEdgeFunc{GM: gm})
	stdlib.AddStdlibFunc("db", "traverse", &dbfunc.TraverseFunc{GM: gm})
	stdlib.AddStdlibFunc("db", "newTrans", &dbfunc.NewTransFunc{GM: gm})
	stdlib.AddStdlibFunc("db", "newRollingTrans", &dbfunc.NewRollingTransFunc{GM: gm})
	stdlib.AddStdlibFunc("db", "commit", &dbfunc.CommitTransFunc{GM: gm})
	stdlib.AddStdlibFunc("db", "query", &dbfunc.QueryFunc{GM: gm})
	stdlib.AddStdlibFunc("db", "graphQL", &dbfunc.GraphQLFunc{GM: gm})
	stdlib.AddStdlibFunc("db", "raiseGraphEventHandled", &dbfunc.RaiseGraphEventHandledFunc{})
	stdlib.AddStdlibFunc("db", "raiseWebEventHandled", &dbfunc.RaiseWebEventHandledFunc{})

}
