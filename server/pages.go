/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package server

/*
LoginSRC is a simple login page.
*/
const LoginSRC = `
<!DOCTYPE html>
<html>
<head>
    <title>EliasDB Login</title>

    <meta name="viewport" content="width=device-width, initial-scale=1">

    <style>
        
        body, html {
            height: 100%;
        }

        #wrapper {
            position:absolute;

		   /* Uncomment this if you have a fancy background image
            background-image: url("/img/bg/bg.jpg");
            background-position: center;
            background-repeat: no-repeat;
            background-size: cover;
		   */

            height: 100%;
            width:100%;
        }

        #login-panel {
            text-align: center;
            padding : 2em;
            max-width : 40em;
            width : 35%;
            min-width : 20em;
            margin: 15% auto;
            background: #ffffff;
            border-radius: 5px;
        }

        #login-panel .form-input, #login-panel h4 {
            margin-bottom: 1em;
        }

        #login-panel .input-icon {
            position: absolute;
            margin: 0.8em;
            display: inline-block;
            width: 1.2em;
            z-index: 1;
        }

        #login-panel .form-input {
            padding-left: 2.5em;
            display: inline-block;
        }

        #login-panel .btn {
            width: 30%;
        }

    </style>
</head>
<body>
    <div id="wrapper">
        <div id="login-panel">
            <h4>Login</h4>
            <form action="/db/login/" method="post">
                <input class="form-input input-lg" name="user" placeholder="Enter Login" autofocus><br>
                <input class="form-input input-lg" name="pass" type="password" placeholder="Enter Password"><br>

                <input class="btn btn-primary" type="submit" value="Login">
            </form>
        </div>
    </div>
</body>
</html>
`

/*
TermSRC is the terminal HTML as a text blob.
*/
const TermSRC = `
<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Terminal</title>
    <style>
        body {
            background: #fff;
            font-family: 'verdana';
            font-size: 11px;
            margin: 0;
            min-width: 320px;
        }

        .t-header {
            background: linear-gradient(#000, #444);
            color: #fff;
            font-weight: bold;
            padding: 0 1em;
            margin: 0 0 1em 0;
            box-shadow: 3px 3px 3px rgba(50, 50, 50, 0.25);
        }

        .t-header h1 {
            display: inline-block;
            font-size: 18px;
            margin: 3px 0;
        }

        .t-terms {
            padding: 0 10%;
            display: inline-block;
            width: 80%
        }

        .t-terms .t-term {
            background: #EEEEEE;
            padding: 10px;
            border: #000000 2px solid;
            border-radius: 10px;
            margin: 3em 0 0 0;
        }

        .t-terms .t-term.t-input-term:hover {
            border-color: #888888;
        }

        .t-terms .t-term.t-output-term {
            white-space: pre-wrap;
            font-family: "Lucida Console", "Courier";
            font-size: 11px;
        }

        .t-terms .t-term.t-output-term.t-error {
            background: #FFBBBB;
        }

        .t-terms .t-term.t-output-term.t-normal {
            background: #B3D9FF;
        }

        .t-terms .t-term .t-result-table {
            width: 100%;
            border-spacing: 0;
            border-collapse: collapse;
        }

        .t-terms .t-term .t-result-table th {
            text-align: left;
            border-style: none none solid none;
        }

        .t-terms .t-term .t-result-table td {
            padding: 5px;
        }

        .t-terms .t-term .t-prompt {
            float: left;
            width: 15px;
            font-weight: bold;
        }

        .t-terms .t-term .t-input {
            display: inline-block;
            width: calc(100% - 15px);
            height: 10px;
            outline: none;
        }

        .t-terms .t-term .t-button {
            background: #EEEEEE;
            float: right;
            border: #000000 2px solid;
            margin: 2px;
            border-radius: 10px;
            font-weight: bold;
        }

        .t-terms .t-term .t-button:hover {
            color: #888888;
            border-color: #888888;
        }

    </style>
  </head>
  <body onload="t.main.init()">

    <div class="t-header"><h1 id="name"></h1> <h1 id="version"></h1></div>
    <div id="terms" class="t-terms"></div>

    <script>

        // Utility functions
        // =================

        if (t === undefined) {
          var t = {};
        }

        t.$ = function(id) { "use strict"; return document.getElementById(id); };
        t.copyObject = function (o1, o2) { "use strict"; for (var attr in o1) { o2[attr] = o1[attr]; } };
        t.mergeObject = function (o1, o2) { "use strict"; for (var attr in o1) { if(o2[attr] === undefined) { o2[attr] = o1[attr]; } } };
        t.cloneObject = function (o) { "use strict"; var r = {}; ge.copyObject(o, r); return r; };
        t.esc = function (str) { "use strict"; return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;' ); };
        t.unesc = function (str) { "use strict"; return str.stripTags().replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&amp;/g,'&'); };
        t.insert = function(element, child) { "use strict"; element.appendChild(child); return element; };
        t.insertBefore = function(element, child) { "use strict"; element.parentNode.insertBefore(child, element); };
        t.insertAfter = function(element, child) { "use strict"; element.parentNode.insertBefore(child, element.nextSibling); };
        t.create = function(tag, attrs) {
            "use strict";
            var element = document.createElement(tag);
            if (attrs !== undefined) {
                t.getObjectKeys(attrs).forEach(function (v) {
                    element.setAttribute(v, attrs[v]);
                });
            }
            return element;
        };
        t.getObjectKeys = function (obj) {
            "use strict";
            var key, keys = [];
            for(key in obj) {
                if (obj.hasOwnProperty(key)) {
                    keys.push(key);
                }
            }
            return keys;
        };
        t.getObjectValues = function (obj) {
            "use strict";
            var key, values = [];
            for(key in obj) {
                if (obj.hasOwnProperty(key)) {
                    values.push(obj[key]);
                }
            }
            return values;
        };
        t.addEvent = function (element, eventName, func) {
            "use strict";
            if (element.addEventListener) {
                element.addEventListener(eventName, func, false);
            } else if (element.attachEvent) {
                element.attachEvent('on' + eventName, func);
            }
        };
        t.stopBubbleEvent = function (e) {
            "use strict";
            e = e ? e:window.event;
            if (e.stopPropagation) {
                e.stopPropagation();
            }
            if (e.cancelBubble !== null) {
                e.cancelBubble = true;
            }
            if (e.preventDefault) {
                e.preventDefault();
            }
        };
        t.bind = function () {
            "use strict";
            var f = arguments[0], t = Array.prototype.slice.call(arguments, 1);
            var a = t.splice(1);
            return function() {
                "use strict";
                return f.apply(t[0],
                               a.concat(Array.prototype.slice.call(arguments, 0)));
            }
        };
        t.ajax = function (url, method, body, callbackOK, callbackError) {
            "use strict";
            var http = new XMLHttpRequest(),
                start;

            if (method === undefined) {
                method = "GET"
            }

            http.open(method, url, true);
            http.setRequestHeader("content-type", "application/json");
            http.setRequestHeader("accept", "application/json");
            http.onload = function () {
                var rtime = Date.now() - start;
                document.title = "Terminal (Last response time: "+ rtime +"ms)";
                console.log("Response time:", rtime);
                try {
                    if (http.status === 200) {
                        if (callbackOK) {
                            if (http.response !== "") {
                                callbackOK(JSON.parse(http.response));
                            } else {
                                callbackOK();
                            }
                        }
                    } else {
                        if (callbackError) {
                           callbackError(http.response);
                        } else {
                            console.log(http.response);
                        }
                    }
                } catch(e) {
                    console.log("Ajax call failed - exception:", e);
                }
            };

            start = Date.now();

            if (body !== undefined) {
                http.send(JSON.stringify(body));
            } else {
                http.send();
            }
        };

        // Global variables
        // ================

        t.ajaxPrefix = "/db";
        t.partition = "main";

        // Console
        // =======

        t.main = {

            init : function() {
                "use strict";

                console.log("Init console")

                t.ajax(t.ajaxPrefix + "/about/", "GET", undefined, function (r) {
                    t.$("name").innerHTML = t.esc(r.product);
                    t.$("version").innerHTML = t.esc(r.version);

                    t.main.addPrompt();
                });
            },

            // Add a new prompt element.
            //
            addPrompt : function () {
                "use strict";
                var input = t.create("div", {
                    "contenteditable" : "true",
                    "class" : "t-input"
                }),
                term = t.create("div", {
                    "class" : "t-term t-input-term"
                }),
                prompt = t.create("div", {
                    "class" : "t-prompt"
                }),
                buttonOK = t.create("button", {
                    "class" : "t-button",
                }),
                buttonClear = t.create("button", {
                    "class" : "t-button",
                }),
                stripText = function (t) {

                    // Strip out unwanted html tags

                    t = t.replace(/<(?:.|\n)*?>/g, '');
                    
                    // Convert sansitised html to text
                    
                    t = t.replace(/&gt;/g, '>');
                    t = t.replace(/&lt;/g, '<');
                    t = t.replace(/&quot;/g, '"');
                    t = t.replace(/&apos;/g, "'");
                    t = t.replace(/&amp;/g, '&');
                    t = t.replace(/&nbsp;/g, ' ');

                    return t
                };

                buttonOK.innerHTML    = "Ok";
                buttonClear.innerHTML = "Clear";
                prompt.innerHTML      = "&gt;"

                t.insert(term, prompt)
                t.insert(term, input);
                t.insert(term, buttonOK);
                t.insert(term, buttonClear);

                t.insert(t.$("terms"), term);

                input.focus();

                t.addEvent(input, "keydown", function (e) {

                    // Shift+Return pressed

                    if (e.shiftKey && e.keyCode === 13) {
                        t.stopBubbleEvent(e);
                        t.main.exec(input, stripText(input.innerHTML));
                    }
                });

                t.addEvent(term, "click", function (e) {
                    input.focus();
                });

                t.addEvent(term, "drop", function (e) {
                    e.stopPropagation();
                    e.preventDefault();

                    var files = e.dataTransfer.files;
                    for (var i = 0, f; f = files[i]; i++) {

                        var reader = new FileReader();

                        reader.onload = function(content) {
                            input.innerHTML += t.esc(content.target.result);
                        }

                        reader.readAsText(f);
                    }
                });

                t.addEvent(term, "dragover", function (e) {
                    e.stopPropagation();
                    e.preventDefault();
                    e.dataTransfer.dropEffect = 'copy';
                });

                t.addEvent(buttonOK, "click", function (e) {
                    t.main.exec(input, stripText(input.innerHTML));
                });

                t.addEvent(buttonClear, "click", function (e) {
                    input.innerHTML = "";
                });
            },

            // Execute a command.
            //
            exec : function (element, text) {
                "use strict";

                text = text.trim();
                text = text.replace( /\n/g, " ");

                var sp   = text.split(" ", 1),
                    cmd  = sp[0].toLowerCase(),
                    rest = text.substring(sp[0].length);

                console.log("Execute:", cmd, rest);

                // After the cmd has been executed

                var cmdFunc = t.cmds[cmd];

                if (cmdFunc !== undefined) {
                    cmdFunc(element, rest);
                } else {
                    t.main.addError(element, "Unknown command: '" + cmd + "'");
                }
            },

            // Add an error message. Returns true if a new element was added.
            //
            addError : function (element, text) {
                "use strict";
                var ret = t.main._addTextTerm(element, text, "t-term t-output-term t-error");

                if (ret) {

                    // A new output was added we need a new prompt below it

                    t.main.addPrompt();
                }

                element.focus();
            },

            // Add a normal message. Returns true if a new element was added.
            //
            addOutput : function (element, text) {
                "use strict";
                var ret = t.main._addTextTerm(element, text, "t-term t-output-term t-normal");

                if (ret) {

                    // A new output was added we need a new prompt below it

                    t.main.addPrompt();
                }
            },

            // Add a new element which should just display text.
            //
            _addTextTerm : function (element, text, classes) {
                "use strict";
                var term = element._term,
                    addedNewElement = false;

                if (term === undefined) {

                    term = t.create("div", {
                        "class" : classes
                    });

                    element._term = term;
                    t.insert(t.$("terms"), term);
                    addedNewElement = true;

                } else {

                    term.setAttribute("class", classes);
                }

                t.main._showColorEffect(term);

                term.innerHTML = t.esc(text);

                return addedNewElement;
            },

            // Output a result table.
            //
            addTableOutput : function (element, tableObj) {
                "use strict";
                // Clear the output term

                t.main.addOutput(element, "");

                // Build up the table

                var term = element._term,
                    table = t.create("table", {
                        "class" : "t-result-table"
                    }),
                    tableHeader = t.create("tr");

                // Build up the header

                tableObj.header.labels.forEach(function (l) {
                    var cell = t.create("th");
                    cell.innerHTML = l;
                    t.insert(tableHeader, cell);
                });

                t.insert(table, tableHeader);

                // Build up the rows

                tableObj.rows.forEach(function (r) {
                    var row = t.create("tr");
                    t.insert(table, row);

                    r.forEach(function (c) {
                        var cell = t.create("td");

                        // Make sure nested structures are printed
                        // in a human readable fashion

                        if (typeof c === 'object') {
                            c = JSON.stringify(c);
                        }
            
                        cell.innerHTML = c;
                        t.insert(row, cell);
                    });
                });

                t.insert(term, table);
            },

            // Show a color effect on an element.
            //
            _showColorEffect : function (elem) {
                "use strict";
                var count = parseInt("AAAAAA", 16),
                    loop = function () {
                        elem.style.backgroundColor = "#" + count.toString(16);
                        count += parseInt("080808", 16);
                        if (count < parseInt("DDDDDD", 16)) {
                            window.setTimeout(loop, 30);
                        } else {
                            elem.style.backgroundColor = "";
                        }
                    };
                loop();
            },
        };

        t.cmds = {

            // Get the help text
            //
            "help" : function (element, data) {
                "use strict";

                data = data.replace(/^\s+|\s+$/gm, '');

                if (data === "") {
                    t.main.addOutput(element, "Available commands:\n\n" +
                                              "ver        - Returns product information\n" +
                                              "info       - Returns general datastore information\n" +
                                              "part       - Display / change the partition which is queried\n" +
                                              "get/lookup - Run a YQL query\n" +
                                              "find       - Do a fulltext search\n");
                    return;
                }
                else if (data === "get") {
                    t.main.addOutput(element, "Run a YQL query.\n\n" +
                              "A query can have the form: get <node kind> where <condition>\n");
                    return;
                }
                else if (data === "lookup") {
                    t.main.addOutput(element, "Run a YQL query.\n\n" +
                              'A query can have the form: lookup <node kind> "<node id>",... where <condition>\n');
                    return;
                }
                else if (data === "find") {
                    t.main.addOutput(element, "Do a fulltext search.\n\n" +
                              'Find a word or phrase in the datastore. This will query all node kinds in all visible partitions (partitions which don\'t start with a _ character');
                    return;
                }

                t.main.addOutput(element, "Unknown help topic: " + data);
            },

            // Get the "about" JSON
            //
            "ver" : function (element) {
                "use strict";

                t.ajax(t.ajaxPrefix + "/about/", "GET", undefined, function (r) {
                    t.main.addOutput(element, JSON.stringify(r, undefined, 4));
                });
            },

            // Get the "info" JSON
            //
            "info" : function (element) {
                "use strict";

                t.ajax(t.ajaxPrefix + "/v1/info/", "GET", undefined, function (r) {
                    t.main.addOutput(element, JSON.stringify(r, undefined, 4));
                });
            },

            // Get data in the datastore.
            //
            "get" : function (element, data) {
                "use strict";

                t.ajax(t.ajaxPrefix + "/v1/query/" + t.partition + "?q=get" + encodeURIComponent(data), "GET", undefined,
                    function (r) {
                        t.main.addTableOutput(element, r);
                    },
                    function (r) {
                        t.main.addError(element, r);
                    });
            },

            // Lookup data in the datastore.
            //
            "lookup" : function (element, data) {
                "use strict";
                t.ajax(t.ajaxPrefix + "/v1/query/" + t.partition + "?q=lookup" + encodeURIComponent(data), "GET", undefined,
                    function (r) {
                        t.main.addTableOutput(element, r);
                    },
                    function (r) {
                        t.main.addError(element, r);
                    });
            },

            // Lookup data in the datastore.
            //
            "find" : function (element, data) {
                "use strict";

                data = data.replace(/^\s+|\s+$/gm, ''); // Remove whitespace from beginning and end

                var phrase = data;

                console.log("p:", phrase);

                // Check all arguments were given
                
                if (!phrase) {
                    t.main.addError(element, "Find requires a search word / phrase");
                    return;
                }

                // Do the index lookup for nodes. Could also do edge 
                // index lookup using /e/ but I am too lazy in the moment ...

                t.ajax(t.ajaxPrefix + "/v1/find/?lookup=1&text=" + encodeURIComponent(phrase), "GET", undefined,
                    function (r) {
                        t.main.addOutput(element, JSON.stringify(r, undefined, 4));
                    },
                    function (r) {
                        t.main.addError(element, r);
                    });
            },

            // Change partition.
            //
            "part" : function (element, data) {
                "use strict";
                data = data.replace(/^\s+|\s+$/gm, '');

                if (data !== "") {
                    t.partition = data;
                }

                t.main.addOutput(element, "Partition to query is: " + t.partition);
            }
        };
    </script>
  </body>
</html>
`

/*
ClusterTermSRC is the cluster terminal HTML as a text blob.
*/
const ClusterTermSRC = `
<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Cluster Terminal</title>
    <style>    
        body {
            background: #fff;
            font-family: 'verdana';
            font-size: 11px;
            margin: 0;
            min-width: 320px;
        }
        
        .c-toggle{
            float:right;
        }

        /* Header */
    
        .c-header {
            background: linear-gradient(#000, #444);
            color: #fff;
            font-weight: bold;
            padding: 0 1em;
            margin: 0 0 1em 0;
            box-shadow: 3px 3px 3px rgba(50, 50, 50, 0.25);
        }

        .c-header h1 {
            display: inline-block;
            font-size: 18px;
            margin: 3px 0;
        }

        /* Info */
        
        table.c-info {
            margin: auto;
            min-width: 40%;
        }        
        
        table.c-info td {
            background: #FFFEEF;
            width:50%;
            padding: 10px;
            border: #000000 2px solid;
            border-radius: 10px;
            margin: 3em 0 0 0;
        }        

        table.c-info td:nth-child(odd) {
            text-align: right;
        }

        table.c-info td:nth-child(even) {
            background: #EEEEEE;
        }

        /* Terminal */

        .c-term {
            display: inline-block;
            background: #FFFEEF;
            padding: 10px;
            border: #000000 2px solid;
            border-radius: 10px;
            margin: 3em 0 0 0;
        }

        .c-term h2 {
            padding: 0;
            margin: 0;
        }

        .c-term table {
            padding: 5px;
            width:100%;
        }

        .c-term input {
            padding: 2px;
            width:100%;
        }

        .c-term .term_result {
            padding: 20px;
            font-weight: bold;
            white-space: pre-wrap;
        }

        /* Status */

        .c-status {
            float: left;
            margin: 0 0 0 50%;
            min-height: 100%;
            padding: 0;
            width: 50%;
        }

        .c-status dl {
            margin: 0;
        }
        
        .c-status dd {
            padding: 0 0 5px 0;
        }

        .c-log {
            background: #EEEEEE;
            padding: 10px;
            border: #000000 2px solid;
            border-radius: 10px;
            margin: 3em 0 0 0;
        }

        .c-log pre {
            white-space: pre-wrap;
            word-wrap: break-word;
        }

        .c-members {
            float: left;
            margin-left: -100%;
            text-align: left;
            width: 100%;
        }

        .c-members .c-member {
            background: #B3D9FF;
            display: inline-block;
            padding: 10px;
            border: #000000 2px solid;
            border-radius: 10px;
            margin: 3em 0 0 0;
            max-width: 80%;
            word-wrap: break-word;
        }

        .c-members .c-member c-error {
            background: #FFBBBB;
        }
    </style>
  </head>
  <body onload="c.main.init()">

    <div class="c-header"><h1 id="name"></h1> <h1 id="version"></h1></div>

    <!-- Info section - information on this cluster member are displayed here -->
    
    <table class="c-info">
        <tr>
            <td>Member:</td>
            <td id="mname"></td>
        </tr>
        <tr>
            <td>Network Interface:</td>
            <td id="mnetaddr"></td>
        </tr>
    </table>

    <!-- Terminal section - commands can be send to the cluster here -->
        
    <div id="term-toggle" class="c-term">
        <a href="javascript:void(0)" onclick="c.toggle('term', 'term-toggle')">Show Config Panel</a>
    </div>
    
    <div id="term" class="c-term" style="display:none">
        <div class="c-toggle"><a href="javascript:void(0)" onclick="c.toggle('term-toggle', 'term')">Hide Config Panel</a></div>
        <h2>Ping other instance:</h2>
    
        <table>
            <tr>
                <td>Member name:</td>
                <td><input id="ping_name" placeholder="e.g. member1"></td>
            </tr>
            <tr>
                <td>Network Interface:</td>
                <td><input id="ping_interface" placeholder="e.g. localhost:9030"></td>
            </tr>
        </table>
        <button onclick="c.main.PingInstance(c.$('ping_name').value, c.$('ping_interface').value)">Ping Instance</button>
        <pre id="ping_message" class="term_result"></pre>

        <br><br>

        <h2>Join a cluster:</h2>
    
        <table>
            <tr>
                <td>Member name:</td>
                <td><input id="join_name" placeholder="e.g. member1"></td>
            </tr>
            <tr>
                <td>Network Interface:</td>
                <td><input id="join_interface" placeholder="e.g. localhost:9030"></td>
            </tr>
        </table>
        <button onclick="c.main.JoinCluster(c.$('join_name').value, c.$('join_interface').value)">Join Cluster</button>
        <pre id="join_message" class="term_result"></pre>

        <br><br>

        <h2>Eject a member from this cluster:</h2>
        <table>
            <tr>
                <td>Member name:</td>
                <td><input id="eject_name" placeholder="e.g. member1"></td>
            </tr>
        </table>
        <button onclick="c.main.EjectMember(c.$('eject_name').value)">Eject Member</button>
        <pre id="eject_message" class="term_result"></pre>

    </div>
    
    <!-- Status section - the cluster status is displayed here -->
    
    <div class="c-status">
        <div id="members" class="c-members">
        </div>
        <div class="c-log">
            Member Log: <a title="Clear all log messages" href="javascript:void(0)" onclick="c.main.ClearLog()">[Clear]</a>
            <div id="logOn" class="c-toggle"><a title="Switch live log off" href="javascript:void(0)" onclick="c.toggle('logOff', 'logOn');c.pollLog=false">Live Log: On</a></div>
            <div id="logOff" class="c-toggle" style="display:none"><a title="Switch live log on" href="javascript:void(0)" onclick="c.toggle('logOn', 'logOff');c.pollLog=true;c.main.RunLogMonitor()">Live Log: Off</a></div>
            <pre id="log"></pre>
        </div>
    </div>

    <script>

        // Utility functions
        // =================

        if (c === undefined) {
          var c = {};
        }

        c.$ = function(id) { "use strict"; return document.getElementById(id); };
        c.toggle = function (show, hide) { "use strict"; c.$(hide).style = "display:none;"; c.$(show).style = "display:auto;"; };
        c.copyObject = function (o1, o2) { "use strict"; for (var attr in o1) { o2[attr] = o1[attr]; } };
        c.mergeObject = function (o1, o2) { "use strict"; for (var attr in o1) { if(o2[attr] === undefined) { o2[attr] = o1[attr]; } } };
        c.cloneObject = function (o) { "use strict"; var r = {}; ge.copyObject(o, r); return r; };
        c.esc = function (str) { "use strict"; return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;' ); };
        c.unesc = function (str) { "use strict"; return str.stripTags().replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&amp;/g,'&'); };
        c.insert = function(element, child) { "use strict"; if (typeof child === "string") { element.innerHTML = c.esc(child); } else { element.appendChild(child); } return element; };
        c.insertBefore = function(element, child) { "use strict"; element.parentNode.insertBefore(child, element); };
        c.insertAfter = function(element, child) { "use strict"; element.parentNode.insertBefore(child, element.nextSibling); };
        c.create = function(tag, attrs) {
            "use strict";
            var element = document.createElement(tag);
            if (attrs !== undefined) {
                c.getObjectKeys(attrs).forEach(function (v) {
                    element.setAttribute(v, attrs[v]);
                });
            }
            return element;
        };
        c.getObjectKeys = function (obj) {
            "use strict";
            var key, keys = [];
            for(key in obj) {
                if (obj.hasOwnProperty(key)) {
                    keys.push(key);
                }
            }
            keys.sort();
            return keys;
        };
        c.getObjectValues = function (obj) {
            "use strict";
            var key, values = [];
            for(key in obj) {
                if (obj.hasOwnProperty(key)) {
                    values.push(obj[key]);
                }
            }
            return values;
        };
        c.addEvent = function (element, eventName, func) {
            "use strict";
            if (element.addEventListener) {
                element.addEventListener(eventName, func, false);
            } else if (element.attachEvent) {
                element.attachEvent('on' + eventName, func);
            }
        };
        c.stopBubbleEvent = function (e) {
            "use strict";
            e = e ? e:window.event;
            if (e.stopPropagation) {
                e.stopPropagation();
            }
            if (e.cancelBubble !== null) {
                e.cancelBubble = true;
            }
            if (e.preventDefault) {
                e.preventDefault();
            }
        };
        c.bind = function () {
            "use strict";
            var f = arguments[0], t = Array.prototype.slice.call(arguments, 1);
            var a = t.splice(1);
            return function() {
                "use strict";
                return f.apply(t[0],
                               a.concat(Array.prototype.slice.call(arguments, 0)));
            }
        };
        c.ajax = function (url, method, body, callbackOK, callbackError) {
            "use strict";
            var http = new XMLHttpRequest(),
                start;

            if (method === undefined) {
                method = "GET"
            }

            http.open(method, url, true);
            http.setRequestHeader("content-type", "application/json");
            http.setRequestHeader("accept", "application/json");
            http.onload = function () {
                var rtime = Date.now() - start;
                document.title = "Terminal (Last response time: "+ rtime +"ms)";
                console.log("Response time:", rtime);
                try {
                    if (http.status === 200) {
                        if (callbackOK) {
                            if (http.response !== "") {
                                callbackOK(JSON.parse(http.response));
                            } else {
                                callbackOK();
                            }
                        }
                    } else {
                        if (callbackError) {
                           callbackError(http.response);
                        } else {
                            console.log(http.response);
                        }
                    }
                } catch(e) {
                    console.log("Ajax call failed - exception:", e);
                    throw e;
                }
            };

            if (body !== undefined) {
                http.send(JSON.stringify(body));
            } else {
                start = Date.now()
                http.send();
            }
        };

        // Global variables
        // ================

        c.ajaxPrefix = "/db";
        c.pollStatus = true;
        c.pollLog = true;
        c.memberInfos =  {};
        c.pollInterval = 1000;

        // Console
        // =======

        c.main = {

            init : function() {
                "use strict";

                console.log("Init console")

                c.ajax(c.ajaxPrefix + "/about/", "GET", undefined, function (r) {
                    c.$("name").innerHTML = c.esc(r.product + " Cluster Terminal");
                    c.$("version").innerHTML = c.esc(r.version);
                });

                c.main.PopulateMemberInfo();                
                c.main.PopulateMembers();
                c.main.RunLogMonitor();                
            },

            // Ping another cluster member instance.
            //
            PingInstance : function(name, netaddr) {
                "use strict";
                c.ajax(c.ajaxPrefix + "/v1/cluster/ping", "PUT", {
                    "name" : name,
                    "netaddr" : netaddr
                }, function (r) {
                    c.$("ping_message").innerHTML = c.esc(r.join(", "));
                }, function(e) {
                    c.$("ping_message").innerHTML = c.esc(e);
                });            
            },

            // Join another cluster.
            //
            JoinCluster : function(name, netaddr) {
                "use strict";
                c.ajax(c.ajaxPrefix + "/v1/cluster/join", "PUT", {
                    "name" : name,
                    "netaddr" : netaddr
                }, function (r) {
                    console.log(r);
                    c.$("join_message").innerHTML = c.esc("This instance joined the cluster of " + name);                    
                }, function(e) {
                    c.$("join_message").innerHTML = c.esc(e);
                });            
            },

            // Eject a member from the cluster.
            //
            EjectMember : function(name) {
                "use strict";
                c.ajax(c.ajaxPrefix + "/v1/cluster/eject", "PUT", {
                    "name" : name
                }, function (r) {
                    console.log(r);
                    c.$("eject_message").innerHTML = c.esc("Member " + name + " was ejected from the cluster");                    
                }, function(e) {
                    c.$("eject_message").innerHTML = c.esc(e);
                });            
            },

            // Clear the member's cluster log.
            //
            ClearLog : function(name) {
                "use strict";
                c.ajax(c.ajaxPrefix + "/v1/cluster/log", "DELETE", {
                    "name" : name
                }, function (r) {
                    c.$("log").innerHTML = "";
                });            
            },

            // Populate the member infos for the cluster.
            // NOTE: This is relative expensive as all members need to be contacted!
            //
            PopulateMemberInfo : function () {
                "use strict";

                c.ajax(c.ajaxPrefix + "/v1/cluster/memberinfos", "GET", undefined, function (r) {
                    c.memberInfos = r;
                });
            },

            // Populate the cluster members.
            //
            PopulateMembers : function () {
                "use strict";
                
                var membersElement = c.$("members"),
                      refreshMemberInfos = false;
                
                function insertMember(name, netaddr) {
                        var memberElement = c.create("div", {
                                "class" : "c-member"
                              }),
                              dl = c.create("dl"),
                              memberInfo = c.memberInfos[name];
                        c.insert(dl, c.insert(c.create("dt"), "Member"));
                        c.insert(dl, c.insert(c.create("dd"), name));
                        c.insert(dl, c.insert(c.create("dt"), "Network Interface"));
                        c.insert(dl, c.insert(c.create("dd"), netaddr));
                        c.insert(memberElement, dl);
                        c.insert(membersElement, memberElement);

                        if (memberInfo !== undefined) {
                            if (memberInfo.termurl !== undefined) {
                                c.insert(dl, c.insert(c.create("dt"), "Term URL"));
                                c.insert(dl, c.insert(c.create("dd"), c.insert(c.create("a", {
                                    "href" : memberInfo.termurl
                                }), memberInfo.termurl)));
                            }
                        } else {
                            console.log("Member info of", name, "not found - refresh member infos");
                            refreshMemberInfos = true;
                        }
                }

                c.ajax(c.ajaxPrefix + "/v1/cluster/", "GET", undefined, function (r) {
                    "use strict";

                    var members = c.main._listToObject(r.members),
                          failed = c.main._listToObject(r.failed);

                    // Populate info section
                    
                    c.$("mname").innerHTML = c.esc(r.members[0]);
                    c.$("mnetaddr").innerHTML = c.esc(r.members[1]);

                    // Populate status section

                    c.$("members").innerHTML = "";

                    c.getObjectKeys(members).forEach(function (m) {
                        insertMember(m, members[m]);
                    });

                    if (refreshMemberInfos) {
                        c.main.PopulateMemberInfo();
                    }
                    
                    if (c.pollStatus) {
                        window.setTimeout(c.main.PopulateMembers, c.pollInterval);
                    }
                });                
            },

            // Convert a stateinfo list into an object.
            //
            _listToObject : function (l) {
                "use strict";
                var o = {};
                if (l) {
                    for (var i=0;i<l.length;i+=2) {
                        o[l[i]] = l[i+1];
                    }
                }
                return o;
            },

            // Update the cluster log.
            //
            RunLogMonitor : function () {
                "use strict";
                
                c.ajax(c.ajaxPrefix + "/v1/cluster/log", "GET", undefined, function (r) {
                    c.$("log").innerHTML = c.esc(r.join("\n"));
                    if (c.pollLog) {
                        window.setTimeout(c.main.RunLogMonitor, c.pollInterval);
                    }
                });
            }
        };
    </script>
  </body>
</html>
`
