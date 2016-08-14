/* 
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. 
 */

package main

const TERM_SRC = `
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
        getObjectValues = function (obj) {
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
            var http = new XMLHttpRequest();

            if (method === undefined) {
                method = "GET"
            }

            http.open(method, url, true);
            http.setRequestHeader("content-type", "application/json");
            http.setRequestHeader("accept", "application/json");
            http.onload = function () {
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
                stipText = function (t) {

                    // Strip out unwanted html tags

                    return t.replace(/<(?:.|\n)*?>/gm, ' ').replace(/&(?:.|\n)*?;/gm, ' ');
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
                        t.main.exec(input, stipText(input.innerHTML));
                    }
                });

                t.addEvent(term, "click", function (e) {
                    input.focus();
                });

                t.addEvent(buttonOK, "click", function (e) {
                    t.main.exec(input, stipText(input.innerHTML));
                });

                t.addEvent(buttonClear, "click", function (e) {
                    input.innerHTML = "";
                });
            },

            // Execute a command.
            //
            exec : function (element, text) {
                "use strict";

                var sp   = text.split(" ", 1),
                    cmd  = sp[0],
                    rest = text.substring(sp[0].length);

                console.log("Execute:", cmd, rest);

                // After the cmd has been executed

				var cmdFunc = t.cmds[cmd];

				if (cmdFunc !== undefined) {
                	cmdFunc(element, rest);
				} else {
					t.main.addError(element, "Unknown command");
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
                                              "about      - Returns product information\n" +
                                              "info       - Returns general datastore information\n" +
                                              "part       - Display / change the partition which is queried\n" +
                                              "get/lookup - Run a YQL query\n" +
                                              "index      - Do a fulltext search index lookup\n" +
                                              "store      - Stores given JSON structure as data\n" +
                                              "delete     - Delete data from the datastore\n");
                    return;
                }
                else if (data === "store") {
                    t.main.addOutput(element, "Stores given JSON structure as data.\n\n" +
                                              "Data is inserted/updated and should be of the form:\n" +
                                              "{\n" +
                                              '    "nodes" : [ \n' +
                                              '        {\n' +
                                              '            "key"  : "<node key>"\n' +
                                              '            "kind" : "<node kind>"\n' +
                                              '            ... more node attributes\n' +
                                              '        }\n' +
                                              '        ... more nodes\n' +
                                              '    ],\n' +
                                              '    "edges" : [ \n' +
                                              '        {\n' +
                                              '            "key"           : "<edge key>"\n' +
                                              '            "kind"          : "<edge kind>"\n' +
                                              '            "end1key"       : "<end1 info>"\n' +
                                              '            "end1kind"      : "<end1 info>"\n' +
                                              '            "end1role"      : "<end1 info>"\n' +
                                              '            "end1cascading" : <true/false>\n' +
                                              '            "end2key"       : "<end2 info>"\n' +
                                              '            "end2kind"      : "<end2 info>"\n' +
                                              '            "end2role"      : "<end2 info>"\n' +
                                              '            "end2cascading" : <true/false>\n' +
                                              '            ... more edge attributes\n' +
                                              '        }\n' +
                                              '        ... more edges\n' +
                                              '    ]\n' +
                                              "}\n");
                    return;
                }
                else if (data === "delete") {
                    t.main.addOutput(element, "Delete data from the datastore.\n\n" +
                                              "Data is deleted and should be of the form:\n" +
                                              "{\n" +
                                              '    "nodes" : [ \n' +
                                              '        {\n' +
                                              '            "key"  : "<node key>"\n' +
                                              '            "kind" : "<node kind>"\n' +
                                              '            ... more node attributes\n' +
                                              '        }\n' +
                                              '        ... more nodes\n' +
                                              '    ],\n' +
                                              '    "edges" : [ \n' +
                                              '        {\n' +
                                              '            "key"           : "<edge key>"\n' +
                                              '            "kind"          : "<edge kind>"\n' +
                                              '        }\n' +
                                              '        ... more edges\n' +
                                              '    ]\n' +
                                              "}\n");
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
                else if (data === "index") {
                    t.main.addOutput(element, "Do a fulltext search index lookup.\n\n" +
                              'The index can lookup a word, phrase (multiple words in a consecutive order) or attribute value.\n'+
                              'A query should have the form: index <node kind> <node attr> <type (word, phrase or value)> <search string>\n');
                    return;
                }

                t.main.addOutput(element, "Unknown help topic: " + data);
            },

            // Get the "about" JSON
            //
			"about" : function (element) {
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

            // Store data in the datastore.
            //
            "store" : function (element, data) {
				"use strict";
                var dataObj;

                try {
                    dataObj = JSON.parse(data);
                } catch(e) {
                    t.main.addError(element, e.toString());
                    return
                }

                t.ajax(t.ajaxPrefix + "/v1/graph/" + t.partition + "/", "POST", dataObj,
                    function () {
                        t.main.addOutput(element, "Ok");
                    },
                    function (r) {
                        t.main.addError(element, r);
                    });
			},

            // Delete data in the datastore.
            //
            "delete" : function (element, data) {
				"use strict";
                var dataObj;

                try {
                    dataObj = JSON.parse(data);
                } catch(e) {
                    t.main.addError(element, e.toString());
                    return
                }

                t.ajax(t.ajaxPrefix + "/v1/graph/" + t.partition + "/", "DELETE", dataObj,
                    function () {
                        t.main.addOutput(element, "Ok");
                    },
                    function (r) {
                        t.main.addError(element, r);
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
            "index" : function (element, data) {
                "use strict";
                data = data.replace(/^\s+|\s+$/gm, '');

                var args = data.split(" ");

                // Check all arguments were given
                
                if (args.length < 4) {
                    t.main.addError(element, "Index lookup requires: <node kind> <node attr> <type (word, phrase or value)> <string>");
                    return;
                }

                // Do some splice magic
                
                var nargs = args.splice(0,3),
                    str = args.join(' ');

                args = nargs;

                // Do the index lookup for nodes. Could also do edge 
                // index lookup using /e/ but I am too lazy in the moment ...

                t.ajax(t.ajaxPrefix + "/v1/index/" + t.partition + "/n/" + args[0] + "?attr=" + encodeURIComponent(args[1]) +
                    "&" + encodeURIComponent(args[2]) + "=" + encodeURIComponent(str), "GET", undefined,
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
