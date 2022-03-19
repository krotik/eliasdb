/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package interpreter

import (
	"encoding/json"
	"testing"
)

func TestSortingAndLimiting(t *testing.T) {
	gm, _ := songGraphGroups()

	query := map[string]interface{}{
		"operationName": nil,
		"query": `
{
  Song(ascending:"key") {
    key
	name
  }
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "key": "Aria1",
        "name": "Aria1"
      },
      {
        "key": "Aria2",
        "name": "Aria2"
      },
      {
        "key": "Aria3",
        "name": "Aria3"
      },
      {
        "key": "Aria4",
        "name": "Aria4"
      },
      {
        "key": "DeadSong2",
        "name": "DeadSong2"
      },
      {
        "key": "FightSong4",
        "name": "FightSong4"
      },
      {
        "key": "LoveSong3",
        "name": "LoveSong3"
      },
      {
        "key": "MyOnlySong3",
        "name": "MyOnlySong3"
      },
      {
        "key": "StrangeSong1",
        "name": "StrangeSong1"
      }
    ]
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
{
  Song(ascending:"name", last: 3) {
    key
	name
  }
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "key": "LoveSong3",
        "name": "LoveSong3"
      },
      {
        "key": "MyOnlySong3",
        "name": "MyOnlySong3"
      },
      {
        "key": "StrangeSong1",
        "name": "StrangeSong1"
      }
    ]
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
{
  Song(descending:"name", last: 3) {
    key
	name
  }
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "key": "Aria3",
        "name": "Aria3"
      },
      {
        "key": "Aria2",
        "name": "Aria2"
      },
      {
        "key": "Aria1",
        "name": "Aria1"
      }
    ]
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
{
  Song(ascending:"ranking", last: 3) {
    key
	name
	ranking
  }
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "key": "Aria1",
        "name": "Aria1",
        "ranking": 8
      },
      {
        "key": "Aria4",
        "name": "Aria4",
        "ranking": 18
      },
      {
        "key": "MyOnlySong3",
        "name": "MyOnlySong3",
        "ranking": 19
      }
    ]
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
{
  Song(descending:"ranking", last: 3) {
    key
	name
	ranking
  }
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "key": "FightSong4",
        "name": "FightSong4",
        "ranking": 3
      },
      {
        "key": "Aria2",
        "name": "Aria2",
        "ranking": 2
      },
      {
        "key": "LoveSong3",
        "name": "LoveSong3",
        "ranking": 1
      }
    ]
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
{
  Song(ascending:"name", items: 2, last: 3) {
    key
	name
  }
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "key": "LoveSong3",
        "name": "LoveSong3"
      },
      {
        "key": "MyOnlySong3",
        "name": "MyOnlySong3"
      }
    ]
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	// From the last 3 we retrieve item 1 and the next

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
{
  Song(ascending:"name", from : 1, items: 2, last: 3) {
    key
	name
  }
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "key": "MyOnlySong3",
        "name": "MyOnlySong3"
      },
      {
        "key": "StrangeSong1",
        "name": "StrangeSong1"
      }
    ]
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
{
  Song(ascending:"name", from : 100, items: 200, last: 2) {
    key
	name
  }
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "key": "MyOnlySong3",
        "name": "MyOnlySong3"
      },
      {
        "key": "StrangeSong1",
        "name": "StrangeSong1"
      }
    ]
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
{
  Song(ascending:"name", descending:"key") {
    key
	name
  }
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": []
  },
  "errors": [
    {
      "locations": [
        {
          "column": 45,
          "line": 3
        }
      ],
      "message": "Cannot specify ascending and descending sorting",
      "path": [
        "Song"
      ]
    }
  ]
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
{
  Song(ascending:"hans") {
    key
	name
  }
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "key": "StrangeSong1",
        "name": "StrangeSong1"
      },
      {
        "key": "FightSong4",
        "name": "FightSong4"
      },
      {
        "key": "DeadSong2",
        "name": "DeadSong2"
      },
      {
        "key": "LoveSong3",
        "name": "LoveSong3"
      },
      {
        "key": "MyOnlySong3",
        "name": "MyOnlySong3"
      },
      {
        "key": "Aria1",
        "name": "Aria1"
      },
      {
        "key": "Aria2",
        "name": "Aria2"
      },
      {
        "key": "Aria3",
        "name": "Aria3"
      },
      {
        "key": "Aria4",
        "name": "Aria4"
      }
    ]
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}
}

func TestFragments(t *testing.T) {
	gm, _ := songGraphGroups()

	// Test fragments for different return types

	query := map[string]interface{}{
		"operationName": nil,
		"query": `
{
  Song(key : "StrangeSong1") {
    song_key : key
	foo : bar(traverse : ":::") {
		key
		kind
		...groupFields
		...authorFields @skip(if: false)
	}
  }
}
fragment groupFields on group {
  owner
}
fragment authorFields on Author {
  name
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "foo": [
          {
            "key": "123",
            "kind": "Author",
            "name": "Mike"
          },
          {
            "key": "Best",
            "kind": "group",
            "owner": "noowner"
          }
        ],
        "song_key": "StrangeSong1"
      }
    ]
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	// Test fragments for different return types - now using inline fragments

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
{
  Song(key : "StrangeSong1") {
    song_key : key
	foo : bar(traverse : ":::") {
		key
		kind
		... on group @skip(if: false) {
		  owner
		}
		... on Author {
		  name
		}
	}
  }
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "foo": [
          {
            "key": "123",
            "kind": "Author",
            "name": "Mike"
          },
          {
            "key": "Best",
            "kind": "group",
            "owner": "noowner"
          }
        ],
        "song_key": "StrangeSong1"
      }
    ]
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
{
  key
  name1 : name
  ...SongKind
  ...foo
}
fragment SongKind on Song {
  kind
  name2 : name
  key
}
`,
		"variables": nil,
	}

	if rerr := checkResult("", query, gm); rerr == nil ||
		rerr.Error() != "Fatal GraphQL query error in test: Invalid construct (Fragment foo is not defined) (Line:2 Pos:2)" {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
{
  key
  name1 : name
  ...SongKind
  ...foo
}
fragment SongKind on Song {
  kind
  name2 : name
  key
}
fragment SongKind on Song {
  kind
  name2 : name
  key
}
`,
		"variables": nil,
	}

	if rerr := checkResult("", query, gm); rerr == nil ||
		rerr.Error() != "Fatal GraphQL query error in test: Ambiguous definition (Fragment SongKind defined multiple times) (Line:2 Pos:2)" {
		t.Error(rerr)
		return
	}
}

func TestMutation(t *testing.T) {
	gm, _ := songGraphGroups()

	query := map[string]interface{}{
		"operationName": nil,
		"query": `
{
  Song(storeNode : {
	key:"newsongkey",
	name:"newsongname"
}, key : "newsongkey") {
    key,
    name
  }
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": []
  },
  "errors": [
    {
      "locations": [
        {
          "column": 25,
          "line": 6
        }
      ],
      "message": "Operation must be a mutation to modify data",
      "path": [
        "Song"
      ]
    }
  ]
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
mutation {
  Song(storeNode : {
    key  : "newsongkey",
    name : "newsongname"
  }, storeEdge : {
    key  : "newedgekey",
    kind : "newedgekind"
    end1key       : "newsongkey",
    end1kind      : "Song",
    end1role      : "song",
    end1cascading : false,
    end2key       : "Best",
    end2kind      : "group",
    end2role      : "group",
    end2cascading : false,
  },key : "newsongkey") {
    key,
    name,
    group(traverse : ":::group") {
    	key
		otherMembers(traverse : ":::Song", matches : { 
			not_key : "newsongkey",
			name : "^.*Song[0-9]$" 
		}) {
			key,
			kind,
			name,
		}
    }
  }
}
`,
		"variables": nil,
	}

	result, _ := runQuery("test", "main", query, gm, nil, true)
	actualResultBytes, _ := json.MarshalIndent(result, "", "  ")
	actualResult := string(actualResultBytes)

	if actualResult != `{
  "data": {
    "Song": []
  },
  "errors": [
    {
      "locations": [
        {
          "column": 26,
          "line": 17
        }
      ],
      "message": "Can only perform read operations",
      "path": [
        "Song"
      ]
    }
  ]
}` {
		t.Error("Unexpected result:", actualResult)
		return
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "group": [
          {
            "key": "Best",
            "otherMembers": [
              {
                "key": "LoveSong3",
                "kind": "Song",
                "name": "LoveSong3"
              },
              {
                "key": "MyOnlySong3",
                "kind": "Song",
                "name": "MyOnlySong3"
              },
              {
                "key": "StrangeSong1",
                "kind": "Song",
                "name": "StrangeSong1"
              }
            ]
          }
        ],
        "key": "newsongkey",
        "name": "newsongname"
      }
    ]
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
mutation {
  Song(removeNode : {
    key  : "newsongkey",
  }, removeEdge : {
    key  : "newedgekey",
    kind : "newedgekind"
  },key : "newsongkey") {
    key,
    name,
    group(traverse : ":::group") {
    	key
		otherMembers(traverse : ":::Song", matches : { 
			not_key : "newsongkey",
			name : "^.*Song[0-9]$" 
		}) {
			key,
			kind,
			name,
		}
    }
  }
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": []
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
mutation {
  Song(removeNode : {}) {
    key
    kind
  }
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": []
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
mutation {
  Song(storeNode : "Hans", storeEdge : "Franz", key : "Honk") {
    key,
    name,
  }
}`,
		"variables": nil,
	}

	if rerr := checkResult(`{
  "data": {
    "Song": []
  },
  "errors": [
    {
      "locations": [
        {
          "column": 64,
          "line": 3
        }
      ],
      "message": "Object required for node attributes and values",
      "path": [
        "Song"
      ]
    },
    {
      "locations": [
        {
          "column": 64,
          "line": 3
        }
      ],
      "message": "Object required for edge attributes and values",
      "path": [
        "Song"
      ]
    }
  ]
}`, query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
mutation {
  Song(removeNode : "Hans", removeEdge : "Franz", key : "Honk") {
    key,
    name,
  }
}`,
		"variables": nil,
	}

	if rerr := checkResult(`{
  "data": {
    "Song": []
  },
  "errors": [
    {
      "locations": [
        {
          "column": 66,
          "line": 3
        }
      ],
      "message": "Object required for node key and kind",
      "path": [
        "Song"
      ]
    },
    {
      "locations": [
        {
          "column": 66,
          "line": 3
        }
      ],
      "message": "Object required for edge key and kind",
      "path": [
        "Song"
      ]
    }
  ]
}`, query, gm); rerr != nil {
		t.Error(rerr)
		return
	}
}

func TestTraversals(t *testing.T) {
	gm, _ := songGraphGroups()

	query := map[string]interface{}{
		"operationName": nil,
		"query": `
{
  Song(key : "StrangeSong1") {
    song_key : key
	foo : bar(traverse : ":::") {
		key
		kind
		Name : name
	}
  }
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "foo": [
          {
            "Name": "Mike",
            "key": "123",
            "kind": "Author"
          },
          {
            "Name": null,
            "key": "Best",
            "kind": "group"
          }
        ],
        "song_key": "StrangeSong1"
      }
    ]
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}
}

func TestListQueries(t *testing.T) {
	gm, _ := songGraphGroups()

	query := map[string]interface{}{
		"operationName": "foo",
		"query": `
{
  Song {
	key
  }
}
`,
		"variables": nil,
	}

	_, err := runQuery("test", "main", query, gm, nil, false)
	if err == nil || err.Error() != "Fatal GraphQL operation error in test: Missing operation (Operation foo not found) (Line:2 Pos:2)" {
		t.Error("Unexpected result:", err)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
fragment friendFields on User {
  id
  name
  profilePic(size: 50)
}
`,
		"variables": nil,
	}

	res, err := runQuery("test", "main", query, gm, nil, false)
	if err == nil || err.Error() != "Fatal GraphQL operation error in test: Missing operation (No executable expression found) (Line:2 Pos:2)" {
		t.Error("Unexpected result:", res, err)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
{
  Song1 : Song {
    song_key : key
    song_key1 : key
    song_key1 : name # This is illegal and will be ignored
    song_key1 : key
	name
	name
  }
  group {
	key
  },
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song1": [
      {
        "name": "StrangeSong1",
        "song_key": "StrangeSong1",
        "song_key1": "StrangeSong1"
      },
      {
        "name": "FightSong4",
        "song_key": "FightSong4",
        "song_key1": "FightSong4"
      },
      {
        "name": "DeadSong2",
        "song_key": "DeadSong2",
        "song_key1": "DeadSong2"
      },
      {
        "name": "LoveSong3",
        "song_key": "LoveSong3",
        "song_key1": "LoveSong3"
      },
      {
        "name": "MyOnlySong3",
        "song_key": "MyOnlySong3",
        "song_key1": "MyOnlySong3"
      },
      {
        "name": "Aria1",
        "song_key": "Aria1",
        "song_key1": "Aria1"
      },
      {
        "name": "Aria2",
        "song_key": "Aria2",
        "song_key1": "Aria2"
      },
      {
        "name": "Aria3",
        "song_key": "Aria3",
        "song_key1": "Aria3"
      },
      {
        "name": "Aria4",
        "song_key": "Aria4",
        "song_key1": "Aria4"
      }
    ],
    "group": [
      {
        "key": "Best"
      }
    ]
  },
  "errors": [
    {
      "locations": [
        {
          "column": 17,
          "line": 3
        }
      ],
      "message": "Field identifier song_key1 used multiple times",
      "path": [
        "Song1"
      ]
    },
    {
      "locations": [
        {
          "column": 17,
          "line": 3
        }
      ],
      "message": "Field identifier name used multiple times",
      "path": [
        "Song1"
      ]
    }
  ]
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
{
  Song(key : "StrangeSong1") {
    song_key : key
	name
  }
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "name": "StrangeSong1",
        "song_key": "StrangeSong1"
      }
    ]
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
	   {
	     Song(matches : { name : "Aria[2-4]", not_name : "Aria4", foo : "a[" }) {
	       song_key : key
	     }
	   }
	   `,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "song_key": "Aria2"
      },
      {
        "song_key": "Aria3"
      }
    ]
  },
  "errors": [
    {
      "locations": [
        {
          "column": 79,
          "line": 3
        }
      ],
      "message": "Regex a[ did not compile: error parsing regexp: missing closing ]: `[1:]+"`[`"+`",
      "path": [
        "Song"
      ]
    }
  ]
}`, query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
	   {
	     Song(matches : { name1 : "test" }) {
	       song_key : key
	     }
	   }
	   `,
		"variables": nil,
	}

	if rerr := checkResult(`{
  "data": {
    "Song": []
  }
}`, query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
	   {
	     Song(matches :  [ "name1", "test" ]) {
	     }
	   }
	   `,
		"variables": nil,
	}

	if rerr := checkResult(`{
  "data": {
    "Song": [
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {}
    ]
  },
  "errors": [
    {
      "locations": [
        {
          "column": 45,
          "line": 3
        }
      ],
      "message": "Matches expression is not a map",
      "path": [
        "Song"
      ]
    }
  ]
}`, query, gm); rerr != nil {
		t.Error(rerr)
		return
	}

	query = map[string]interface{}{
		"operationName": nil,
		"query": `
fragment Song on Song {
  key
  name1 : name
  ...SongKind
}
query b {  # This should be executed
  Song {
	key
	...Song
  }
}
query a {
  Song1 {
	key1
  }
}
fragment SongKind on Song {
  kind
  name2 : name
  key
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "key": "StrangeSong1",
        "kind": "Song",
        "name1": "StrangeSong1",
        "name2": "StrangeSong1"
      },
      {
        "key": "FightSong4",
        "kind": "Song",
        "name1": "FightSong4",
        "name2": "FightSong4"
      },
      {
        "key": "DeadSong2",
        "kind": "Song",
        "name1": "DeadSong2",
        "name2": "DeadSong2"
      },
      {
        "key": "LoveSong3",
        "kind": "Song",
        "name1": "LoveSong3",
        "name2": "LoveSong3"
      },
      {
        "key": "MyOnlySong3",
        "kind": "Song",
        "name1": "MyOnlySong3",
        "name2": "MyOnlySong3"
      },
      {
        "key": "Aria1",
        "kind": "Song",
        "name1": "Aria1",
        "name2": "Aria1"
      },
      {
        "key": "Aria2",
        "kind": "Song",
        "name1": "Aria2",
        "name2": "Aria2"
      },
      {
        "key": "Aria3",
        "kind": "Song",
        "name1": "Aria3",
        "name2": "Aria3"
      },
      {
        "key": "Aria4",
        "kind": "Song",
        "name1": "Aria4",
        "name2": "Aria4"
      }
    ]
  },
  "errors": [
    {
      "locations": [
        {
          "column": 9,
          "line": 8
        }
      ],
      "message": "Field identifier key used multiple times",
      "path": [
        "Song"
      ]
    }
  ]
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}
}

func TestShortcutListQueries(t *testing.T) {
	gm, _ := songGraphGroups()

	query := map[string]interface{}{
		"operationName": nil,
		"query": `
{
  Song(matches : {name : ["Aria1", Aria2, "Aria3" ]}) {
	foo : key
	name
	bar: Author {
	  name
	}
  }
}
`,
		"variables": nil,
	}

	if rerr := checkResult(`
{
  "data": {
    "Song": [
      {
        "bar": [
          {
            "name": "John"
          }
        ],
        "foo": "Aria1",
        "name": "Aria1"
      },
      {
        "bar": [
          {
            "name": "John"
          }
        ],
        "foo": "Aria2",
        "name": "Aria2"
      },
      {
        "bar": [
          {
            "name": "John"
          }
        ],
        "foo": "Aria3",
        "name": "Aria3"
      }
    ]
  }
}`[1:], query, gm); rerr != nil {
		t.Error(rerr)
		return
	}
}
