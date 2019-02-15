/*
 * EliasDB
 *
 * Copyright 2016 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package eql

/*
SearchResultHeader models the header of an EQL search result.
*/
type SearchResultHeader interface {

	/*
		Partition returns the partition of a search result.
	*/
	Partition() string

	/*
		PrimaryKind returns the primary kind of a search result.
	*/
	PrimaryKind() string

	/*
	   Labels returns all column labels of a search result.
	*/
	Labels() []string

	/*
	   Format returns all column format definitions of a search result.
	*/
	Format() []string

	/*
	   Data returns the data which is displayed in each column of a search result.
	   (e.g. 1:n:name - Name of starting nodes,
	         3:e:key  - Key of edge traversed in the second traversal)
	*/
	Data() []string
}

/*
SearchResult models an EQL search result.
*/
type SearchResult interface {

	/*
	   Header returns a data structure describing the result header.
	*/
	Header() SearchResultHeader

	/*
	   Query returns the query which produced this result.
	*/
	Query() string

	/*
	   RowCount returns the number of rows of the result.
	*/
	RowCount() int

	/*
	   Row returns a row of the result.
	*/
	Row(line int) []interface{}

	/*
	   Rows returns all result rows.
	*/
	Rows() [][]interface{}

	/*
	   RowSource returns the sources of a result row.
	   Format is either: <n/e>:<kind>:<key> or q:<query>
	*/
	RowSource(line int) []string

	/*
	   RowSources returns the sources of a result.
	*/
	RowSources() [][]string

	/*
		String returns a string representation of this search result.
	*/
	String() string

	/*
	   CSV returns this search result as comma-separated strings.
	*/
	CSV() string
}
