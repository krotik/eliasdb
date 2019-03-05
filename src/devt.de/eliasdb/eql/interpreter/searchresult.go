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
	"bytes"
	"encoding/csv"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"devt.de/eliasdb/graph/data"
)

/*
SearchHeader is the header of a search result.
*/
type SearchHeader struct {
	ResPrimaryKind string   // Primary node kind
	ResPartition   string   // Partition of result
	ColLabels      []string // Labels for columns
	ColFormat      []string // Format for columns
	ColData        []string // Data which should be displayed in the columns
}

/*
Partition returns the partition of a search result.
*/
func (sh *SearchHeader) Partition() string {
	return sh.ResPartition
}

/*
PrimaryKind returns the primary kind of a search result.
*/
func (sh *SearchHeader) PrimaryKind() string {
	return sh.ResPrimaryKind
}

/*
Labels returns all column labels of a search result.
*/
func (sh *SearchHeader) Labels() []string {
	return sh.ColLabels
}

/*
Format returns all column format definitions of a search result.
*/
func (sh *SearchHeader) Format() []string {
	return sh.ColFormat
}

/*
Data returns the data which is displayed in each column of a search result.
(e.g. 1:n:name - Name of starting nodes,
      3:e:key  - Key of edge traversed in the second traversal)
*/
func (sh *SearchHeader) Data() []string {
	return sh.ColData
}

/*
SearchResult data structure. A search result represents the result of an EQL query.
*/
type SearchResult struct {
	name      string     // Name to identify the result
	query     string     // Query which produced the search result
	withFlags *withFlags // With flags which should be applied to the result

	SearchHeader            // Embedded search header
	colFunc      []FuncShow // Function which transforms the data

	Source [][]string      // Special string holding the data source (node / edge) for each column
	Data   [][]interface{} // Data which is held by this search result
}

/*
newSearchResult creates a new search result object.
*/
func newSearchResult(rtp *eqlRuntimeProvider, query string) *SearchResult {

	cdl := make([]string, 0, len(rtp.colData))
	for i, cd := range rtp.colData {
		if rtp.colFunc[i] != nil {
			colDataSpec := strings.SplitN(cd, ":", 2)
			cdl = append(cdl, colDataSpec[0]+":func:"+rtp.colFunc[i].name()+"()")
		} else {
			cdl = append(cdl, cd)
		}
	}

	return &SearchResult{rtp.name, query, rtp.withFlags, SearchHeader{rtp.primaryKind, rtp.part, rtp.colLabels, rtp.colFormat,
		cdl}, rtp.colFunc, make([][]string, 0), make([][]interface{}, 0)}
}

/*
addRow adds a row to the result.
*/
func (sr *SearchResult) addRow(rowNodes []data.Node, rowEdges []data.Edge) error {
	var pos int
	var isNode bool
	var err error

	src := make([]string, 0, len(sr.ColData))
	row := make([]interface{}, 0, len(sr.ColData))

	addNil := func() {
		src = append(src, "")
		row = append(row, nil)
	}
	addNode := func(n data.Node, attr string) {
		if n == nil {
			addNil()
			return
		}
		src = append(src, "n:"+n.Kind()+":"+n.Key())
		row = append(row, n.Attr(attr))
	}
	addEdge := func(e data.Edge, attr string) {
		if e == nil {
			addNil()
			return
		}
		row = append(row, e.Attr(attr))
		src = append(src, "e:"+e.Kind()+":"+e.Key())
	}

	// Pick only the data which is needed for the result

	for i, colData := range sr.ColData {
		attr := ""

		// Row data should be picked from the node

		colDataSpec := strings.SplitN(colData, ":", 3)

		if len(colDataSpec) != 3 {
			return &ResultError{sr.name, ErrInvalidColData, "Column data spec must have 3 items: " + colData}
		}

		posstring := colDataSpec[0]

		if colDataSpec[1] == "func" {

			pos, _ = strconv.Atoi(posstring)

		} else {

			if colDataSpec[1] == "n" {
				isNode = true
			} else if colDataSpec[1] == "e" {
				isNode = false
			} else {
				return &ResultError{sr.name, ErrInvalidColData, "Invalid data source '" + colDataSpec[1] + "' (either n - Node or e - Edge)"}
			}

			attr = colDataSpec[2]

			pos, err = strconv.Atoi(posstring)
			if err != nil || pos < 1 {
				return &ResultError{sr.name, ErrInvalidColData, "Invalid data index: " + colData}
			}
		}

		// Make pos an index

		pos--

		// Check if the row data should come from a function transformation
		// or from a node itself

		if cf := sr.colFunc[i]; cf != nil {

			fres, fsrc, err := sr.colFunc[i].eval(rowNodes[pos], rowEdges[pos])
			if err != nil {
				return err
			}

			row = append(row, fres)
			src = append(src, fsrc)

		} else {

			if isNode {
				addNode(rowNodes[pos], attr)
			} else {
				addEdge(rowEdges[pos], attr)
			}
		}
	}

	sr.Source = append(sr.Source, src)
	sr.Data = append(sr.Data, row)

	return nil
}

/*
finish is called once all rows have been added.
*/
func (sr *SearchResult) finish() {

	// Apply filtering

	if len(sr.withFlags.notnullCol) > 0 || len(sr.withFlags.uniqueCol) > 0 {

		uniqueMaps := make([]map[string]int, len(sr.withFlags.uniqueCol))
		for i := range uniqueMaps {
			uniqueMaps[i] = make(map[string]int)
		}

		// Using downward loop so we can remove the current element if necessary

		for i := len(sr.Data) - 1; i >= 0; i-- {
			row := sr.Data[i]
			cont := false

			// Apply not null

			for _, nn := range sr.withFlags.notnullCol {
				if row[nn] == nil {
					sr.Data = append(sr.Data[:i], sr.Data[i+1:]...)
					cont = true
					break
				}
			}

			if cont {
				continue
			}

			// Apply unique

			for j, u := range sr.withFlags.uniqueCol {
				if _, ok := uniqueMaps[j][fmt.Sprint(row[u])]; ok {
					uniqueMaps[j][fmt.Sprint(row[u])]++
					sr.Data = append(sr.Data[:i], sr.Data[i+1:]...)
					break
				} else {
					uniqueMaps[j][fmt.Sprint(row[u])] = 1
				}
			}
		}

		// Add unique counts if necessary

		for j, uc := range sr.withFlags.uniqueColCnt {
			u := sr.withFlags.uniqueCol[j]
			if uc {
				for _, row := range sr.Data {
					row[u] = fmt.Sprintf("%v (%d)", row[u], uniqueMaps[j][fmt.Sprint(row[u])])
				}
			}
		}
	}

	// Apply ordering

	for i, ordering := range sr.withFlags.ordering {
		sort.Stable(&SearchResultRowComparator{ordering == withOrderingAscending,
			sr.withFlags.orderingCol[i], sr.Data, sr.Source})
	}

}

/*
Header returns all column headers.
*/
func (sr *SearchResult) Header() *SearchHeader {
	return &sr.SearchHeader
}

/*
Query returns the query which produced this result.
*/
func (sr *SearchResult) Query() string {
	return sr.query
}

/*
RowCount returns the number of rows of the result.
*/
func (sr *SearchResult) RowCount() int {
	return len(sr.Data)
}

/*
Row returns a row of the result.
*/
func (sr *SearchResult) Row(line int) []interface{} {
	return sr.Data[line]
}

/*
Rows returns all rows.
*/
func (sr *SearchResult) Rows() [][]interface{} {
	return sr.Data
}

/*
RowSource returns the sources of a result row.
Format is either: <n/e>:<kind>:<key> or q:<query>
*/
func (sr *SearchResult) RowSource(line int) []string {
	return sr.Source[line]
}

/*
RowSources returns the sources of a result.
*/
func (sr *SearchResult) RowSources() [][]string {
	return sr.Source
}

/*
String returns a string representation of this search result.
*/
func (sr *SearchResult) String() string {
	var buf bytes.Buffer

	buf.WriteString("Labels: ")
	buf.WriteString(strings.Join(sr.ColLabels, ", "))
	buf.WriteString("\n")

	buf.WriteString("Format: ")
	buf.WriteString(strings.Join(sr.ColFormat, ", "))
	buf.WriteString("\n")

	buf.WriteString("Data: ")
	buf.WriteString(strings.Join(sr.ColData, ", "))
	buf.WriteString("\n")

	// Render the table

	for _, row := range sr.Data {
		for i, col := range row {

			if col != nil {
				buf.WriteString(fmt.Sprint(col))
			} else {
				buf.WriteString("<not set>")
			}
			if i < len(row)-1 {
				buf.WriteString(", ")
			}
		}
		buf.WriteString("\n")
	}

	return buf.String()
}

/*
CSV returns this search result as comma-separated strings.
*/
func (sr *SearchResult) CSV() string {
	var buf bytes.Buffer

	labels := sr.Header().ColLabels
	strData := make([][]string, len(sr.Data)+1)

	// Prepare string data

	strData[0] = make([]string, len(labels))
	for i, s := range labels {
		strData[0][i] = s
	}
	for i, row := range sr.Data {
		strData[i+1] = make([]string, len(row))
		for j, s := range row {
			strData[i+1][j] = fmt.Sprint(s)
		}
	}

	// Write CSV data into buffer

	w := csv.NewWriter(&buf)

	w.WriteAll(strData)

	return buf.String()
}

// Util functions
// ==============

/*
SearchResultRowComparator is a comparator object used for sorting the result
*/
type SearchResultRowComparator struct {
	Ascening bool            // Sort should be ascending
	Column   int             // Column to sort
	Data     [][]interface{} // Data to sort
	Source   [][]string      // Source entries which follow the data
}

func (c SearchResultRowComparator) Len() int {
	return len(c.Data)
}

func (c SearchResultRowComparator) Less(i, j int) bool {
	c1 := c.Data[i][c.Column]
	c2 := c.Data[j][c.Column]

	num1, err := strconv.ParseFloat(fmt.Sprint(c1), 64)
	if err == nil {
		num2, err := strconv.ParseFloat(fmt.Sprint(c2), 64)
		if err == nil {
			if c.Ascening {
				return num1 < num2
			}
			return num1 > num2
		}
	}

	if c.Ascening {
		return fmt.Sprintf("%v", c1) < fmt.Sprintf("%v", c2)
	}

	return fmt.Sprintf("%v", c1) > fmt.Sprintf("%v", c2)
}

func (c SearchResultRowComparator) Swap(i, j int) {
	c.Data[i], c.Data[j] = c.Data[j], c.Data[i]
	c.Source[i], c.Source[j] = c.Source[j], c.Source[i]
}

// Testing functions
// =================

type rowSort SearchResult

func (s rowSort) Len() int {
	return len(s.Data)
}
func (s rowSort) Swap(i, j int) {
	s.Data[i], s.Data[j] = s.Data[j], s.Data[i]
	s.Source[i], s.Source[j] = s.Source[j], s.Source[i]
}
func (s rowSort) Less(i, j int) bool {

	keyString := func(data []interface{}) string {
		var ret bytes.Buffer
		for _, d := range data {
			ret.WriteString(fmt.Sprintf("%v", d))
		}
		return ret.String()
	}

	return keyString(s.Data[i]) < keyString(s.Data[j])
}

/*
StableSort sorts the rows of the result in a stable 100% reproducible way.
*/
func (sr *SearchResult) StableSort() {
	sort.Stable(rowSort(*sr))
}
