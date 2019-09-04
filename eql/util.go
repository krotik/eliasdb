package eql

import (
	"fmt"
	"sort"
	"strings"

	"devt.de/krotik/common/stringutil"
	"devt.de/krotik/eliasdb/eql/parser"
)

/*
RefineQueryByResultRows tries to construct a query which will only
show certain rows of a given query result. Might fail if no primary
node per row can be identified or if the "primary" keyword is used.
*/
func RefineQueryByResultRows(res SearchResult, rows []int) (string, error) {
	var col = -1
	var ret = ""

	// Search for column which gives the root node key and kind

	err := fmt.Errorf("Could not determine root nodes")
	rowCount := res.RowCount()

	if rowCount > 0 {
		for i, d := range res.Header().Data() {
			if ds := strings.Split(d, ":"); ds[0] == "1" && ds[1] == "n" {
				col = i
				err = nil
			}
		}
	}

	if err == nil {
		var ast *parser.ASTNode

		// Get the AST

		if ast, err = ParseQuery("Refine query", res.Query()); err == nil {
			var qtail []*parser.ASTNode

			// Get the children of the AST which do not produce the root nodes

			for i, c := range ast.Children {
				if c.Name != parser.NodeVALUE {
					qtail = ast.Children[i:]
					break
				}
			}

			// Now collect the keys which should be the new root nodes

			keys := make([]string, 0, len(rows))
			kind := ""

			for _, r := range rows {

				if r < rowCount {
					src := strings.Split(res.RowSource(r)[col], ":")

					keys = append(keys, src[2])
					if kind == "" {
						kind = src[1]
					}
				}
			}

			sort.Strings(keys)

			err = fmt.Errorf("Could not find requested row%v", stringutil.Plural(len(rows)))

			if len(keys) > 0 {

				// Assemble the query

				ast, _ = ParseQuery("", fmt.Sprintf("lookup %v '%v'", kind, strings.Join(keys, "', '")))
				ast.Children = append(ast.Children, qtail...)

				ret, err = parser.PrettyPrint(ast)
			}
		}
	}

	return ret, err
}
