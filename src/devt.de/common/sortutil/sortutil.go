/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package sortutil contains common sorting definitions and utilities for sorting data.
*/
package sortutil

import (
	"fmt"
	"sort"
)

/*
Int64Slice is a special type implementing the sort interface for int64
*/
type Int64Slice []int64

func (p Int64Slice) Len() int           { return len(p) }
func (p Int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

/*
Int64s sorts a slice of int64s in increasing order.
*/
func Int64s(a []int64) { sort.Sort(Int64Slice(a)) }

/*
UInt64Slice is a special type implementing the sort interface for uint64
*/
type UInt64Slice []uint64

func (p UInt64Slice) Len() int           { return len(p) }
func (p UInt64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p UInt64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

/*
UInt64s sorts a slice of uint64s in increasing order.
*/
func UInt64s(a []uint64) { sort.Sort(UInt64Slice(a)) }

/*
AbstractSlice is a special type implementing the sort interface for interface{}
(Sorting is by string value)
*/
type AbstractSlice []interface{}

func (p AbstractSlice) Len() int           { return len(p) }
func (p AbstractSlice) Less(i, j int) bool { return fmt.Sprint(p[i]) < fmt.Sprint(p[j]) }
func (p AbstractSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

/*
InterfaceStrings sorts a slice of interface{} in increasing order by their string
values.
*/
func InterfaceStrings(a []interface{}) { sort.Sort(AbstractSlice(a)) }
