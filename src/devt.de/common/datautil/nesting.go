/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package datautil

import "fmt"

/*
GetNestedValue gets a value from a nested object structure.
*/
func GetNestedValue(d map[string]interface{}, path []string) (interface{}, error) {
	var ret interface{}
	var err error

	getNestedMap := func(d map[string]interface{}, key string) (map[string]interface{}, error) {
		val := d[key]
		newMap, ok := val.(map[string]interface{})

		if !ok {
			return nil, fmt.Errorf("Unexpected data type %T as value of %v", val, key)
		}

		return newMap, nil
	}

	// Drill into the object structure and return the requested value.

	nestedMap := d
	atomLevel := len(path) - 1

	for i, elem := range path {

		if i < atomLevel {

			if nestedMap, err = getNestedMap(nestedMap, elem); err != nil {
				break
			}

		} else {

			ret = nestedMap[elem]
		}
	}

	return ret, err
}
