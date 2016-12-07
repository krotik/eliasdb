/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package datautil

import (
	"encoding/gob"
	"os"
)

/*
PersistentMap is a persistent map storing string values. This implementation returns
more encoding / decoding errors since not all possible values are supported.
*/
type PersistentMap struct {
	filename string                 // File of the persistent map
	Data     map[string]interface{} // Data of the persistent map
}

/*
NewPersistentMap creates a new persistent map.
*/
func NewPersistentMap(filename string) (*PersistentMap, error) {
	pm := &PersistentMap{filename, make(map[string]interface{})}
	return pm, pm.Flush()
}

/*
LoadPersistentMap loads a persistent map from a file.
*/
func LoadPersistentMap(filename string) (*PersistentMap, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0660)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	pm := &PersistentMap{filename, make(map[string]interface{})}

	de := gob.NewDecoder(file)

	return pm, de.Decode(&pm.Data)
}

/*
Flush writes contents of the persistent map to the disk.
*/
func (pm *PersistentMap) Flush() error {
	file, err := os.OpenFile(pm.filename, os.O_CREATE|os.O_RDWR, 0660)
	if err != nil {
		return err
	}
	defer file.Close()

	en := gob.NewEncoder(file)

	return en.Encode(pm.Data)
}

/*
PersistentStringMap is a persistent map storing string values.
*/
type PersistentStringMap struct {
	filename string            // File of the persistent map
	Data     map[string]string // Data of the persistent map
}

/*
NewPersistentStringMap creates a new persistent map.
*/
func NewPersistentStringMap(filename string) (*PersistentStringMap, error) {
	pm := &PersistentStringMap{filename, make(map[string]string)}
	return pm, pm.Flush()
}

/*
LoadPersistentStringMap loads a persistent map from a file.
*/
func LoadPersistentStringMap(filename string) (*PersistentStringMap, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0660)
	if err != nil {
		return nil, err
	}

	pm := &PersistentStringMap{filename, make(map[string]string)}

	de := gob.NewDecoder(file)
	de.Decode(&pm.Data)

	return pm, file.Close()
}

/*
Flush writes contents of the persistent map to the disk.
*/
func (pm *PersistentStringMap) Flush() error {
	file, err := os.OpenFile(pm.filename, os.O_CREATE|os.O_RDWR, 0660)
	if err != nil {
		return err
	}

	en := gob.NewEncoder(file)
	en.Encode(pm.Data)

	return file.Close()
}
