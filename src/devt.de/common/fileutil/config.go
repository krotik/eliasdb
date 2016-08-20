/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
Package fileutil contains file based utilities and helper functions.

JSON based configuration files which can be loaded on startup.
*/
package fileutil

import (
	"encoding/json"
	"io/ioutil"
)

/*
LoadConfig loads or creates a config file. Missing settings from the config file
will be filled with default settings.
*/
func LoadConfig(filename string, defaultConfig map[string]interface{}) (map[string]interface{}, error) {
	var mdata []byte
	var data map[string]interface{}
	var err error
	var ok bool

	if ok, err = PathExists(filename); err != nil {
		return nil, err

	} else if ok {

		// Load config

		mdata, err = ioutil.ReadFile(filename)
		if err == nil {

			err = json.Unmarshal(mdata, &data)
			if err == nil {

				// Make sure all required configuration values are set

				for k, v := range defaultConfig {
					if dv, ok := data[k]; !ok || dv == nil {
						data[k] = v
					}
				}
			}
		}

	} else if err == nil {

		// Write config

		data = defaultConfig

		mdata, err = json.MarshalIndent(data, "", "    ")
		if err == nil {

			err = ioutil.WriteFile(filename, mdata, 0644)
		}
	}

	if err != nil {
		return nil, err
	}

	return data, nil
}
