/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package fileutil

import (
	"io/ioutil"
	"os"
	"testing"
)

var testDefaultConfig = map[string]interface{}{
	"MemoryOnlyStorage": false,
	"DatastoreLocation": "db",
}

func TestLoadingConfig(t *testing.T) {

	configFile := "test.config.json"

	if res, _ := PathExists(configFile); res {
		os.Remove(configFile)
	}

	// Test config creation

	config, err := LoadConfig(configFile, testDefaultConfig)
	if err != nil {
		t.Error(err)
		return
	}

	if res, _ := PathExists(configFile); !res {
		t.Error("Config should have been created")
		return
	}

	// We should have now created a default config file

	compareConfig(t, config, testDefaultConfig)

	// Test reload of config creation

	config, err = LoadConfig(configFile, testDefaultConfig)
	if err != nil {
		t.Error(err)
		return
	}

	compareConfig(t, config, testDefaultConfig)

	ioutil.WriteFile(configFile, []byte("{ \"wrong"), 0644)

	_, err = LoadConfig(configFile, testDefaultConfig)
	if err.Error() != "unexpected end of JSON input" {
		t.Error(err)
		return
	}

	// Write partial config - Make sure all is loaded

	ioutil.WriteFile(configFile, []byte(`{"MemoryOnlyStorage":false}`), 0644)

	config, err = LoadConfig(configFile, testDefaultConfig)
	if err != nil {
		t.Error(err)
		return
	}

	// Check that the default values have been added

	compareConfig(t, config, testDefaultConfig)

	if res, _ := PathExists(configFile); res {
		os.Remove(configFile)
	}

	// Check invalid config file

	configFile = "**" + string(0x0)

	_, err = LoadConfig(configFile, testDefaultConfig)
	if err.Error() != "stat **"+string(0)+": invalid argument" &&
		err.Error() != "Lstat **"+string(0)+": invalid argument" {
		t.Error(err)
		return
	}
}

func compareConfig(t *testing.T, config1 map[string]interface{}, config2 map[string]interface{}) {
	if len(config1) != len(config2) {
		t.Error("Given config has different elements to loaded config:",
			config1, config2)
		return
	}
	for k, v := range config1 {
		if v != config2[k] {
			t.Error("Different values for:", k, " -> ", v, "vs", config2[k])
			return
		}
	}
}
