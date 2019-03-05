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
	"strings"
	"testing"
	"time"
)

const InvalidFileName = "**" + string(0x0)

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

	// Test value retrival

	if res := ConfBool(config, "MemoryOnlyStorage"); res {
		t.Error("Unexpected result:", res)
		return
	}

	if res := ConfStr(config, "DatastoreLocation"); res != "db" {
		t.Error("Unexpected result:", res)
		return
	}

	if res, _ := PathExists(configFile); res {
		os.Remove(configFile)
	}

	// Check invalid config file

	configFile = "**" + string(0x0)

	_, err = LoadConfig(configFile, testDefaultConfig)
	if !strings.Contains(strings.ToLower(err.Error()), string(0)+": invalid argument") {
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

func TestPersistedConfig(t *testing.T) {
	testFile := "persist_tester.cfg"
	defer func() {
		os.Remove(testFile)
	}()

	// Test the most basic start and stop

	pt, err := NewWatchedConfig(testFile, testDefaultConfig, time.Millisecond)
	if err != nil {
		t.Error(err)
		return
	}

	v, ok, err := pt.GetValue("MemoryOnlyStorage")
	if !ok || err != nil || v != false {
		t.Error("Unexpected stored value:", v, ok, err)
		return
	}

	v, ok, err = pt.GetValue("foo")
	if ok || err != nil || v != nil {
		t.Error("Unexpected stored value:", v, ok, err)
		return
	}

	c, err := pt.GetConfig()
	if err != nil {
		t.Error("Unexpected result:", err)
		return
	}

	if len(c) != 2 {
		t.Error("Unexpected result:", c)
		return
	}

	ioutil.WriteFile(testFile, []byte(`{"MemoryOnlyStorage":true}`), 0644)

	time.Sleep(100 * time.Millisecond)

	v, ok, err = pt.GetValue("MemoryOnlyStorage")
	if !ok || err != nil || v != true {
		t.Error("Unexpected stored value:", v, ok, err)
		return
	}

	// Check error state

	pt.filename = InvalidFileName

	WatchedConfigErrRetries = 2

	time.Sleep(10 * time.Millisecond)

	_, _, err = pt.GetValue("MemoryOnlyStorage")
	if err == nil || err.Error() != "Could not sync config from disk: open **"+string(0)+": invalid argument" {
		t.Error("Unexpected stored value:", err)
		return
	}

	_, err = pt.GetConfig()
	if err == nil || err.Error() != "Could not sync config from disk: open **"+string(0)+": invalid argument" {
		t.Error("Unexpected stored value:", err)
		return
	}

	err = pt.Close()
	if err == nil || err.Error() != "Could not sync config from disk: open **"+string(0)+": invalid argument" {
		t.Error("Unexpected stored value:", err)
		return
	}

	pt, err = NewWatchedConfig(testFile, testDefaultConfig, time.Millisecond)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(100 * time.Millisecond)

	os.Remove(testFile)

	time.Sleep(100 * time.Millisecond)

	v, ok, err = pt.GetValue("MemoryOnlyStorage")
	if !ok || err != nil || v != true {
		t.Error("Unexpected stored value:", v, ok, err)
		return
	}

	err = pt.Close()
	if err != nil {
		t.Error("Unexpected stored value:", err)
		return
	}
}
