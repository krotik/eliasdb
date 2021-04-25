package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

const testconf = "testconfig"

const invalidFileName = "**" + "\x00"

func TestConfig(t *testing.T) {

	Config = nil

	ioutil.WriteFile(testconf, []byte(`{
    "EnableReadOnly": true
}`), 0644)

	defer func() {
		if err := os.Remove(testconf); err != nil {
			fmt.Print("Could not remove test config file:", err.Error())
		}
	}()

	if err := LoadConfigFile(testconf); err != nil {
		t.Error(err)
		return
	}

	if res := Str("EnableReadOnly"); res != "true" {
		t.Error("Unexpected result:", res)
		return
	}

	if res := Bool("EnableReadOnly"); !res {
		t.Error("Unexpected result:", res)
		return
	}

	if res := Int("HTTPSPort"); fmt.Sprint(res) != DefaultConfig[HTTPSPort] {
		t.Error("Unexpected result:", res)
		return
	}

	LoadDefaultConfig()

	if res := Str("EnableReadOnly"); res != "false" {
		t.Error("Unexpected result:", res)
		return
	}

	Config[HTTPSPort] = "123"

	if res := Int("HTTPSPort"); fmt.Sprint(res) == DefaultConfig[HTTPSPort] {
		t.Error("Unexpected result:", res)
		return
	}

	if res := WebPath("123", "456"); res != "web/123/456" {
		t.Error("Unexpected result:", res)
		return
	}
}
