/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain.
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package cryptutil

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"devt.de/common/fileutil"
)

const certDir = "certs"

const invalidFileName = "**" + string(0x0)

func TestMain(m *testing.M) {
	flag.Parse()

	// Setup
	if res, _ := fileutil.PathExists(certDir); res {
		os.RemoveAll(certDir)
	}

	err := os.Mkdir(certDir, 0770)
	if err != nil {
		fmt.Print("Could not create test directory:", err.Error())
		os.Exit(1)
	}

	// Run the tests
	res := m.Run()

	// Teardown
	err = os.RemoveAll(certDir)
	if err != nil {
		fmt.Print("Could not remove test directory:", err.Error())
	}

	os.Exit(res)
}

func TestGenCert(t *testing.T) {

	checkGeneration := func(ecdsaCurve string) error {

		// Generate a certificate and private key

		err := GenCert(certDir, "cert.pem", "key.pem", "localhost,127.0.0.1", "", 365*24*time.Hour, true, 2048, ecdsaCurve)
		if err != nil {
			return err
		}

		// Check that the files were generated

		if ok, _ := fileutil.PathExists(certDir + "/key.pem"); !ok {
			return errors.New("Private key was not generated")
		}

		if ok, _ := fileutil.PathExists(certDir + "/cert.pem"); !ok {
			return errors.New("Certificate was not generated")
		}

		_, err = ReadX509CertsFromFile(certDir + "/cert.pem")
		if err != nil {
			return err
		}

		return nil
	}

	if err := checkGeneration(""); err != nil {
		t.Error(err)
		return
	}

	if err := checkGeneration("P224"); err != nil {
		t.Error(err)
		return
	}

	if err := checkGeneration("P256"); err != nil {
		t.Error(err)
		return
	}

	if err := checkGeneration("P384"); err != nil {
		t.Error(err)
		return
	}

	if err := checkGeneration("P521"); err != nil {
		t.Error(err)
		return
	}

	// Test error cases

	err := GenCert(certDir, "cert.pem", "key.pem", "", "", 365*24*time.Hour, true, 2048, "")
	if err.Error() != "Host required for certificate generation" {
		t.Error(err)
		return
	}

	err = GenCert(certDir, "cert.pem", "key.pem", "localhost", "", 365*24*time.Hour, true, 2048, "xxx")
	if err.Error() != `Failed to generate private key: Unrecognized elliptic curve: "xxx"` {
		t.Error(err)
		return
	}

	err = GenCert(certDir, "cert.pem", "key.pem", "localhost", "xxx", 365*24*time.Hour, true, 2048, "")
	if err.Error() != `Failed to parse creation date: parsing time "xxx" as "Jan 2 15:04:05 2006": cannot parse "xxx" as "Jan"` {
		t.Error(err)
		return
	}

	err = GenCert(certDir, "cert.pem", invalidFileName, "localhost", "", 365*24*time.Hour, true, 2048, "")
	if !strings.HasPrefix(err.Error(), "Failed to open") {
		t.Error(err)
		return
	}

	err = GenCert(certDir, invalidFileName, "key.pem", "localhost", "", 365*24*time.Hour, true, 2048, "")
	if !strings.HasPrefix(err.Error(), "Failed to open") {
		t.Error(err)
		return
	}

	if publicKey(nil) != nil {
		t.Error("Unexpected result")
		return
	}

	if pemBlockForKey(nil) != nil {
		t.Error("Unexpected result")
		return
	}
}
