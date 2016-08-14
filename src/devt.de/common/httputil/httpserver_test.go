/*
 * Public Domain Software
 *
 * I (Matthias Ladkau) am the author of the source code in this file.
 * I have placed the source code in this file in the public domain. 
 *
 * For further information see: http://creativecommons.org/publicdomain/zero/1.0/
 */

package httputil

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"html"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"devt.de/common/cryptutil"
	"devt.de/common/fileutil"
)

const CERTDIR = "certs"

const TESTPORT_HTTP = ":9090"
const TESTPORT_HTTPS = ":9091"

const INVALID_FILE_NAME = "**" + string(0x0)

func TestMain(m *testing.M) {
	flag.Parse()

	// Setup

	if res, _ := fileutil.PathExists(CERTDIR); res {
		os.RemoveAll(CERTDIR)
	}

	err := os.Mkdir(CERTDIR, 0770)
	if err != nil {
		fmt.Print("Could not create test directory:", err.Error())
		os.Exit(1)
	}

	// Run the tests

	res := m.Run()

	// Teardown

	err = os.RemoveAll(CERTDIR)
	if err != nil {
		fmt.Print("Could not remove test directory:", err.Error())
	}

	os.Exit(res)
}

func TestHTTPSServer(t *testing.T) {

	// Generate a certificate and private key

	err := cryptutil.GenCert(CERTDIR, "cert.pem", "key.pem", "localhost", "", 365*24*time.Hour, true, 2048, "")
	if err != nil {
		t.Error(err)
		return
	}

	// Add dummy handler

	http.HandleFunc("/httpsserver_test", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello over HTTPS, %q", html.EscapeString(r.URL.Path))
	})

	hs := &HTTPServer{}

	var wg sync.WaitGroup
	wg.Add(1)

	go hs.RunHTTPSServer(CERTDIR, "cert.pem", "key.pem", TESTPORT_HTTPS, &wg)

	wg.Wait()

	// HTTPS Server has started

	if hs.LastError != nil {
		t.Error(hs.LastError)
		return

	} else {

		// Check we can't start two servers

		var wg2 sync.WaitGroup
		hs2 := &HTTPServer{}

		wg2.Add(1)

		err := hs2.RunHTTPSServer(CERTDIR, "c.pem", "k.pem", TESTPORT_HTTPS, &wg2)
		if hs2.LastError == nil ||
			(hs2.LastError.Error() != "open certs/c.pem: no such file or directory" &&
			hs2.LastError.Error() != "open certs/c.pem: The system cannot find the file specified.") ||
			err != hs2.LastError {
			t.Error("Unexpected error return:", hs2.LastError)
			return
		}

		// Add again to wait group so we can try again

		wg2.Add(1)

		err = hs2.RunHTTPSServer(CERTDIR, "cert.pem", "key.pem", TESTPORT_HTTPS, &wg2)
		if hs2.LastError == nil || (hs2.LastError.Error() != "listen tcp "+TESTPORT_HTTPS+
			": bind: address already in use" && hs2.LastError.Error() != "listen tcp "+TESTPORT_HTTPS+
			": bind: Only one usage of each socket address (protocol/network address/port) is normally permitted.") ||
			err != hs2.LastError {
			t.Error("Unexpected error return:", hs2.LastError)
		}

		// Add to the wait group so we can wait for the shutdown

		wg.Add(1)

		// Send something to the server

		if res := sendTestHTTPSRequest(CERTDIR + "/cert.pem"); res != `Hello over HTTPS, "/httpsserver_test"` {
			t.Error("Unexpected request response:", res)
			return
		}

		// Server is shut down

		hs.Shutdown()

		if hs.Running == true {
			wg.Wait()
		} else {
			t.Error("Server was not running as expected")
		}
	}
}

func TestSignalling(t *testing.T) {

	// Add dummy handler

	http.HandleFunc("/httpserver_test", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, %q", html.EscapeString(r.URL.Path))
	})

	hs := &HTTPServer{}

	var wg sync.WaitGroup
	wg.Add(1)

	go hs.RunHTTPServer(TESTPORT_HTTP, &wg)

	wg.Wait()

	// Server is started

	if hs.LastError != nil {
		t.Error(hs.LastError)
		return

	} else {

		// Check we can't start two servers

		var wg2 sync.WaitGroup
		wg2.Add(1)
		hs2 := &HTTPServer{}
		err := hs2.RunHTTPServer(":9090", &wg2)
		if hs2.LastError == nil || (hs2.LastError.Error() != "listen tcp "+TESTPORT_HTTP+
			": bind: address already in use" && hs2.LastError.Error() != "listen tcp "+TESTPORT_HTTP+
			": bind: Only one usage of each socket address (protocol/network address/port) is normally permitted.") ||
			err != hs2.LastError {
			t.Error("Unexpected error return:", hs2.LastError)
		}

		// Add to the wait group so we can wait for the shutdown

		wg.Add(1)

		// Send something to the server

		if res := sendTestRequest(); res != `Hello, "/httpserver_test"` {
			t.Error("Unexpected request response:", res)
			return
		}

		// Check we can send other signals

		hs.signalling <- syscall.SIGHUP

		time.Sleep(time.Duration(50) * time.Millisecond)
		if hs.Running != true {
			t.Error("Server should still be running after sending wrong shutdown signal")
			return
		}

		// Server is shut down

		hs.Shutdown()

		if hs.Running == true {
			wg.Wait()
		} else {
			t.Error("Server was not running as expected")
		}
	}

	// Test listener panic

	originalListener, _ := net.Listen("tcp", TESTPORT_HTTP)
	sl := newSignalTCPListener(originalListener, originalListener.(*net.TCPListener), nil)

	go testUnknownSignalPanic(t, sl)
	sl.Signals <- -1
}

func testUnknownSignalPanic(t *testing.T, sl *signalTCPListener) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Sending an unknown signal did not cause a panic.")
		}
	}()
	sl.Accept()
}

func sendTestRequest() string {
	url := "http://localhost" + TESTPORT_HTTP + "/httpserver_test"

	var jsonStr = []byte(`{"msg":"Hello!"}`)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	req.Header.Set("X-Custom-Header", "myvalue")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	return string(body)
}

func sendTestHTTPSRequest(ca_cert string) string {

	// Build ca cert pool

	ca_pool := x509.NewCertPool()
	serverCert, err := ioutil.ReadFile(ca_cert)
	if err != nil {
		panic(err)
	}
	ca_pool.AppendCertsFromPEM(serverCert)

	tr := &http.Transport{
		TLSClientConfig:    &tls.Config{RootCAs: ca_pool},
		DisableCompression: true,
	}

	url := "https://localhost" + TESTPORT_HTTPS + "/httpsserver_test"

	var jsonStr = []byte(`{"msg":"Hello!"}`)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	req.Header.Set("X-Custom-Header", "myvalue")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Transport: tr}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	return string(body)
}
