package user

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"

	"devt.de/common/httputil"
)

const TESTPORT = ":9090"

const TESTQUERYURL = "http://localhost" + TESTPORT + "/foo"

var handleCallback = func(w http.ResponseWriter, r *http.Request) {}

var handleFunction = func(w http.ResponseWriter, r *http.Request) {

	// Check if a valid session cookie is there

	session, _ := UserSessionManager.GetSession("", w, r, false)

	handleCallback(w, r)

	if session == nil {
		w.Write([]byte("Content"))
	} else {
		w.Write([]byte(fmt.Sprint("Content - User session: ", session.User())))
	}
}

func TestMain(m *testing.M) {
	flag.Parse()

	// Setup a simple webserver

	hs, wg := startServer()
	if hs == nil {
		return
	}

	// Make sure the webserver shuts down

	defer stopServer(hs, wg)

	// Register a simple content delivery function

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		// Call the wrapped handle function which then adds the authentication

		handleFunction(w, r)
	})

	// Run the tests

	res := m.Run()

	os.Exit(res)
}

func TestNoAuthNoSession(t *testing.T) {

	// By default there is no session and no authentication

	res, _ := sendTestRequest(TESTQUERYURL, "GET", nil, nil, nil)

	if res != "Content" {
		t.Error("Unexpected result:", res)
		return
	}

	// Trying to create an anonymous session should fail

	r, _ := http.NewRequest("GET", "", nil)
	_, err := UserSessionManager.GetSession("", nil, r, true)

	if err.Error() != "Cannot create a session without a user" {
		t.Error("Unexpected error:", err)
		return
	}
}

/*
Send a request to a HTTP test server
*/
func sendTestRequest(url string, method string, headers map[string]string,
	cookies []*http.Cookie, content []byte) (string, *http.Response) {

	var req *http.Request
	var err error

	// Create request

	if content != nil {
		req, err = http.NewRequest(method, url, bytes.NewBuffer(content))
	} else {
		req, err = http.NewRequest(method, url, nil)
	}

	// Add headers

	req.Header.Set("Content-Type", "application/json")

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	// Add cookies

	for _, v := range cookies {
		req.AddCookie(v)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	bodyStr := strings.Trim(string(body), " \n")

	// Try json decoding first

	out := bytes.Buffer{}
	err = json.Indent(&out, []byte(bodyStr), "", "  ")
	if err == nil {
		return out.String(), resp
	}

	// Just return the body

	return bodyStr, resp
}

/*
Start a HTTP test server.
*/
func startServer() (*httputil.HTTPServer, *sync.WaitGroup) {
	hs := &httputil.HTTPServer{}

	var wg sync.WaitGroup
	wg.Add(1)

	go hs.RunHTTPServer(TESTPORT, &wg)

	wg.Wait()

	// Server is started

	if hs.LastError != nil {
		panic(hs.LastError)
	}

	return hs, &wg
}

/*
Stop a started HTTP test server.
*/
func stopServer(hs *httputil.HTTPServer, wg *sync.WaitGroup) {

	if hs.Running == true {

		wg.Add(1)

		// Server is shut down

		hs.Shutdown()

		wg.Wait()

	} else {

		panic("Server was not running as expected")
	}
}
