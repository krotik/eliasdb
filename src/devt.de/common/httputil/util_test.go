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
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
)

const InvalidFileName = "**" + string(0x0)

/*
dummyResponse is a dummy object for http response testing
*/
type dummyResponse struct {
	out    *bytes.Buffer
	header map[string][]string
}

func (dr *dummyResponse) Header() http.Header {
	return dr.header
}

func (dr *dummyResponse) Write(b []byte) (int, error) {
	return dr.out.Write(b)
}

func (dr *dummyResponse) WriteHeader(int) {
}

func TestCheckLocalRedirect(t *testing.T) {

	// Check local redirects

	if err := CheckLocalRedirect("/foo/bar"); err != nil {
		t.Error(err)
		return
	}

	if err := CheckLocalRedirect("foo/bar"); err != nil {
		t.Error(err)
		return
	}

	if err := CheckLocalRedirect("x"); err != nil {
		t.Error(err)
		return
	}

	// Check absolute redirects

	if err := CheckLocalRedirect("http://hans.foo/bla"); err == nil || err.Error() != "Redirection URL must not be an absolute URL" {
		t.Error(err)
		return
	}

	if err := CheckLocalRedirect("file://hans.foo/bla"); err == nil || err.Error() != "Redirection URL must not be an absolute URL" {
		t.Error(err)
		return
	}

	if err := CheckLocalRedirect("://hans.foo/bla"); err == nil || err.Error() != "parse ://hans.foo/bla: missing protocol scheme" {
		t.Error(err)
		return
	}

	if err := CheckLocalRedirect("https:www.foo.co.uk"); err == nil || err.Error() != "Redirection URL must not be an absolute URL" {
		t.Error(err)
		return
	}

	if err := CheckLocalRedirect("https:3627733859"); err == nil || err.Error() != "Redirection URL must not be an absolute URL" {
		t.Error(err)
		return
	}
}

func TestSingleFileServer(t *testing.T) {

	ioutil.WriteFile("foo.txt", []byte("foo test"), 0666)
	defer os.Remove("foo.txt")

	sfs := SingleFileServer("foo.txt", nil)
	dr := &dummyResponse{&bytes.Buffer{}, make(map[string][]string)}

	sfs.ServeHTTP(dr, nil)

	if res := fmt.Sprint(dr.header); res != "map[Content-Type:[text/plain; charset=utf-8]]" {
		t.Error("Unexpected result:", res)
		return
	}

	if res := fmt.Sprint(dr.out); res != "foo test" {
		t.Error("Unexpected result:", res)
		return
	}

	sfs = SingleFileServer(InvalidFileName, nil)
	dr = &dummyResponse{&bytes.Buffer{}, make(map[string][]string)}

	sfs.ServeHTTP(dr, nil)

	if res := fmt.Sprint(dr.header); res != "map[]" {
		t.Error("Unexpected result:", res)
		return
	}

	if res := fmt.Sprint(dr.out); res != "Unauthorized\n" {
		t.Error("Unexpected result:", res)
		return
	}
}

func TestRandomFileServer(t *testing.T) {

	ioutil.WriteFile("foo.txt", []byte("foo test"), 0666)
	defer os.Remove("foo.txt")

	rfs := RandomFileServer([]string{"foo.txt", "foo.txt", "foo.txt"}, nil)
	dr := &dummyResponse{&bytes.Buffer{}, make(map[string][]string)}

	rfs.ServeHTTP(dr, nil)

	if res := fmt.Sprint(dr.header); res != "map[Content-Type:[text/plain; charset=utf-8]]" {
		t.Error("Unexpected result:", res)
		return
	}

	if res := fmt.Sprint(dr.out); res != "foo test" {
		t.Error("Unexpected result:", res)
		return
	}
}
