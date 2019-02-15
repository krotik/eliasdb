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
	"errors"
	"io/ioutil"
	"math/rand"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"time"

	"devt.de/common/fileutil"
)

/*
CheckLocalRedirect checks if a given redirect URL is a local redirect.
The function returns an error in all other cases.
*/
func CheckLocalRedirect(urlString string) error {

	u, err := url.Parse(urlString)

	if err == nil && u.IsAbs() {
		err = errors.New("Redirection URL must not be an absolute URL")
	}

	return err
}

/*
singleFileHandler is a handler for a single file.
*/
type singleFileHandler struct {
	path       string
	errHandler func(err error)
}

/*
SingleFileServer returns a handler that serves all HTTP requests
with the contents of a single file.
*/
func SingleFileServer(path string, errHandler func(err error)) http.Handler {
	if errHandler == nil {
		errHandler = func(err error) {}
	}
	return &singleFileHandler{path, errHandler}
}

/*
ServeHTTP serves HTTP requests.
*/
func (f *singleFileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	ok, err := fileutil.PathExists(f.path)

	if ok {
		var content []byte

		ctype := mime.TypeByExtension(filepath.Ext(f.path))
		w.Header().Set("Content-Type", ctype)

		if content, err = ioutil.ReadFile(f.path); err == nil {
			if _, err = w.Write(content); err == nil {
				return
			}
		}
	}

	if err != nil {
		f.errHandler(err)
	}

	w.WriteHeader(http.StatusUnauthorized)
	w.Write([]byte("Unauthorized\n"))
}

/*
randomFileHandler is a handler for a random file.
*/
type randomFileHandler struct {
	*singleFileHandler
	paths []string
}

/*
RandomFileServer returns a handler that serves all HTTP requests
with the contents of a random file. The file is picked from a predefined
list.
*/
func RandomFileServer(paths []string, errHandler func(err error)) http.Handler {
	return &randomFileHandler{&singleFileHandler{"", errHandler}, paths}
}

/*
ServeHTTP serves HTTP requests.
*/
func (f *randomFileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rand.Seed(int64(time.Now().Nanosecond()))

	selected := f.paths[rand.Intn(len(f.paths))]
	f.singleFileHandler.path = selected

	f.singleFileHandler.ServeHTTP(w, r)
}
