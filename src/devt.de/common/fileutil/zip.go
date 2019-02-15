package fileutil

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

/*
UnzipFile extracts a given zip file into a given output folder.
*/
func UnzipFile(name string, dest string, overwrite bool) error {
	var f *os.File

	stat, err := os.Stat(name)

	if err == nil {

		if f, err = os.Open(name); err == nil {
			defer f.Close()

			err = UnzipReader(f, stat.Size(), dest, overwrite)
		}
	}

	return err
}

/*
UnzipReader extracts a given zip archive into a given output folder.
Size is the size of the archive.
*/
func UnzipReader(reader io.ReaderAt, size int64, dest string, overwrite bool) error {
	var rc io.ReadCloser

	r, err := zip.NewReader(reader, size)

	if err == nil {

		for _, f := range r.File {

			if rc, err = f.Open(); err == nil {
				var e bool

				fpath := filepath.Join(dest, f.Name)

				if e, _ = PathExists(fpath); e && !overwrite {
					err = fmt.Errorf("Path already exists: %v", fpath)

				} else if f.FileInfo().IsDir() {

					// Create folder

					err = os.MkdirAll(fpath, os.ModePerm)

				} else {
					var fdir string

					// Create File

					if lastIndex := strings.LastIndex(fpath, string(os.PathSeparator)); lastIndex > -1 {
						fdir = fpath[:lastIndex]
					}

					if err = os.MkdirAll(fdir, os.ModePerm); err == nil {
						f, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())

						if err == nil {
							_, err = io.Copy(f, rc)

							f.Close()
						}
					}
				}

				rc.Close()
			}

			if err != nil {
				break
			}
		}
	}

	return err
}
