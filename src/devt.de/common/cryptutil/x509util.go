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
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
)

/*
ReadX509CertsFromFile reads a list of pem encoded certificates from a given file.
*/
func ReadX509CertsFromFile(filename string) ([]*x509.Certificate, error) {
	var err error
	var certs []*x509.Certificate

	file, err := os.OpenFile(filename, os.O_RDONLY, 0660)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	certsString, err := ioutil.ReadAll(file)
	if err == nil {
		certs, err = ReadX509Certs(certsString)
	}

	return certs, err
}

/*
ReadX509Certs reads a list of pem encoded certificates from a byte array.
*/
func ReadX509Certs(certs []byte) ([]*x509.Certificate, error) {

	var blocks []byte

	for {
		var block *pem.Block

		block, certs = pem.Decode(certs)
		if block == nil {
			return nil, errors.New("PEM not parsed")
		}

		blocks = append(blocks, block.Bytes...)
		if len(certs) == 0 {
			break
		}
	}
	c, err := x509.ParseCertificates(blocks)
	if err != nil {
		return nil, err
	}

	return c, nil
}

/*
Sha1CertFingerprint computes a sha1 fingerprint for a certificate.
*/
func Sha1CertFingerprint(cert *x509.Certificate) string {
	return formatFingerprint(fmt.Sprintf("%x", sha1.Sum(cert.Raw)))
}

/*
Sha256CertFingerprint computes a sha256 fingerprint for a certificate.
*/
func Sha256CertFingerprint(cert *x509.Certificate) string {
	return formatFingerprint(fmt.Sprintf("%x", sha256.Sum256(cert.Raw)))
}

/*
Md5CertFingerprint computes a md5 fingerprint for a certificate.
*/
func Md5CertFingerprint(cert *x509.Certificate) string {
	return formatFingerprint(fmt.Sprintf("%x", md5.Sum(cert.Raw)))
}

/*
Format a given fingerprint string.
*/
func formatFingerprint(raw string) string {
	var buf bytes.Buffer

	for i, c := range raw {
		buf.WriteByte(byte(c))
		if (i+1)%2 == 0 && i != len(raw)-1 {
			buf.WriteByte(byte(':'))
		}
	}

	return buf.String()
}
