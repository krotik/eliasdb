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
	"strings"
	"testing"
)

func TestCertificateDecoding(t *testing.T) {

	_, err := ReadX509CertsFromFile(invalidFileName)
	if err == nil {
		t.Error("Attempting to load an invalid file should result in an error")
		return
	}

	googleCert := `
-----BEGIN CERTIFICATE-----
MIIEgDCCA2igAwIBAgIIORWTXMrZJggwDQYJKoZIhvcNAQELBQAwSTELMAkGA1UE
BhMCVVMxEzARBgNVBAoTCkdvb2dsZSBJbmMxJTAjBgNVBAMTHEdvb2dsZSBJbnRl
cm5ldCBBdXRob3JpdHkgRzIwHhcNMTYwNzEzMTMxODU2WhcNMTYxMDA1MTMxNjAw
WjBoMQswCQYDVQQGEwJVUzETMBEGA1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwN
TW91bnRhaW4gVmlldzETMBEGA1UECgwKR29vZ2xlIEluYzEXMBUGA1UEAwwOd3d3
Lmdvb2dsZS5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDkNYMd
9AGxMuv6wC7XBkzi6G7l+jqq+xoxs3zW+8jmGntRh/ggnTNLTQiwLPquusGbPo4n
bVX2UQV7ATyWeg7WZQuVjgeeF7WG++xwtLUtW3noSCmePSasWx0mcJu2tiuMWqsm
PbR08k14tz4jiqmRDQQfttffVS1wk0Oul6+x7hN8AyZ24gUWzb+L5ILA+8CtsZB/
u9XFtf+yEr277J7vH7GyEJxYt3u2dxy/nrNlF8o2wUl+U1bvUnQVRPNiFXLK2uiQ
4XkL7F3Uk19q09snjHcOixYHSYgyGYATCfV/d6hQ+RSKzd7TQp/YHtT1LgmUUefH
Hu04LXVnuhKUYYZnAgMBAAGjggFLMIIBRzAdBgNVHSUEFjAUBggrBgEFBQcDAQYI
KwYBBQUHAwIwGQYDVR0RBBIwEIIOd3d3Lmdvb2dsZS5jb20waAYIKwYBBQUHAQEE
XDBaMCsGCCsGAQUFBzAChh9odHRwOi8vcGtpLmdvb2dsZS5jb20vR0lBRzIuY3J0
MCsGCCsGAQUFBzABhh9odHRwOi8vY2xpZW50czEuZ29vZ2xlLmNvbS9vY3NwMB0G
A1UdDgQWBBRU6a8Q+y3AwMTsYpTXqT+xJ6n9bzAMBgNVHRMBAf8EAjAAMB8GA1Ud
IwQYMBaAFErdBhYbvPZotXb1gba7Yhq6WoEvMCEGA1UdIAQaMBgwDAYKKwYBBAHW
eQIFATAIBgZngQwBAgIwMAYDVR0fBCkwJzAloCOgIYYfaHR0cDovL3BraS5nb29n
bGUuY29tL0dJQUcyLmNybDANBgkqhkiG9w0BAQsFAAOCAQEAiw4H269LfRl/Vrm6
BmTCS5ipvbE6qMbwdB++eA/NaHU29bbFzRIRIo7T6nHynAE6QTUS0fRoZ2bnoaxY
Z98hSqnPlpDC3D2IImcrSywIejS0aFcT6UZT57QUm7iANDs3N7XHsXXLT0wrvXZS
GPKxS2JtOS3J5lRoN4fbYLuAHEzBn7zAqtrd98EEaYGdDerMo8kAyIDHqV4OiukI
YkefRqQpi1B8hPFuFw8KDGuAHdfHOoUmuRo4yxs5Br7FhoLLtdN+5UD3tbWYGZo4
9dl+K2ZqYOiNIHSTg78YaLM2s82G0WcL3oSzZg/ne+HZdhTu2YNFbGnoBIrgPjiP
TV6Wsg==
-----END CERTIFICATE-----
`

	c, err := ReadX509Certs([]byte(googleCert))
	if err != nil {
		t.Error(err)
		return
	}

	if len(c) != 1 {
		t.Error("Only one certificate should have been read")
		return
	}

	if res := Sha256CertFingerprint(c[0]); res != "d0:88:88:3c:7b:b3:da:b4:9e:d8:bf:ec:43:aa:92:cb:29:58:e8:e2:e1:c3:89:8d:73:50:6a:b8:c8:f1:12:21" {
		t.Error("Unexpected fingerprint:", res)
		return
	}

	if res := Sha1CertFingerprint(c[0]); res != "ee:b6:d4:d8:88:e5:75:5f:ff:c0:19:27:b6:67:9c:77:e8:0d:2c:7f" {
		t.Error("Unexpected fingerprint:", res)
		return
	}

	if res := Md5CertFingerprint(c[0]); res != "5c:a6:bd:96:9c:96:79:a7:90:ee:89:a6:ee:1a:04:a8" {
		t.Error("Unexpected fingerprint:", res)
		return
	}

	// Test error cases

	_, err = ReadX509Certs([]byte(googleCert[2:]))
	if err.Error() != "PEM not parsed" {
		t.Error("PEM parsing error expected:", err)
		return
	}

	_, err = ReadX509Certs([]byte(googleCert[0:29] + "Mi" + googleCert[31:]))
	if strings.HasPrefix("asn1: structure error", err.Error()) {
		t.Error("asn1 parsing error expected:", err)
		return
	}
}
