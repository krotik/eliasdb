/*
Package cryptutil contains cryptographic utility functions.

Certificate generation code based on:
go source src/crypto/tls/generate_cert.go

Copyright 2009 The Go Authors. All rights reserved.
Use of this source code is governed by a BSD-style license.
*/
package cryptutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"strings"
	"time"
)

/*
GenCert generates certificate files in a given path.

path       - Path to generate the certificate in.
certFile   - Certificate file to generate.
keyFile    - Key file to generate.
host       - Comma-separated hostnames and IPs to generate a certificate for.
validFrom  - Creation date formatted as Jan 1 15:04:05 2011. Default is empty string which means now.
validFor   - Duration that certificate is valid for. Default is 365*24*time.Hour.
isCA       - Flag whether this cert should be its own Certificate Authority.
rsaBits    - Size of RSA key to generate. Ignored if ecdsa-curve is set. Default is 2048.
ecdsaCurve - ECDSA curve to use to generate a key. Valid values are P224, P256, P384, P521 or empty string (not set).
*/
func GenCert(path string, certFile string, keyFile string, host string,
	validFrom string, validFor time.Duration, isCA bool, rsaBits int, ecdsaCurve string) error {

	var err error

	// Check parameters

	if path != "" && !strings.HasSuffix(path, "/") {
		path += "/"
	}

	if host == "" {
		return errors.New("Host required for certificate generation")
	}

	var notBefore time.Time

	if validFrom == "" {
		notBefore = time.Now()
	} else {
		notBefore, err = time.Parse("Jan 2 15:04:05 2006", validFrom)
		if err != nil {
			return fmt.Errorf("Failed to parse creation date: %s", err)
		}
	}

	notAfter := notBefore.Add(validFor)

	// Generate private key

	var priv interface{}

	switch ecdsaCurve {
	case "":
		priv, err = rsa.GenerateKey(rand.Reader, rsaBits)
	case "P224":
		priv, err = ecdsa.GenerateKey(elliptic.P224(), rand.Reader)
	case "P256":
		priv, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	case "P384":
		priv, err = ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	case "P521":
		priv, err = ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	default:
		err = fmt.Errorf("Unrecognized elliptic curve: %q", ecdsaCurve)
	}

	if err != nil {
		return fmt.Errorf("Failed to generate private key: %s", err)
	}

	// Generate serial random number

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, _ := rand.Int(rand.Reader, serialNumberLimit)

	// Create and populate the certificate template

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"None"},
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// Add hosts

	hosts := strings.Split(host, ",")
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, h)
		}
	}

	// Set the CA flag

	if isCA {
		template.IsCA = isCA
		template.KeyUsage |= x509.KeyUsageCertSign
	}

	// Create the certificate and write it out

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, publicKey(priv), priv)

	if err == nil {

		certOut, err := os.Create(path + certFile)
		defer certOut.Close()

		if err != nil {
			return fmt.Errorf("Failed to open %s for writing: %s", certFile, err)
		}

		pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

		// Write out private key

		keyOut, err := os.OpenFile(path+keyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		defer keyOut.Close()

		if err != nil {
			return fmt.Errorf("Failed to open %v for writing: %v", keyFile, err)
		}

		pem.Encode(keyOut, pemBlockForKey(priv))
	}

	return err
}

/*
Return public key from a given key pair.
*/
func publicKey(priv interface{}) interface{} {
	switch k := priv.(type) {
	case *rsa.PrivateKey:
		return &k.PublicKey
	case *ecdsa.PrivateKey:
		return &k.PublicKey
	default:
		return nil
	}
}

/*
Return private key pem block for a given key pair.
*/
func pemBlockForKey(priv interface{}) *pem.Block {
	switch k := priv.(type) {
	case *rsa.PrivateKey:
		return &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)}
	case *ecdsa.PrivateKey:
		b, _ := x509.MarshalECPrivateKey(k)
		return &pem.Block{Type: "EC PRIVATE KEY", Bytes: b}
	default:
		return nil
	}
}
