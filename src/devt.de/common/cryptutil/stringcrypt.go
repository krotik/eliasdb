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
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
)

/*
EncryptString encrypts a given string using AES (cfb mode).
*/
func EncryptString(passphrase, text string) (string, error) {
	var ret []byte

	// Create a new cipher with the given key

	key := sha256.Sum256([]byte(passphrase))

	block, err := aes.NewCipher((&key)[:])

	if err == nil {

		// Base64 encode the string

		b := base64.StdEncoding.EncodeToString([]byte(text))

		ciphertext := make([]byte, aes.BlockSize+len(b))

		// Create the initialization vector using random numbers

		iv := ciphertext[:aes.BlockSize]

		if _, err = io.ReadFull(rand.Reader, iv); err == nil {

			// Do the encryption

			cfb := cipher.NewCFBEncrypter(block, iv)

			cfb.XORKeyStream(ciphertext[aes.BlockSize:], []byte(b))

			ret = ciphertext
		}
	}

	return string(ret), err
}

/*
DecryptString decrypts a given string using AES (cfb mode).
*/
func DecryptString(passphrase, text string) (string, error) {
	var ret []byte

	// Check encrypted text

	if len(text) < aes.BlockSize {
		return "", fmt.Errorf("Ciphertext is too short - must be at least: %v", aes.BlockSize)
	}

	// Create a new cipher with the given key

	key := sha256.Sum256([]byte(passphrase))

	block, err := aes.NewCipher((&key)[:])

	if err == nil {

		// Separate initialization vector and actual encrypted text

		iv := text[:aes.BlockSize]

		text = text[aes.BlockSize:]

		// Do the decryption

		cfb := cipher.NewCFBDecrypter(block, []byte(iv))

		ret = []byte(text) // Reuse text buffer

		cfb.XORKeyStream(ret, []byte(text))

		// Decode text from base64

		ret, err = base64.StdEncoding.DecodeString(string(ret))

		if err != nil {

			// Return a proper error if something went wrong

			ret = nil
			err = fmt.Errorf("Could not decrypt data")
		}
	}

	return string(ret), err
}
