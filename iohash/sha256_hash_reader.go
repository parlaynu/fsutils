package iohash

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
)

type Sha256HashReader struct {
	in io.Reader
	h  hash.Hash
}

func NewSha256HashReader(in io.Reader) *Sha256HashReader {
	return &Sha256HashReader{in, sha256.New()}
}

func (hasher *Sha256HashReader) Read(p []byte) (int, error) {
	// read data into the buffer
	sz, err := hasher.in.Read(p)
	if err != nil && err != io.EOF {
		return sz, err
	}

	// add this new data to the hasher
	hasher.h.Write(p[:sz])

	// and return
	return sz, err
}

func (hasher *Sha256HashReader) Hash() string {
	return hex.EncodeToString(hasher.h.Sum(nil))
}
