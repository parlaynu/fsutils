package iohash

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
)

type Sha256HashWriter struct {
	in io.Writer
	h  hash.Hash
}

func NewSha256HashWriter(in io.Writer) *Sha256HashWriter {
	return &Sha256HashWriter{in, sha256.New()}
}

func (hasher *Sha256HashWriter) Write(p []byte) (int, error) {
	// read data into the buffer
	sz, err := hasher.in.Write(p)
	if err != nil && err != io.EOF {
		return sz, err
	}

	// add this new data to the hasher
	hasher.h.Write(p[:sz])

	// and return
	return sz, err
}

func (hasher *Sha256HashWriter) Hash() string {
	return hex.EncodeToString(hasher.h.Sum(nil))
}
