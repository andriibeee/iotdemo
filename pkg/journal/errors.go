package journal

import "errors"

var (
	ErrBadChecksum      = errors.New("bad checksum")
	ErrInvalidKeySize   = errors.New("key must be 32 bytes")
	ErrCiphertextShort  = errors.New("ciphertext too short")
)
