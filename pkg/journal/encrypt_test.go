package journal

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func randomKey(t *testing.T) []byte {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	return key
}

func TestEncryptDecrypt(t *testing.T) {
	enc, err := NewAESGCMEncryptor(randomKey(t))
	require.NoError(t, err)

	plaintext := []byte("never gonna give you up!")
	ciphertext, err := enc.Encrypt(plaintext)
	require.NoError(t, err)
	assert.NotEqual(t, plaintext, ciphertext)

	decrypted, err := enc.Decrypt(ciphertext)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decrypted)
}

func TestEncryptDecryptVariousSizes(t *testing.T) {
	enc, err := NewAESGCMEncryptor(randomKey(t))
	require.NoError(t, err)

	for _, size := range []int{0, 1, 16, 100, 1000, 10000} {
		plaintext := make([]byte, size)
		if size > 0 {
			rand.Read(plaintext)
		}

		ciphertext, err := enc.Encrypt(plaintext)
		require.NoError(t, err)

		decrypted, err := enc.Decrypt(ciphertext)
		require.NoError(t, err)
		assert.Len(t, decrypted, size)
		if size > 0 {
			assert.Equal(t, plaintext, decrypted)
		}
	}
}

func TestInvalidKeySize(t *testing.T) {
	for _, size := range []int{0, 16, 31, 33, 64} {
		_, err := NewAESGCMEncryptor(make([]byte, size))
		assert.ErrorIs(t, err, ErrInvalidKeySize)
	}

	_, err := NewAESGCMEncryptor(make([]byte, 32))
	assert.NoError(t, err)
}

func TestUniqueNonces(t *testing.T) {
	enc, err := NewAESGCMEncryptor(randomKey(t))
	require.NoError(t, err)

	seen := make(map[string]struct{})
	for i := 0; i < 100; i++ {
		ct, _ := enc.Encrypt([]byte("same"))
		nonce := string(ct[:12])
		assert.NotContains(t, seen, nonce, "duplicate nonce")
		seen[nonce] = struct{}{}
	}
}

func TestDecryptTooShort(t *testing.T) {
	enc, err := NewAESGCMEncryptor(randomKey(t))
	require.NoError(t, err)

	_, err = enc.Decrypt([]byte("short"))
	assert.ErrorIs(t, err, ErrCiphertextShort)
}

func TestDecryptTampered(t *testing.T) {
	enc, err := NewAESGCMEncryptor(randomKey(t))
	require.NoError(t, err)

	ct, _ := enc.Encrypt([]byte("secret"))
	ct[len(ct)-1] ^= 0xff

	_, err = enc.Decrypt(ct)
	assert.Error(t, err)
}
