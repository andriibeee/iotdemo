package journal

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
)

type MemStorage struct {
	mu    sync.Mutex
	files map[string]*memFile
}

type memFile struct {
	data   *bytes.Buffer
	closed bool
}

func NewMemStorage() *MemStorage {
	return &MemStorage{
		files: make(map[string]*memFile),
	}
}

func (ms *MemStorage) Create(name string) (io.WriteCloser, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.files[name]; exists {
		return nil, fmt.Errorf("file exists")
	}

	mf := &memFile{data: &bytes.Buffer{}}
	ms.files[name] = mf
	return &memWriter{ms: ms, name: name, mf: mf}, nil
}

func (ms *MemStorage) Open(name string) (io.ReadCloser, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	mf, exists := ms.files[name]
	if !exists {
		return nil, fmt.Errorf("file not found")
	}

	return io.NopCloser(bytes.NewReader(mf.data.Bytes())), nil
}

func (ms *MemStorage) OpenAppend(name string) (io.WriteCloser, int64, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	mf, exists := ms.files[name]
	if !exists {
		return nil, 0, fmt.Errorf("file not found")
	}

	size := int64(mf.data.Len())
	return &memWriter{ms: ms, name: name, mf: mf}, size, nil
}

func (ms *MemStorage) List() ([]string, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	names := make([]string, 0, len(ms.files))
	for name := range ms.files {
		names = append(names, name)
	}
	return names, nil
}

func (ms *MemStorage) Sync(name string) error {
	return nil
}

type memWriter struct {
	ms   *MemStorage
	name string
	mf   *memFile
}

var ErrClosed = errors.New("memWriter: closed")

func (mw *memWriter) Write(p []byte) (int, error) {
	if mw.mf.closed {
		return 0, ErrClosed
	}
	return mw.mf.data.Write(p)
}

func (mw *memWriter) Close() error {
	mw.mf.closed = true
	return nil
}
