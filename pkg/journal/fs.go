package journal

import (
	"io"
	"os"
	"path/filepath"
)

type FileStorage struct {
	dir string
}

func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &FileStorage{dir: dir}, nil
}

func (fs *FileStorage) Create(name string) (io.WriteCloser, error) {
	path := filepath.Join(fs.dir, name)
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
}

func (fs *FileStorage) Open(name string) (io.ReadCloser, error) {
	path := filepath.Join(fs.dir, name)
	return os.Open(path)
}

func (fs *FileStorage) OpenAppend(name string) (io.WriteCloser, int64, error) {
	path := filepath.Join(fs.dir, name)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, 0, err
	}
	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, err
	}
	return f, stat.Size(), nil
}

func (fs *FileStorage) List() ([]string, error) {
	files, err := filepath.Glob(filepath.Join(fs.dir, "*.wal"))
	if err != nil {
		return nil, err
	}
	names := make([]string, len(files))
	for i, f := range files {
		names[i] = filepath.Base(f)
	}
	return names, nil
}

func (fs *FileStorage) Sync(name string) error {
	path := filepath.Join(fs.dir, name)
	f, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
