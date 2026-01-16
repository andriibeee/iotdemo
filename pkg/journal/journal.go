package journal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"sync"
)

type Entry struct {
	Key   []byte
	Value []byte
	Seq   uint64
}

type Storage interface {
	Create(name string) (io.WriteCloser, error)
	Open(name string) (io.ReadCloser, error)
	OpenAppend(name string) (io.WriteCloser, int64, error)
	List() ([]string, error)
	Sync(name string) error
}

type Journal struct {
	mu        sync.RWMutex
	storage   Storage
	current   string
	writer    *bufio.Writer
	closer    io.Closer
	seq       uint64
	size      int64
	maxSize   int64
	segment   int
	encryptor Encryptor
}

// Option configures a Journal.
type Option func(*Journal)

// WithEncryptor sets the encryptor for WAL entries.
func WithEncryptor(enc Encryptor) Option {
	return func(j *Journal) {
		j.encryptor = enc
	}
}

func New(storage Storage, maxSize int64, opts ...Option) (*Journal, error) {
	if maxSize == 0 {
		maxSize = 64 * 1024 * 1024
	}

	w := &Journal{
		storage: storage,
		maxSize: maxSize,
	}

	for _, opt := range opts {
		opt(w)
	}

	if err := w.openLatest(); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *Journal) openLatest() error {
	names, err := w.storage.List()
	if err != nil {
		return err
	}

	if len(names) == 0 {
		return w.newSegment()
	}

	// find highest segment number
	latest := 0
	for _, name := range names {
		var n int
		if _, err := fmt.Sscanf(name, "%d.wal", &n); err == nil {
			if n > latest {
				latest = n
			}
		}
	}

	w.segment = latest
	name := segmentName(latest)

	// scan to get latest sequence
	if err := w.scan(name); err != nil {
		return err
	}

	// open for append
	wc, size, err := w.storage.OpenAppend(name)
	if err != nil {
		return err
	}

	w.current = name
	w.writer = bufio.NewWriter(wc)
	w.closer = wc
	w.size = size

	return nil
}

func (w *Journal) scan(name string) error {
	rc, err := w.storage.Open(name)
	if err != nil {
		return err
	}
	defer rc.Close()

	r := bufio.NewReader(rc)
	for {
		e, err := w.read(r)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if e.Seq > w.seq {
			w.seq = e.Seq
		}
	}

	return nil
}

func (w *Journal) newSegment() error {
	if w.closer != nil {
		if err := w.writer.Flush(); err != nil {
			return err
		}
		if err := w.storage.Sync(w.current); err != nil {
			return err
		}
		if err := w.closer.Close(); err != nil {
			return err
		}
	}

	w.segment++
	name := segmentName(w.segment)

	wc, err := w.storage.Create(name)
	if err != nil {
		return err
	}

	w.current = name
	w.writer = bufio.NewWriter(wc)
	w.closer = wc
	w.size = 0

	return nil
}

func segmentName(n int) string {
	return fmt.Sprintf("%06d.wal", n)
}

func (w *Journal) Write(key, value []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.seq++
	e := &Entry{
		Key:   key,
		Value: value,
		Seq:   w.seq,
	}

	if w.size >= w.maxSize {
		if err := w.newSegment(); err != nil {
			return 0, err
		}
	}

	n, err := w.write(w.writer, e)
	if err != nil {
		return 0, err
	}

	w.size += int64(n)
	return e.Seq, nil
}

func (w *Journal) WriteBatch(entries []Entry) ([]uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	seqs := make([]uint64, len(entries))

	for i := range entries {
		w.seq++
		entries[i].Seq = w.seq
		seqs[i] = w.seq

		if w.size >= w.maxSize {
			if err := w.newSegment(); err != nil {
				return nil, err
			}
		}

		n, err := w.write(w.writer, &entries[i])
		if err != nil {
			return nil, err
		}

		w.size += int64(n)
	}

	return seqs, nil
}

func (w *Journal) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.storage.Sync(w.current)
}

// Replay reads all journal entries and calls fn for each.
// Uses read lock to allow concurrent writes during replay.
// Caller should coordinate externally if write exclusion is needed.
func (w *Journal) Replay(fn func(*Entry) error) error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	names, err := w.storage.List()
	if err != nil {
		return err
	}

	sort.Strings(names)

	for _, name := range names {
		rc, err := w.storage.Open(name)
		if err != nil {
			continue
		}

		r := bufio.NewReader(rc)
		for {
			e, err := w.read(r)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = rc.Close()
				return err
			}
			if err := fn(e); err != nil {
				_ = rc.Close()
				return err
			}
		}
		rc.Close()
	}

	return nil
}

func (w *Journal) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var firstErr error
	if w.writer != nil {
		firstErr = w.writer.Flush()
	}
	if w.closer != nil {
		w.storage.Sync(w.current)
		if err := w.closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (j *Journal) write(w *bufio.Writer, e *Entry) (int, error) {
	keyLen := len(e.Key)
	valLen := len(e.Value)

	dataSize := 8 + 4 + keyLen + 4 + valLen
	data := make([]byte, dataSize)

	pos := 0
	binary.BigEndian.PutUint64(data[pos:], e.Seq)
	pos += 8

	binary.BigEndian.PutUint32(data[pos:], uint32(keyLen))
	pos += 4
	copy(data[pos:], e.Key)
	pos += keyLen

	binary.BigEndian.PutUint32(data[pos:], uint32(valLen))
	pos += 4
	copy(data[pos:], e.Value)

	if j.encryptor != nil {
		var err error
		data, err = j.encryptor.Encrypt(data)
		if err != nil {
			return 0, err
		}
	}

	crc := crc32.ChecksumIEEE(data)

	buf := make([]byte, 8+len(data))
	binary.BigEndian.PutUint32(buf[0:], uint32(len(data)))
	binary.BigEndian.PutUint32(buf[4:], crc)
	copy(buf[8:], data)

	return w.Write(buf)
}

func (j *Journal) read(r *bufio.Reader) (*Entry, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf)

	crcBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, crcBuf); err != nil {
		return nil, err
	}
	expectedCRC := binary.BigEndian.Uint32(crcBuf)

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	if crc32.ChecksumIEEE(data) != expectedCRC {
		return nil, ErrBadChecksum
	}

	if j.encryptor != nil {
		var err error
		data, err = j.encryptor.Decrypt(data)
		if err != nil {
			return nil, err
		}
	}

	pos := 0
	seq := binary.BigEndian.Uint64(data[pos:])
	pos += 8

	keyLen := binary.BigEndian.Uint32(data[pos:])
	pos += 4
	key := make([]byte, keyLen)
	copy(key, data[pos:pos+int(keyLen)])
	pos += int(keyLen)

	valLen := binary.BigEndian.Uint32(data[pos:])
	pos += 4
	val := make([]byte, valLen)
	copy(val, data[pos:])

	return &Entry{
		Key:   key,
		Value: val,
		Seq:   seq,
	}, nil
}
