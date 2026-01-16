package journal

import (
	"bytes"
	"fmt"
	"testing"
)

func TestBasicWrite(t *testing.T) {
	s := NewMemStorage()
	w, _ := New(s, 1024)
	defer w.Close()

	seq, err := w.Write([]byte("foo"), []byte("bar"))
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 {
		t.Fatalf("first write got seq=%d, want 1", seq)
	}
}

func TestMultipleWrites(t *testing.T) {
	s := NewMemStorage()
	w, _ := New(s, 1024)
	defer w.Close()

	for i := 1; i <= 5; i++ {
		seq, _ := w.Write([]byte("hello"), []byte("world"))
		if seq != uint64(i) {
			t.Fatalf("write %d: seq=%d, want %d", i, seq, i)
		}
	}
}

func TestBatchWrite(t *testing.T) {
	s := NewMemStorage()
	w, _ := New(s, 1024)
	defer w.Close()

	entries := []Entry{
		{Key: []byte("alice"), Value: []byte("bob")},
		{Key: []byte("ping"), Value: []byte("pong")},
		{Key: []byte("fizz"), Value: []byte("buzz")},
	}

	seqs, err := w.WriteBatch(entries)
	if err != nil {
		t.Fatal(err)
	}

	if len(seqs) != 3 {
		t.Fatalf("batch returned %d seqs, want 3", len(seqs))
	}

	for i, seq := range seqs {
		if seq != uint64(i+1) {
			t.Fatalf("seqs[%d]=%d, want %d", i, seq, i+1)
		}
	}
}

func TestEmptyBatch(t *testing.T) {
	s := NewMemStorage()
	w, _ := New(s, 1024)
	defer w.Close()

	seqs, _ := w.WriteBatch([]Entry{})
	if len(seqs) != 0 {
		t.Fatal("empty batch should return no seqs")
	}
}

func TestReplay(t *testing.T) {
	s := NewMemStorage()
	w, _ := New(s, 1024)

	w.Write([]byte("biba"), []byte("boba"))
	w.Write([]byte("pewpew"), []byte("666"))
	w.Write([]byte("batman"), []byte("robin"))
	w.Sync()
	w.Close()

	// reopen and replay
	w2, _ := New(s, 1024)
	defer w2.Close()

	var got []*Entry
	w2.Replay(func(e *Entry) error {
		got = append(got, e)
		return nil
	})

	if len(got) != 3 {
		t.Fatalf("replayed %d entries, want 3", len(got))
	}

	if !bytes.Equal(got[0].Key, []byte("biba")) {
		t.Errorf("entry[0].key=%s, want biba", got[0].Key)
	}
	if !bytes.Equal(got[2].Value, []byte("robin")) {
		t.Errorf("entry[2].val=%s, want robin", got[2].Value)
	}
}

func TestReplayEmpty(t *testing.T) {
	s := NewMemStorage()
	w, _ := New(s, 1024)
	defer w.Close()

	count := 0
	w.Replay(func(e *Entry) error {
		count++
		return nil
	})

	if count != 0 {
		t.Fatalf("empty wal replayed %d entries", count)
	}
}

func TestReplayError(t *testing.T) {
	s := NewMemStorage()
	w, _ := New(s, 1024)

	w.Write([]byte("rock"), []byte("paper"))
	w.Write([]byte("scissors"), []byte("lizard"))
	w.Write([]byte("king"), []byte("gizzard"))
	w.Sync()
	w.Close()

	w2, _ := New(s, 1024)
	defer w2.Close()

	count := 0
	err := w2.Replay(func(e *Entry) error {
		count++
		if count == 2 {
			return fmt.Errorf("nope")
		}
		return nil
	})

	if err == nil {
		t.Fatal("expected error from replay")
	}
	if count != 2 {
		t.Fatalf("replayed %d entries before error, want 2", count)
	}
}

func TestSegmentRotation(t *testing.T) {
	s := NewMemStorage()
	w, _ := New(s, 100)
	defer w.Close()

	// write enough to trigger rotation
	for i := 0; i < 20; i++ {
		w.Write([]byte("yolo"), []byte("swag hashtag blessed fam"))
	}
	w.Sync()

	files, _ := s.List()
	if len(files) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(files))
	}
}

func TestReplayAcrossSegments(t *testing.T) {
	s := NewMemStorage()
	w, _ := New(s, 100)

	for i := 0; i < 20; i++ {
		w.Write([]byte("never"), []byte("gonna give you up"))
	}
	w.Sync()
	w.Close()

	w2, _ := New(s, 100)
	defer w2.Close()

	count := 0
	w2.Replay(func(e *Entry) error {
		count++
		return nil
	})

	if count != 20 {
		t.Fatalf("replayed %d entries, want 20", count)
	}
}

func TestSequenceAfterRestart(t *testing.T) {
	s := NewMemStorage()

	w, _ := New(s, 1024)
	w.Write([]byte("salt"), []byte("pepper"))
	w.Write([]byte("bread"), []byte("butter"))
	w.Sync()
	w.Close()

	w2, _ := New(s, 1024)
	defer w2.Close()

	seq, _ := w2.Write([]byte("peanut"), []byte("jelly"))
	if seq != 3 {
		t.Fatalf("next seq after restart should be 3, got %d", seq)
	}
}

func TestSync(t *testing.T) {
	s := NewMemStorage()
	w, _ := New(s, 1024)
	defer w.Close()

	w.Write([]byte("yip"), []byte("yap"))

	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
}

func TestLargeValue(t *testing.T) {
	s := NewMemStorage()
	w, _ := New(s, 1024*1024)
	defer w.Close()

	// chonky boi
	bigVal := make([]byte, 50000)
	for i := range bigVal {
		bigVal[i] = byte(i % 256)
	}

	seq, err := w.Write([]byte("chonky"), bigVal)
	if err != nil {
		t.Fatal(err)
	}
	w.Sync()
	w.Close()

	w2, _ := New(s, 1024*1024)
	defer w2.Close()

	found := false
	w2.Replay(func(e *Entry) error {
		if e.Seq == seq && bytes.Equal(e.Value, bigVal) {
			found = true
		}
		return nil
	})

	if !found {
		t.Fatal("large value not found in replay")
	}
}

func TestBatchAcrossSegments(t *testing.T) {
	s := NewMemStorage()
	w, _ := New(s, 100)
	defer w.Close()

	entries := make([]Entry, 10)
	for i := range entries {
		entries[i] = Entry{
			Key:   []byte("spam"),
			Value: []byte("eggs and spam"),
		}
	}

	seqs, err := w.WriteBatch(entries)
	if err != nil {
		t.Fatal(err)
	}

	if len(seqs) != 10 {
		t.Fatalf("batch returned %d seqs, want 10", len(seqs))
	}

	files, _ := s.List()
	if len(files) < 2 {
		t.Fatalf("batch should span segments, got %d files", len(files))
	}
}

func TestZeroSizeKey(t *testing.T) {
	s := NewMemStorage()
	w, _ := New(s, 1024)
	defer w.Close()

	seq, err := w.Write([]byte{}, []byte("nobody home"))
	if err != nil {
		t.Fatal(err)
	}
	w.Sync()
	w.Close()

	w2, _ := New(s, 1024)
	defer w2.Close()

	found := false
	w2.Replay(func(e *Entry) error {
		if e.Seq == seq && len(e.Key) == 0 {
			found = true
		}
		return nil
	})

	if !found {
		t.Fatal("empty key not replayed")
	}
}

func TestZeroSizeValue(t *testing.T) {
	s := NewMemStorage()
	w, _ := New(s, 1024)
	defer w.Close()

	seq, err := w.Write([]byte("404"), []byte{})
	if err != nil {
		t.Fatal(err)
	}
	w.Sync()
	w.Close()

	w2, _ := New(s, 1024)
	defer w2.Close()

	found := false
	w2.Replay(func(e *Entry) error {
		if e.Seq == seq && len(e.Value) == 0 {
			found = true
		}
		return nil
	})

	if !found {
		t.Fatal("empty value not replayed")
	}
}

func TestReplayOrder(t *testing.T) {
	s := NewMemStorage()
	w, _ := New(s, 1024)

	pairs := [][]string{
		{"one", "two"},
		{"red", "blue"},
		{"hot", "cold"},
		{"yes", "no"},
		{"left", "right"},
	}

	for _, p := range pairs {
		w.Write([]byte(p[0]), []byte(p[1]))
	}
	w.Sync()
	w.Close()

	w2, _ := New(s, 1024)
	defer w2.Close()

	var seqs []uint64
	w2.Replay(func(e *Entry) error {
		seqs = append(seqs, e.Seq)
		return nil
	})

	// should be in order
	for i := 0; i < len(seqs)-1; i++ {
		if seqs[i] >= seqs[i+1] {
			t.Fatalf("seqs not in order: %v", seqs)
		}
	}
}
