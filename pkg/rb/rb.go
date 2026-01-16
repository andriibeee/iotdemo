package rb

import (
	"iter"
	"sync"
)

type RingBuffer[T any] struct {
	mu  sync.RWMutex
	buf []T
	pos int // write pos
	len int // slots used
}

func New[T any](capacity int) *RingBuffer[T] {
	return &RingBuffer[T]{
		buf: make([]T, max(capacity, 1)),
	}
}

func (rb *RingBuffer[T]) Add(val T) (T, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	dropped := rb.buf[rb.pos]
	wasFull := rb.len == len(rb.buf)

	rb.buf[rb.pos] = val
	rb.pos = (rb.pos + 1) % len(rb.buf)

	if !wasFull {
		rb.len++
	}

	return dropped, wasFull
}

func (rb *RingBuffer[T]) All() iter.Seq[T] {
	return func(yield func(T) bool) {
		rb.mu.RLock()
		defer rb.mu.RUnlock()

		for i := 0; i < rb.len; i++ {
			// walk backwards from pos-1
			idx := (rb.pos - 1 - i + len(rb.buf)) % len(rb.buf)
			if !yield(rb.buf[idx]) {
				return
			}
		}
	}
}
