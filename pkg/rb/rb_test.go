package rb_test

import (
	"testing"

	"github.com/andriibeee/iotdemo/pkg/rb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func collect[T any](r *rb.RingBuffer[T]) []T {
	var res []T
	for v := range r.All() {
		res = append(res, v)
	}
	return res
}

func TestAdd(t *testing.T) {
	r := rb.New[int](3)

	r.Add(1)
	assert.Equal(t, []int{1}, collect(r))

	r.Add(2)
	assert.Equal(t, []int{2, 1}, collect(r))

	r.Add(3)
	assert.Equal(t, []int{3, 2, 1}, collect(r))

	removed, evicted := r.Add(4)
	assert.True(t, evicted)
	assert.Equal(t, 1, removed)
	assert.Equal(t, []int{4, 3, 2}, collect(r))

	r.Add(5)
	r.Add(6)
	assert.Equal(t, []int{6, 5, 4}, collect(r))
}

func TestIterEmpty(t *testing.T) {
	r := rb.New[int](3)
	assert.Empty(t, collect(r))
}

func TestIterPartial(t *testing.T) {
	r := rb.New[int](5)
	r.Add(1)
	r.Add(2)
	r.Add(3)
	assert.Equal(t, []int{3, 2, 1}, collect(r))
}

func TestIterFull(t *testing.T) {
	r := rb.New[int](3)
	r.Add(1)
	r.Add(2)
	r.Add(3)
	assert.Equal(t, []int{3, 2, 1}, collect(r))
}

func TestIterWrapped(t *testing.T) {
	r := rb.New[int](3)
	r.Add(1)
	r.Add(2)
	r.Add(3)
	r.Add(4)
	r.Add(5)
	assert.Equal(t, []int{5, 4, 3}, collect(r))
}

func TestIterEarlyBreak(t *testing.T) {
	r := rb.New[int](5)
	for i := 1; i <= 5; i++ {
		r.Add(i)
	}

	var got []int
	for v := range r.All() {
		got = append(got, v)
		if v == 3 {
			break
		}
	}
	assert.Equal(t, []int{5, 4, 3}, got)

	r.Add(6)
	assert.Equal(t, []int{6, 5, 4, 3, 2}, collect(r))
}

func TestZeroCapacity(t *testing.T) {
	r := rb.New[int](0)
	r.Add(1)
	r.Add(2)
	assert.Len(t, collect(r), 1)
}

func TestEviction(t *testing.T) {
	r := rb.New[string](2)

	_, evicted := r.Add("a")
	assert.False(t, evicted)

	_, evicted = r.Add("b")
	assert.False(t, evicted)

	removed, evicted := r.Add("c")
	require.True(t, evicted)
	assert.Equal(t, "a", removed)

	removed, evicted = r.Add("d")
	require.True(t, evicted)
	assert.Equal(t, "b", removed)
}
