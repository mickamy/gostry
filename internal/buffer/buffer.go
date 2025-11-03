package buffer

import (
	"sync"
)

// Buffer collects entries within a transaction.
type Buffer[T any] struct {
	mu sync.Mutex
	ts []T
}

func NewBuffer[T any]() *Buffer[T] {
	return &Buffer[T]{}
}

func (b *Buffer[T]) Add(t T) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ts = append(b.ts, t)
}

func (b *Buffer[T]) Drain() []T {
	b.mu.Lock()
	ts := b.ts
	b.ts = nil
	b.mu.Unlock()
	return ts
}

func (b *Buffer[T]) Reset() {
	b.Drain()
}
