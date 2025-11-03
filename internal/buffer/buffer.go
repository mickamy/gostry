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

func (b *Buffer[T]) Add(e T) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ts = append(b.ts, e)
}

func (b *Buffer[T]) Drain() []T {
	b.mu.Lock()
	es := b.ts
	b.ts = nil
	b.mu.Unlock()
	return es
}

func (b *Buffer[T]) Reset() {
	b.Drain()
}
