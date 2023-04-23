// Package bip implements bipbuffer queue.
package bip

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

const (
	header     = 4
	metadata   = 4 << 10 // bytes in file at the end reserved for metadata.
	privateMod = 0o600
)

var (
	maxBytes uint32 = (1 << (uint(header) * 8)) - header // the biggest size of data can store.

	ErrMaxBytes         = fmt.Errorf("data more than max %d bytes", maxBytes)
	ErrMetadata         = fmt.Errorf("capacity must be non less than %d bytes to store metadata", metadata)
	ErrInsufficentSpace = errors.New("insufficient freespace")
)

type (
	// Buffer represent bipbuffer queue (lock-free ring-buffer). Implementation described in:
	// https://www.codeproject.com/articles/3479/the-bip-buffer-the-circular-buffer-with-a-twist#_articleTop
	// https://ferrous-systems.com/blog/lock-free-ring-buffer/
	Buffer struct {
		writerOffset uint32
		readerOffset uint32
		readerSize   uint32
		capacity     uint32

		// mutex for concurrent writes
		wm sync.Mutex
		// mutex for concurrent reads
		rm sync.Mutex

		data []byte

		writerOffsetStore     uint32
		readerOffsetStore     uint32
		readerSizeOffsetStore uint32
	}
	// BufferOps represent type to modify buffer behaviour.
	BufferOps func(*Buffer)
)

// DefaultBuffer config for default buffer.
func DefaultBuffer() func(*Buffer) {
	return func(b *Buffer) {
		b.capacity = 512
		b.data = make([]byte, b.capacity+metadata)
	}
}

// FromData config for init from buffer.
func FromData(data []byte) (func(*Buffer), error) {
	if len(data) <= metadata {
		return nil, ErrMetadata
	}

	return func(b *Buffer) {
		b.data = data
		b.capacity = uint32(len(b.data)) - metadata
	}, nil
}

// New allocates a buffer.
func New(ops ...BufferOps) *Buffer {
	out := &Buffer{}
	DefaultBuffer()(out)
	for _, o := range ops {
		o(out)
	}
	out.setupMetaOffsets()
	out.updateMeta()
	return out
}

// Reset resets reader.
func (b *Buffer) Reset() {
	b.rm.Lock()
	defer b.rm.Unlock()
	writerOffset := atomic.LoadUint32(&b.writerOffset)
	readerOffset := atomic.LoadUint32(&b.readerOffset)
	b.release(readerOffset, writerOffset)
}

// GetMeta gets buffer metadata.
func (b *Buffer) GetMeta() (writerOffset, readerOffset, readerSize uint32) {
	writerOffset = atomic.LoadUint32(&b.writerOffset)
	readerOffset = atomic.LoadUint32(&b.readerOffset)
	readerSize = atomic.LoadUint32(&b.readerSize)
	return
}

// Write writes to buffer.
func (b *Buffer) Write(p []byte) (n int, err error) {
	if uint32(len(p)) > maxBytes {
		return 0, ErrMaxBytes
	}
	b.wm.Lock()
	defer b.wm.Unlock()

	size := uint32(len(p) + header)
	data, writerOffset, newOffset, err := b.reserveWrite(size)
	if err != nil {
		return 0, err
	}
	binary.LittleEndian.PutUint32(data, size)
	copy(data[header:], p)
	b.commit(writerOffset, newOffset)
	return int(size - header), nil
}

// Read reads from buffer to p. On the reader's side the responsibility to allocate p of sufficient size for reading.
func (b *Buffer) Read(p []byte) (int, error) {
	b.rm.Lock()
	defer b.rm.Unlock()

	data, readerOffset, newOffset, err := b.reserveRead()
	if err != nil {
		return 0, err
	}

	copy(p, data)
	b.release(readerOffset, newOffset)
	return len(data), nil
}

// reserveWrite reserves space in the buffer for a memory write operation.
func (b *Buffer) reserveWrite(size uint32) ([]byte, uint32, uint32, error) {
	writerOffset := atomic.LoadUint32(&b.writerOffset)
	readerOffset := atomic.LoadUint32(&b.readerOffset)

	var (
		start     = writerOffset
		newOffset uint32
	)
	if writerOffset < readerOffset {
		if newOffset = writerOffset + size; newOffset >= readerOffset {
			return nil, 0, 0, ErrInsufficentSpace
		}
	} else if newOffset = writerOffset + size; newOffset > b.capacity {
		if size < readerOffset {
			start = 0
			newOffset = size
		} else {
			return nil, 0, 0, ErrInsufficentSpace
		}
	}
	return b.data[start:newOffset], writerOffset, newOffset, nil
}

// reserveRead reserves space in the buffer for a memory read operation.
func (b *Buffer) reserveRead() ([]byte, uint32, uint32, error) {
	writerOffset := atomic.LoadUint32(&b.writerOffset)
	readerOffset := atomic.LoadUint32(&b.readerOffset)
	readerSize := atomic.LoadUint32(&b.readerSize)

	if readerOffset == writerOffset {
		return nil, 0, 0, io.EOF
	}

	start := readerOffset
	if writerOffset < readerOffset && readerOffset == readerSize {
		start = 0
	}

	headlessStart := start + header
	newOffset := start + binary.LittleEndian.Uint32(b.data[start:headlessStart])
	if newOffset <= headlessStart || readerOffset < writerOffset && newOffset > writerOffset {
		b.release(start, writerOffset) // torn write. TODO: restore
		return nil, 0, 0, io.EOF
	}
	return b.data[headlessStart:newOffset], readerOffset, newOffset, nil
}

// commit commits space that has been written to the buffer and change reader's boundary.
func (b *Buffer) commit(writerOffset, newOffset uint32) {
	if newOffset < writerOffset {
		binary.LittleEndian.PutUint32(b.data[b.readerSizeOffsetStore:], writerOffset)
		atomic.StoreUint32(&b.readerSize, writerOffset)
	}
	binary.LittleEndian.PutUint32(b.data[b.writerOffsetStore:], newOffset)
	atomic.StoreUint32(&b.writerOffset, newOffset)
}

// release increments reader offset and updates metainfo.
func (b *Buffer) release(readerOffset, newOffset uint32) {
	if newOffset < readerOffset {
		binary.LittleEndian.PutUint32(b.data[b.readerSizeOffsetStore:], b.capacity)
		atomic.StoreUint32(&b.readerSize, b.capacity)
	}
	binary.LittleEndian.PutUint32(b.data[b.readerOffsetStore:], newOffset)
	atomic.StoreUint32(&b.readerOffset, newOffset)
}

// drain drains buffer.
func (b *Buffer) drain(ctx context.Context, drainAmount uint32) {
	var size uint32
	for i := uint32(0); i < drainAmount; i += size {
		if ctx.Err() != nil {
			return
		}
		data, readerOffset, newOffset, err := b.reserveRead()
		if err != nil {
			return
		}
		size = uint32(len(data))
		b.release(readerOffset, newOffset)
	}
}

// Drain tries to drain buffer.
func (b *Buffer) Drain(ctx context.Context, bytesAmount uint32) {
	if bytesAmount == 0 {
		return
	}
	b.rm.Lock()
	defer b.rm.Unlock()
	b.drain(ctx, bytesAmount)
}

// setupMetaOffsets setup offsets for meta info.
func (b *Buffer) setupMetaOffsets() {
	b.writerOffsetStore = b.capacity
	b.readerOffsetStore = b.capacity + 8
	b.readerSizeOffsetStore = b.capacity + 16
}

// updateMeta updates meta info.
func (b *Buffer) updateMeta() {
	b.writerOffset = binary.LittleEndian.Uint32(b.data[b.writerOffsetStore:])
	b.readerOffset = binary.LittleEndian.Uint32(b.data[b.readerOffsetStore:])
	b.readerSize = binary.LittleEndian.Uint32(b.data[b.readerSizeOffsetStore:])
	if b.readerSize == 0 {
		b.readerSize = b.capacity
	}
	binary.LittleEndian.PutUint32(b.data[b.readerSizeOffsetStore:], b.readerSize)
}
