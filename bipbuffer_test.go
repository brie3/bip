package bip

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

type test []byte

var tests = []test{
	[]byte("0123"),
	[]byte("456789"),
	[]byte("987"),
	[]byte("6543210"),
}

func setupTest(t *testing.T, dir string, size int) *Buffer {
	var buf *Buffer
	ops, err := FromData(make([]byte, size))
	assert.NoError(t, err)
	buf = New(ops)

	return buf
}

func TestRead(t *testing.T) {
	t.Parallel()
	t.Run("empty", func(t *testing.T) {
		b := setupTest(t, os.TempDir(), len(tests[0])+metadata)
		_, err := b.Read(make([]byte, len(tests[0])))
		assert.Error(t, err)
	})

	t.Run("with big file", func(t *testing.T) {
		b := setupTest(t, os.TempDir(), len(tests[0])-1+metadata)

		_, err := b.Write(tests[0])
		assert.Error(t, err)
	})
	t.Run("read to cap", func(t *testing.T) {
		size := 5 * len(tests[1])
		b := setupTest(t, os.TempDir(), size+metadata)

		for _, test := range tests[:3] {
			_, err := b.Write(test)
			assert.NoError(t, err)
		}
		actual := make([]byte, size/2)
		for _, test := range tests[:3] {
			n, err := b.Read(actual)
			assert.NoError(t, err)
			assert.Equal(t, string(test), string(actual[:n]))
		}
		_, err := b.Read(actual)
		assert.Error(t, err)
	})
	t.Run("with flip", func(t *testing.T) {
		size := 5 * len(tests[1])
		b := setupTest(t, os.TempDir(), size+metadata)

		for _, test := range tests[:3] {
			_, err := b.Write(test)
			assert.NoError(t, err)
		}
		actual := make([]byte, size/2)
		n, err := b.Read(actual)
		assert.NoError(t, err)
		assert.Equal(t, string(tests[0]), string(actual[:n]))
		n, err = b.Read(actual)
		assert.NoError(t, err)
		assert.Equal(t, string(tests[1]), string(actual[:n]))

		_, err = b.Write(tests[0])
		assert.NoError(t, err)
		n, err = b.Read(actual)
		assert.NoError(t, err)
		assert.Equal(t, string(tests[2]), string(actual[:n]))
		n, err = b.Read(actual)
		assert.NoError(t, err)
		assert.Equal(t, string(tests[0]), string(actual[:n]))
		_, err = b.Read(actual)
		assert.Error(t, err)
	})
}

func TestWrite(t *testing.T) {
	t.Parallel()
	t.Run("empty", func(t *testing.T) {
		b := setupTest(t, os.TempDir(), len(tests[0])+metadata)
		_, err := b.Write(make([]byte, len(tests[0])))
		assert.Error(t, err)
	})
	t.Run("write to cap", func(t *testing.T) {
		size := 3 * len(tests[1])
		b := setupTest(t, os.TempDir(), size+metadata)

		_, err := b.Write(tests[0])
		assert.NoError(t, err)
		_, err = b.Write(tests[0])
		assert.NoError(t, err)
		_, err = b.Write(tests[0])
		assert.Error(t, err)
	})
	t.Run("with flip", func(t *testing.T) {
		size := 5 * len(tests[1])
		b := setupTest(t, os.TempDir(), size+metadata)

		for _, test := range tests[:3] {
			_, err := b.Write(test)
			assert.NoError(t, err)
		}
		actual := make([]byte, size/2)
		n, err := b.Read(actual)
		assert.NoError(t, err)
		assert.Equal(t, string(tests[0]), string(actual[:n]))

		n, err = b.Read(actual)
		assert.NoError(t, err)
		assert.Equal(t, string(tests[1]), string(actual[:n]))

		_, err = b.Write(tests[0])
		assert.NoError(t, err)
		_, err = b.Write(tests[1])
		assert.Error(t, err)
	})
}

func TestDrain(t *testing.T) {
	t.Parallel()
	t.Run("empty", func(t *testing.T) {
		b := setupTest(t, os.TempDir(), len(tests[0])+metadata)
		ctx := context.Background()
		b.Drain(ctx, 1)
	})
	t.Run("exact amount", func(t *testing.T) {
		b := setupTest(t, os.TempDir(), 5*len(tests[1])+metadata)
		ctx := context.Background()
		for _, test := range tests[:3] {
			_, err := b.Write(test)
			assert.NoError(t, err)
		}
		b.Drain(ctx, uint32(len(tests[0])))
		actual := make([]byte, len(tests[1]))
		for _, test := range tests[1:3] {
			n, err := b.Read(actual)
			assert.NoError(t, err)
			assert.Equal(t, string(test), string(actual[:n]))
		}
	})
}

func TestReset(t *testing.T) {
	t.Parallel()
	t.Run("without fip", func(t *testing.T) {
		size := 5 * len(tests[1])
		b := setupTest(t, os.TempDir(), size+metadata)

		for _, test := range tests[:3] {
			_, err := b.Write(test)
			assert.NoError(t, err)
		}
		writerOffset, readerOffset, readerSize := b.GetMeta()
		assert.NotEqual(t, writerOffset, readerOffset)
		assert.Equal(t, readerSize, b.capacity)
		assert.Equal(t, false, writerOffset < readerOffset)
		b.Reset()
		_, newReaderOffset, newReaderSize := b.GetMeta()
		assert.Equal(t, writerOffset, newReaderOffset)
		assert.Equal(t, newReaderSize, b.capacity)
		assert.Equal(t, false, writerOffset < readerOffset)
	})

	t.Run("with flip", func(t *testing.T) {
		size := 5 * len(tests[1])
		b := setupTest(t, os.TempDir(), size+metadata)

		for _, test := range tests[:3] {
			_, err := b.Write(test)
			assert.NoError(t, err)
		}
		actual := make([]byte, size/2)
		n, err := b.Read(actual)
		assert.NoError(t, err)
		assert.Equal(t, string(tests[0]), string(actual[:n]))

		n, err = b.Read(actual)
		assert.NoError(t, err)
		assert.Equal(t, string(tests[1]), string(actual[:n]))
		_, err = b.Write(tests[0])
		assert.NoError(t, err)
		writerOffset, readerOffset, readerSize := b.GetMeta()
		assert.NotEqual(t, writerOffset, readerOffset)
		assert.NotEqual(t, readerSize, b.capacity)
		assert.Equal(t, true, writerOffset < readerOffset)
		b.Reset()
		_, newReaderOffset, newReaderSize := b.GetMeta()
		assert.Equal(t, writerOffset, newReaderOffset)
		assert.Equal(t, newReaderSize, b.capacity)
		assert.Equal(t, false, writerOffset < newReaderOffset)
	})
}
