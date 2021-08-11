package meta

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSlotRange_String(t *testing.T) {
	sr, err := NewSlotRange(1, 100)
	require.Nil(t, err)
	assert.Equal(t, sr.String(), "1-100")

	_, err = NewSlotRange(100, 1)
	assert.NotNil(t, err)

	_, err = NewSlotRange(-1, 100)
	assert.Equal(t, ErrSlotOutOfRange, err)

	_, err = NewSlotRange(-1, 65536)
	assert.Equal(t, ErrSlotOutOfRange, err)
}

func TestSlotRange_Parse(t *testing.T) {
	sr, err := ParseSlotRange("1-12")
	assert.Nil(t, err)
	assert.Equal(t, 1, sr.start)
	assert.Equal(t, 12, sr.stop)

	_, err = ParseSlotRange("1-65536")
	assert.Equal(t, ErrSlotOutOfRange, err)

	_, err = ParseSlotRange("-11-65536")
	assert.NotNil(t, err)

	_, err = ParseSlotRange("12-1")
	assert.NotNil(t, err)
}
