package metadata

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
	assert.Equal(t, 1, sr.Start)
	assert.Equal(t, 12, sr.Stop)

	_, err = ParseSlotRange("1-65536")
	assert.Equal(t, ErrSlotOutOfRange, err)

	_, err = ParseSlotRange("-11-65536")
	assert.NotNil(t, err)

	_, err = ParseSlotRange("12-1")
	assert.NotNil(t, err)
}

func TestSlotRange_MergeSlotRanges(t *testing.T) {
	range1 := SlotRange {
		Start: 0,
		Stop:  8191,
	}
	range2 := SlotRange {
		Start: 8192,
		Stop:  16383,
	}
	newSlot := MergeSlotRanges([]SlotRange{range1}, []SlotRange{range2})
	assert.Equal(t, 1, len(newSlot))
	assert.Equal(t, 0, newSlot[0].Start)
	assert.Equal(t, 16383, newSlot[0].Stop)
}

func TestSlotRange_RemoveSlotRanges(t *testing.T) {
	range1 := SlotRange {
		Start: 0,
		Stop:  8191,
	}
	range2 := SlotRange {
		Start: 0,
		Stop:  0,
	}
	newSlot := RemoveSlotRanges([]SlotRange{range1}, []SlotRange{range2})
	assert.Equal(t, 1, newSlot[0].Start)
	assert.Equal(t, 8191, newSlot[0].Stop)

	range1 = SlotRange {
		Start: 0,
		Stop:  8191,
	}
	range2 = SlotRange {
		Start: 8192,
		Stop:  16383,
	}
	range3 := SlotRange {
		Start: 0,
		Stop:  8192,
	}
	newSlot = RemoveSlotRanges([]SlotRange{range1, range2}, []SlotRange{range3})
	assert.Equal(t, 8193, newSlot[0].Start)
	assert.Equal(t, 16383, newSlot[0].Stop)

	range1 = SlotRange {
		Start: 0,
		Stop:  8191,
	}
	range2 = SlotRange {
		Start: 8192,
		Stop:  16383,
	}
	range3 = SlotRange {
		Start: 1,
		Stop:  8192,
	}
	newSlot = RemoveSlotRanges([]SlotRange{range1, range2}, []SlotRange{range3})
	assert.Equal(t, 0, newSlot[0].Start)
	assert.Equal(t, 0, newSlot[0].Stop)
	assert.Equal(t, 8193, newSlot[1].Start)
	assert.Equal(t, 16383, newSlot[1].Stop)

	range1 = SlotRange {
		Start: 0,
		Stop:  8191,
	}
	range2 = SlotRange {
		Start: 8192,
		Stop:  16383,
	}
	range3 = SlotRange {
		Start: 1,
		Stop:  8192,
	}
	range4 := SlotRange {
		Start: 8194,
		Stop:  16383,
	}
	newSlot = RemoveSlotRanges([]SlotRange{range1, range2}, []SlotRange{range3, range4})
	assert.Equal(t, 0, newSlot[0].Start)
	assert.Equal(t, 0, newSlot[0].Stop)
	assert.Equal(t, 8193, newSlot[1].Start)
	assert.Equal(t, 8193, newSlot[1].Stop)
}