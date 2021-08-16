package meta

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

const (
	MinSlotID = 0
	MaxSlotID = 65535
)

var ErrSlotOutOfRange = errors.New("slot id was out of range, should be between 0 and 65535")

type SlotRange struct {
	start int
	stop  int
}

func NewSlotRange(start, stop int) (*SlotRange, error) {
	if start > stop {
		return nil, errors.New("start was larger than stop")
	}
	if (start < MinSlotID || start > MaxSlotID) ||
		(stop < MinSlotID || stop > MaxSlotID) {
		return nil, ErrSlotOutOfRange
	}
	return &SlotRange{
		start: start,
		stop:  stop,
	}, nil
}

func (slotRange *SlotRange) HasOverlap(that *SlotRange) bool {
	return !(slotRange.stop < that.start || slotRange.start > that.stop)
}

func (slotRange *SlotRange) String() string {
	if slotRange.start == slotRange.stop {
		return strconv.Itoa(slotRange.start)
	}
	return strconv.Itoa(slotRange.start) + "-" + strconv.Itoa(slotRange.stop)
}

func (slotRange *SlotRange) MarshalJSON() ([]byte, error) {
	return json.Marshal(slotRange.String())
}

func ParseSlotRange(s string) (*SlotRange, error) {
	index := strings.IndexByte(s, '-')
	if index == -1 {
		start, err := strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		if start < MinSlotID || start > MaxSlotID {
			return nil, ErrSlotOutOfRange
		}
		return &SlotRange{
			start: start,
			stop:  start,
		}, nil
	}

	start, err := strconv.Atoi(s[0:index])
	if err != nil {
		return nil, err
	}
	stop, err := strconv.Atoi(s[index+1:])
	if err != nil {
		return nil, err
	}
	if start > stop {
		return nil, errors.New("start slot id greater than stop slot id")
	}
	if (start < MinSlotID || start > MaxSlotID) ||
		(stop < MinSlotID || stop > MaxSlotID) {
		return nil, ErrSlotOutOfRange
	}
	return &SlotRange{
		start: start,
		stop:  stop,
	}, nil
}
