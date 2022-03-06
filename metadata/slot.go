package metadata

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
	Start int
	Stop  int
}

func NewSlotRange(start, stop int) (*SlotRange, error) {
	if start > stop {
		return nil, errors.New("Start was larger than Stop")
	}
	if (start < MinSlotID || start > MaxSlotID) ||
		(stop < MinSlotID || stop > MaxSlotID) {
		return nil, ErrSlotOutOfRange
	}
	return &SlotRange{
		Start: start,
		Stop:  stop,
	}, nil
}

func (slotRange *SlotRange) HasOverlap(that *SlotRange) bool {
	return !(slotRange.Stop < that.Start || slotRange.Start > that.Stop)
}

func (slotRange *SlotRange) String() string {
	if slotRange.Start == slotRange.Stop {
		return strconv.Itoa(slotRange.Start)
	}
	return strconv.Itoa(slotRange.Start) + "-" + strconv.Itoa(slotRange.Stop)
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
			Start: start,
			Stop:  start,
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
		return nil, errors.New("Start slot id greater than Stop slot id")
	}
	if (start < MinSlotID || start > MaxSlotID) ||
		(stop < MinSlotID || stop > MaxSlotID) {
		return nil, ErrSlotOutOfRange
	}
	return &SlotRange{
		Start: start,
		Stop:  stop,
	}, nil
}
