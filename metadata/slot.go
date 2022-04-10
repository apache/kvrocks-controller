package metadata

import (
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
)

const (
	MinSlotID = 0
	MaxSlotID = 16383
)

var ErrSlotOutOfRange = errors.New("slot id was out of range, should be between 0 and 65535")

type SlotRange struct {
	Start int
	Stop  int
}

func NewSlotRange(start, stop int) (*SlotRange, error) {
	if start > stop {
		return nil, errors.New("start was larger than Stop")
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
		return nil, errors.New("start slot id greater than Stop slot id")
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

func MergeSlotRanges(source []SlotRange, target []SlotRange) []SlotRange {
	source = append(source, target...)
	sort.Slice(source, func(i, j int) bool {
		return source[i].Start < source[j].Start
	})
	merged := make([]SlotRange, 0)
	for _, sourceSlotRange := range source {
		if len(merged) == 0 {
			merged = append(merged, sourceSlotRange)
			continue
		}
		lastSlotRange := merged[len(merged)-1]
		if !sourceSlotRange.HasOverlap(&lastSlotRange) {
			merged = append(merged, sourceSlotRange)
			continue
		}
		// source slot range start is always greater than merged since we have sorted them
		if lastSlotRange.Stop < sourceSlotRange.Stop {
			merged[len(merged)-1].Stop = sourceSlotRange.Stop
		}
	}
	return merged
}

func RemoveSlotRanges(source []SlotRange, target []SlotRange) []SlotRange {
	for _, deleteSlotRange := range target {
		sort.Slice(source, func(i, j int) bool {
			return source[i].Start < source[j].Start
		})
		for i, slotRange := range source {
			if !slotRange.HasOverlap(&deleteSlotRange) {
				continue
			}
			source = append(source[0:i], source[i+1:]...)
			if deleteSlotRange.Start < slotRange.Start && deleteSlotRange.Stop < slotRange.Stop {
				source = append(source, SlotRange{Start: deleteSlotRange.Stop + 1, Stop: slotRange.Stop})
			} else if deleteSlotRange.Start > slotRange.Start && deleteSlotRange.Stop > slotRange.Stop {
				source = append(source, SlotRange{Start: slotRange.Start, Stop: deleteSlotRange.Start - 1})
			} else if deleteSlotRange.Start > slotRange.Start && deleteSlotRange.Stop < slotRange.Stop {
				source = append(source, SlotRange{Start: slotRange.Start, Stop: deleteSlotRange.Start - 1})
				source = append(source, SlotRange{Start: deleteSlotRange.Stop + 1., Stop: slotRange.Stop})
			}
			break
		}
	}
	return source
}
