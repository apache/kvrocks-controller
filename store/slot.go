/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/kvrocks-controller/consts"
)

const (
	MinSlotID = 0
	MaxSlotID = 16383
)

var ErrSlotOutOfRange = errors.New("slot id was out of range, should be between 0 and 16383")

type SlotRange struct {
	Start int `json:"start"` // inclusive
	Stop  int `json:"stop"`  // inclusive
}

type SlotRanges []SlotRange

type MigratingSlot struct {
	SlotRange
	IsMigrating bool
}

func (migratingSlot *MigratingSlot) String() string {
	if migratingSlot == nil {
		return ""
	}
	return migratingSlot.SlotRange.String()
}

func NewSlotRange(start, stop int) (SlotRange, error) {
	if start > stop {
		return SlotRange{}, errors.New("start was larger than stop")
	}
	if (start < MinSlotID || start > MaxSlotID) ||
		(stop < MinSlotID || stop > MaxSlotID) {
		return SlotRange{}, ErrSlotOutOfRange
	}
	return SlotRange{
		Start: start,
		Stop:  stop,
	}, nil
}

func (slotRange *SlotRange) Equal(that SlotRange) bool {
	if slotRange.Start != that.Start {
		return false
	}
	if slotRange.Stop != that.Stop {
		return false
	}
	return true
}

func (slotRange *SlotRange) HasOverlap(that SlotRange) bool {
	return slotRange.Stop >= that.Start && slotRange.Start <= that.Stop
}

func (slotRange *SlotRange) Contains(slot int) bool {
	return slot >= slotRange.Start && slot <= slotRange.Stop
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

func (slotRange *SlotRange) UnmarshalJSON(data []byte) error {
	var slotsString string
	if err := json.Unmarshal(data, &slotsString); err != nil {
		return err
	}
	slotObject, err := ParseSlotRange(slotsString)
	if err != nil {
		return err
	}
	*slotRange = *slotObject
	return nil
}

func ParseSlotRange(s string) (*SlotRange, error) {
	numberOfRanges := strings.Count(s, "-")
	if numberOfRanges > 1 {
		return nil, fmt.Errorf("%w, cannot have more than one range", consts.ErrInvalidArgument)
	}

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
		return nil, errors.New("start slot id greater than stop slot id")
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

func (SlotRanges *SlotRanges) Contains(slot int) bool {
	for _, slotRange := range *SlotRanges {
		if slotRange.Contains(slot) {
			return true
		}
	}
	return false
}

func (SlotRanges *SlotRanges) HasOverlap(slotRange SlotRange) bool {
	for _, slotRange := range *SlotRanges {
		if slotRange.HasOverlap(slotRange) {
			return true
		}
	}
	return false
}

func (s *SlotRange) Reset() {
	s.Start = 0
	s.Stop = 0
}

// FromSlotRange will return a MigratingSlot with the IsMigrating field set to true.
// IsMigrating field would probably only be set to false from an unmarshal, like when
// reading from the topology string
func FromSlotRange(slotRange SlotRange) *MigratingSlot {
	return &MigratingSlot{
		SlotRange:   slotRange,
		IsMigrating: true,
	}
}

func (s *MigratingSlot) UnmarshalJSON(data []byte) error {
	var slotsString any
	if err := json.Unmarshal(data, &slotsString); err != nil {
		return err
	}
	switch t := slotsString.(type) {
	case string:
		slotRange := SlotRange{}
		err := json.Unmarshal(data, &slotRange)
		if err != nil {
			s.Reset()
			return err
		}
		s.SlotRange = slotRange
		s.IsMigrating = true
	case float64:
		// We use integer to represent the slot because we don't support the slot range
		// in the past. So we need to support the integer type for backward compatibility.
		// But the number in JSON is float64, so we need to convert it to int here.
		if t == NotMigratingInt {
			s.Reset()
			return nil
		}
		if t < MinSlotID || t > MaxSlotID {
			s.Reset()
			return ErrSlotOutOfRange
		}
		slotID := int(t)
		s.Start = slotID
		s.Stop = slotID
		s.IsMigrating = true
	default:
		s.Reset()
		return fmt.Errorf("invalid slot range type: %T", slotsString)
	}
	return nil
}

func (s *MigratingSlot) MarshalJSON() ([]byte, error) {
	if !s.IsMigrating {
		// backwards compatibility. When we read from an old cluster that had `-1`
		// denoting !isMigrating. The MigratingSlot field will not be nil. So when
		// this field is marshal'd back into JSON format, we can keep it as it was
		// which was `-1`.
		// The only case this turns back to null is if a migration happens on this
		// shard, and the function `ClearMigrateState()` is called on the shard.
		return json.Marshal(NotMigratingInt)
	}
	return json.Marshal(s.String())
}

func (s *MigratingSlot) Reset() {
	s.SlotRange.Reset()
	s.IsMigrating = false
}

// CanMerge will return true if the given SlotRanges are adjacent with each other
func CanMerge(a, b SlotRange) bool {
	// Ensure a starts before b for easier comparison
	if a.Start > b.Start {
		a, b = b, a
	}
	// If the end of `a` is at least one less than the start of `b`, they can merge
	return a.Stop+1 >= b.Start
}

func MergeSlotRanges(a SlotRange, b SlotRange) SlotRange {
	return SlotRange{
		Start: min(a.Start, b.Start),
		Stop:  max(a.Stop, b.Stop),
	}
}

// Implemented following leetcode solution:
// https://leetcode.com/problems/merge-intervals/solutions/1805268/go-clean-code-with-explanation-and-visual-10ms-100
func AddSlotToSlotRanges(source SlotRanges, slot SlotRange) SlotRanges {
	if len(source) == 0 {
		return append(source, slot)
	}
	source = append(source, slot)
	sort.Slice(source, func(i, j int) bool {
		return source[i].Start < source[j].Start
	})

	mergedSlotRanges := make([]SlotRange, 0, len(source))
	mergedSlotRanges = append(mergedSlotRanges, source[0])

	for _, interval := range source[1:] {
		lastIntervalPos := len(mergedSlotRanges) - 1
		lastInterval := mergedSlotRanges[lastIntervalPos]
		if CanMerge(lastInterval, interval) {
			mergedSlotRanges[lastIntervalPos] = MergeSlotRanges(interval, lastInterval)
		} else {
			mergedSlotRanges = append(mergedSlotRanges, interval)
		}
	}

	return mergedSlotRanges
}

func RemoveSlotFromSlotRanges(source SlotRanges, slot SlotRange) SlotRanges {
	sort.Slice(source, func(i, j int) bool {
		return source[i].Start < source[j].Start
	})
	if !source.HasOverlap(slot) {
		return source
	}

	result := make([]SlotRange, 0, len(source))
	for _, slotRange := range source {
		// if no overlap, keep original range
		if !slotRange.HasOverlap(slot) {
			result = append(result, slotRange)
			continue
		}
		// if overlap, then we need to create a new left and right range
		if slotRange.Start < slot.Start {
			result = append(result, SlotRange{
				Start: slotRange.Start,
				Stop:  slot.Start - 1,
			})
		}
		if slotRange.Stop > slot.Stop {
			result = append(result, SlotRange{
				Start: slot.Stop + 1,
				Stop:  slotRange.Stop,
			})
		}
	}
	return result
}

func CalculateSlotRanges(n int) SlotRanges {
	var slots []SlotRange
	rangeSize := (MaxSlotID + 1) / n
	for i := 0; i < n; i++ {
		if i != n-1 {
			slots = append(slots, SlotRange{Start: i * rangeSize, Stop: (i+1)*rangeSize - 1})
		} else {
			slots = append(slots, SlotRange{Start: i * rangeSize, Stop: MaxSlotID})
		}
	}
	return slots
}
