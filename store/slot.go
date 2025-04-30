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
	Start int `json:"start"`
	Stop  int `json:"stop"`
}

type SlotRanges []SlotRange

func NewSlotRange(start, stop int) (*SlotRange, error) {
	if start > stop {
		return nil, errors.New("start was larger than stop")
	}
	if start == stop {
		return nil, consts.ErrSlotStartAndStopEqual
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
	// TODO: byron apply De Morgan's law later to make this easier to read
	return !(slotRange.Stop < that.Start || slotRange.Start > that.Stop)
}

func (slotRange *SlotRange) Contains(slot int) bool {
	return slot >= slotRange.Start && slot <= slotRange.Stop
}

func (slotRange *SlotRange) String() string {
	if slotRange.Start+1 == slotRange.Stop {
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
	index := strings.IndexByte(s, '-')
	if index == -1 {
		start, err := strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		if start < MinSlotID || start+1 > MaxSlotID {
			return nil, ErrSlotOutOfRange
		}
		return &SlotRange{
			Start: start,
			Stop:  start + 1,
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

	mergedInterval := make([]SlotRange, 0, len(source))
	mergedInterval = append(mergedInterval, source[0])

	for _, interval := range source[1:] {
		lastIntervalPos := len(mergedInterval) - 1
		lastInterval := mergedInterval[lastIntervalPos]
		if lastInterval.HasOverlap(&interval) {
			mergedInterval[lastIntervalPos] = MergeSlotRanges(interval, lastInterval)
		} else {
			mergedInterval = append(mergedInterval, interval)
		}
	}

	return mergedInterval
}

func RemoveSlotFromSlotRanges(source SlotRanges, slot SlotRange) SlotRanges {
	// TODO: byron
	// sort.Slice(source, func(i, j int) bool {
	// 	return source[i].Start < source[j].Start
	// })
	// if !source.Contains(slot) {
	// 	return source
	// }
	// for i, slotRange := range source {
	// 	if slotRange.Contains(slot) {
	// 		if slotRange.Start == slot && slotRange.Stop == slot {
	// 			source = append(source[0:i], source[i+1:]...)
	// 		} else if slotRange.Start == slot {
	// 			source[i].Start = slot + 1
	// 		} else if slotRange.Stop == slot {
	// 			source[i].Stop = slot - 1
	// 		} else {
	// 			tmp := make(SlotRanges, len(source)+1)
	// 			copy(tmp, source[0:i])
	// 			tmp[i] = SlotRange{Start: slotRange.Start, Stop: slot - 1}
	// 			tmp[i+1] = SlotRange{Start: slot + 1, Stop: slotRange.Stop}
	// 			copy(tmp[i+2:], source[i+1:])
	// 			source = tmp
	// 		}
	// 	}
	// }
	return source
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
