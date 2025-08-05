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
	"testing"

	"github.com/apache/kvrocks-controller/consts"
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

func TestMigratingSlot_UnmarshalJSON(t *testing.T) {
	var migratingSlot MigratingSlot

	migratingSlot = MigratingSlot{SlotRange: SlotRange{Start: 5, Stop: 5}, IsMigrating: true} // to set values to migratingSlot
	slotBytes, err := json.Marshal(NotMigratingInt)
	require.NoError(t, err)
	err = json.Unmarshal(slotBytes, &migratingSlot)
	require.NoError(t, err, "expects no error since -1 was a valid 'not migrating' value")
	assert.Equal(t, MigratingSlot{SlotRange{Start: 0, Stop: 0}, false}, migratingSlot)

	migratingSlot = MigratingSlot{SlotRange: SlotRange{Start: 5, Stop: 5}, IsMigrating: true} // to set values to migratingSlot
	slotBytes, err = json.Marshal(-5)
	require.NoError(t, err)
	err = json.Unmarshal(slotBytes, &migratingSlot)
	require.ErrorIs(t, err, ErrSlotOutOfRange, "-5 is not a valid 'not migrating' value")
	assert.Equal(t, MigratingSlot{SlotRange{Start: 0, Stop: 0}, false}, migratingSlot)

	slotBytes, err = json.Marshal("456")
	require.NoError(t, err)
	err = json.Unmarshal(slotBytes, &migratingSlot)
	require.NoError(t, err)
	assert.Equal(t, MigratingSlot{SlotRange{Start: 456, Stop: 456}, true}, migratingSlot)

	slotBytes, err = json.Marshal("123-456")
	require.NoError(t, err)
	err = json.Unmarshal(slotBytes, &migratingSlot)
	require.NoError(t, err)
	assert.Equal(t, MigratingSlot{SlotRange{Start: 123, Stop: 456}, true}, migratingSlot)

	slotBytes, err = json.Marshal("invalid-string")
	require.NoError(t, err)
	err = json.Unmarshal(slotBytes, &migratingSlot)
	require.Error(t, err)
	assert.Equal(t, MigratingSlot{SlotRange{Start: 0, Stop: 0}, false}, migratingSlot)
}

// TestMigratingSlot_MarshalUnmarshalJSON will check that we can marshal and then unmarshal
// back into the MigratingSlot
func TestMigratingSlot_MarshalUnmarshalJSON(t *testing.T) {
	migratingSlot := MigratingSlot{SlotRange: SlotRange{Start: 5, Stop: 5}, IsMigrating: true}
	migratingSlotBytes, err := json.Marshal(&migratingSlot)
	require.NoError(t, err)
	err = json.Unmarshal(migratingSlotBytes, &migratingSlot)
	require.NoError(t, err)
	assert.Equal(t, MigratingSlot{SlotRange{Start: 5, Stop: 5}, true}, migratingSlot)

	// tests that we can marshal isMigrating = false, which results in -1, and then unmarshal it
	// to be isMigrating = false again
	migratingSlot = MigratingSlot{SlotRange: SlotRange{Start: 0, Stop: 0}, IsMigrating: false}
	migratingSlotBytes, err = json.Marshal(&migratingSlot)
	require.NoError(t, err)
	err = json.Unmarshal(migratingSlotBytes, &migratingSlot)
	require.NoError(t, err)
	assert.Equal(t, MigratingSlot{SlotRange{Start: 0, Stop: 0}, false}, migratingSlot)

	// same test as earlier, but checks that it resets the start and stop
	migratingSlot = MigratingSlot{SlotRange: SlotRange{Start: 5, Stop: 5}, IsMigrating: false}
	migratingSlotBytes, err = json.Marshal(&migratingSlot)
	require.NoError(t, err)
	err = json.Unmarshal(migratingSlotBytes, &migratingSlot)
	require.NoError(t, err)
	assert.Equal(t, MigratingSlot{SlotRange{Start: 0, Stop: 0}, false}, migratingSlot, "expects start and stop to reset to 0")
}

// TestMigratingSlot_MarshalJSON will checks the resulting string
func TestMigratingSlot_MarshalJSON(t *testing.T) {
	migratingSlot := MigratingSlot{SlotRange: SlotRange{Start: 5, Stop: 5}, IsMigrating: true}
	migratingSlotBytes, err := json.Marshal(&migratingSlot)
	require.NoError(t, err)
	assert.Equal(t, `"5"`, string(migratingSlotBytes))

	migratingSlot = MigratingSlot{SlotRange: SlotRange{Start: 5, Stop: 10}, IsMigrating: true}
	migratingSlotBytes, err = json.Marshal(&migratingSlot)
	require.NoError(t, err)
	assert.Equal(t, `"5-10"`, string(migratingSlotBytes))

	migratingSlot = MigratingSlot{SlotRange: SlotRange{Start: 5, Stop: 10}, IsMigrating: false}
	migratingSlotBytes, err = json.Marshal(&migratingSlot)
	require.NoError(t, err)
	assert.Equal(t, `-1`, string(migratingSlotBytes))
}

func TestMigrateSlotRange_MarshalAndUnmarshalJSON(t *testing.T) {
	var slotRange SlotRange

	slotBytes, err := json.Marshal("-100")
	require.NoError(t, err)
	err = json.Unmarshal(slotBytes, &slotRange)
	require.NotNil(t, err, "expects error since input is a negative number")
	assert.Equal(t, SlotRange{Start: 0, Stop: 0}, slotRange)

	slotBytes, err = json.Marshal("-100-100000")
	require.NoError(t, err)
	err = json.Unmarshal(slotBytes, &slotRange)
	require.NotNil(t, err, "expects error since input is out of range")
	assert.Equal(t, SlotRange{Start: 0, Stop: 0}, slotRange)

	slotBytes, err = json.Marshal("456")
	require.NoError(t, err)
	err = json.Unmarshal(slotBytes, &slotRange)
	require.NoError(t, err)
	assert.Equal(t, SlotRange{Start: 456, Stop: 456}, slotRange)

	slotBytes, err = json.Marshal("123-456")
	require.NoError(t, err)
	err = json.Unmarshal(slotBytes, &slotRange)
	require.NoError(t, err)
	assert.Equal(t, SlotRange{Start: 123, Stop: 456}, slotRange)
}

func TestSlotRange_Parse(t *testing.T) {
	sr, err := ParseSlotRange("1-12")
	assert.Nil(t, err)
	assert.Equal(t, 1, sr.Start)
	assert.Equal(t, 12, sr.Stop)

	sr, err = ParseSlotRange("5")
	assert.Nil(t, err)
	assert.Equal(t, 5, sr.Start)
	assert.Equal(t, 5, sr.Stop)

	sr, err = ParseSlotRange("0")
	assert.Nil(t, err)
	assert.Equal(t, 0, sr.Start)
	assert.Equal(t, 0, sr.Stop)

	_, err = ParseSlotRange("1-65536")
	assert.Equal(t, ErrSlotOutOfRange, err)

	_, err = ParseSlotRange("-11-65536")
	assert.NotNil(t, err)

	_, err = ParseSlotRange("12-1")
	assert.NotNil(t, err)

	_, err = ParseSlotRange("1-12 5-10")
	require.ErrorIs(t, err, consts.ErrInvalidArgument)

	_, err = ParseSlotRange("1-12, 5")
	assert.NotNil(t, err)
}

func TestAddSlotToSlotRanges(t *testing.T) {
	slotRanges := SlotRanges{
		{Start: 1, Stop: 20},
		{Start: 101, Stop: 199},
		{Start: 201, Stop: 300},
	}
	slotRange, err := NewSlotRange(0, 0)
	require.NoError(t, err)
	slotRanges = AddSlotToSlotRanges(slotRanges, slotRange)
	require.Equal(t, 3, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 0, Stop: 20}, slotRanges[0], slotRanges)

	slotRange, err = NewSlotRange(21, 21)
	require.NoError(t, err)
	slotRanges = AddSlotToSlotRanges(slotRanges, slotRange)
	require.Equal(t, 3, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 0, Stop: 21}, slotRanges[0], slotRanges)

	slotRange, err = NewSlotRange(50, 50)
	require.NoError(t, err)
	slotRanges = AddSlotToSlotRanges(slotRanges, slotRange)
	require.Equal(t, 4, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 50, Stop: 50}, slotRanges[1], slotRanges)

	slotRange, err = NewSlotRange(200, 200)
	require.NoError(t, err)
	slotRanges = AddSlotToSlotRanges(slotRanges, slotRange)
	require.Equal(t, 3, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 101, Stop: 300}, slotRanges[2], slotRanges)

	slotRange, err = NewSlotRange(400, 400)
	require.NoError(t, err)
	slotRanges = AddSlotToSlotRanges(slotRanges, slotRange)
	require.Equal(t, 4, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 400, Stop: 400}, slotRanges[3], slotRanges)
}

func TestRemoveSlotRanges(t *testing.T) {
	slotRanges := SlotRanges{
		{Start: 1, Stop: 20},
		{Start: 101, Stop: 199},
		{Start: 201, Stop: 300},
	}
	slotRange, err := NewSlotRange(0, 0)
	require.NoError(t, err)
	slotRanges = RemoveSlotFromSlotRanges(slotRanges, slotRange)
	require.Equal(t, 3, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 1, Stop: 20}, slotRanges[0], slotRanges)

	slotRange, err = NewSlotRange(21, 21)
	require.NoError(t, err)
	slotRanges = RemoveSlotFromSlotRanges(slotRanges, slotRange)
	require.Equal(t, 3, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 1, Stop: 20}, slotRanges[0], slotRanges)

	slotRange, err = NewSlotRange(20, 20)
	require.NoError(t, err)
	slotRanges = RemoveSlotFromSlotRanges(slotRanges, slotRange)
	require.Equal(t, 3, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 1, Stop: 19}, slotRanges[0], slotRanges)

	slotRange, err = NewSlotRange(150, 150)
	require.NoError(t, err)
	slotRanges = RemoveSlotFromSlotRanges(slotRanges, slotRange)
	require.Equal(t, 4, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 101, Stop: 149}, slotRanges[1], slotRanges)

	slotRange, err = NewSlotRange(101, 101)
	require.NoError(t, err)
	slotRanges = RemoveSlotFromSlotRanges(slotRanges, slotRange)
	require.Equal(t, 4, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 102, Stop: 149}, slotRanges[1], slotRanges)

	slotRange, err = NewSlotRange(199, 199)
	require.NoError(t, err)
	slotRanges = RemoveSlotFromSlotRanges(slotRanges, slotRange)
	require.Equal(t, 4, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 151, Stop: 198}, slotRanges[2], slotRanges)

	slotRange, err = NewSlotRange(300, 300)
	require.NoError(t, err)
	slotRanges = RemoveSlotFromSlotRanges(slotRanges, slotRange)
	require.Equal(t, 4, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 201, Stop: 299}, slotRanges[3], slotRanges)

	slotRange, err = NewSlotRange(298, 298)
	require.NoError(t, err)
	slotRanges = RemoveSlotFromSlotRanges(slotRanges, slotRange)
	require.Equal(t, 5, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 201, Stop: 297}, slotRanges[3], slotRanges)
	require.EqualValues(t, SlotRange{Start: 299, Stop: 299}, slotRanges[4], slotRanges)

	slotRange, err = NewSlotRange(299, 299)
	require.NoError(t, err)
	slotRanges = RemoveSlotFromSlotRanges(slotRanges, slotRange)
	require.Equal(t, 4, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 201, Stop: 297}, slotRanges[3], slotRanges)
}

func TestCalculateSlotRanges(t *testing.T) {
	slots := CalculateSlotRanges(5)
	assert.Equal(t, 0, slots[0].Start)
	assert.Equal(t, 3275, slots[0].Stop)
	assert.Equal(t, 13104, slots[4].Start)
	assert.Equal(t, 16383, slots[4].Stop)
}

func TestSlotRange_HasOverlap(t *testing.T) {
	type args struct {
		that SlotRange
	}
	tests := []struct {
		name       string
		slotRanges SlotRanges
		args       args
		want       bool
	}{
		{
			name: "0-5 does not overlap 6-7",
			slotRanges: SlotRanges{
				{Start: 0, Stop: 5},
			},
			args: args{SlotRange{Start: 6, Stop: 7}},
			want: false,
		},
		{
			name: "0-5 does overlap 3-4",
			slotRanges: SlotRanges{
				{Start: 0, Stop: 5},
			},
			args: args{SlotRange{Start: 3, Stop: 4}},
			want: true,
		},
		{
			name: "0-5 does overlap 5-8",
			slotRanges: SlotRanges{
				{Start: 0, Stop: 5},
			},
			args: args{SlotRange{Start: 5, Stop: 8}},
			want: true,
		},
		{
			name: "0-5 does overlap 4-8",
			slotRanges: SlotRanges{
				{Start: 0, Stop: 5},
			},
			args: args{SlotRange{Start: 4, Stop: 8}},
			want: true,
		},
		{
			name: "0-100 does not overlap 101-150",
			slotRanges: SlotRanges{
				{Start: 0, Stop: 100},
			},
			args: args{SlotRange{Start: 101, Stop: 150}},
			want: false,
		},
		{
			name: "50-100 does overlap 30-50",
			slotRanges: SlotRanges{
				{Start: 50, Stop: 100},
			},
			args: args{SlotRange{Start: 30, Stop: 50}},
			want: true,
		},
		{
			name: "50-100 does overlap 50-51",
			slotRanges: SlotRanges{
				{Start: 50, Stop: 100},
			},
			args: args{SlotRange{Start: 50, Stop: 51}},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.slotRanges.HasOverlap(tt.args.that); got != tt.want {
				t.Errorf("SlotRange.HasOverlap() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCanMerge(t *testing.T) {
	type args struct {
		a SlotRange
		b SlotRange
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "0-5 and 6-10 can merge",
			args: args{SlotRange{0, 5}, SlotRange{6, 10}},
			want: true,
		},
		{
			name: "6-10 and 0-5 can merge",
			args: args{SlotRange{6, 10}, SlotRange{0, 5}},
			want: true,
		},
		{
			name: "6-6 and 0-5 can merge",
			args: args{SlotRange{6, 6}, SlotRange{0, 5}},
			want: true,
		},
		{
			name: "0-5 and 7-10 cannot merge",
			args: args{SlotRange{0, 5}, SlotRange{7, 10}},
			want: false,
		},
		{
			name: "7-10 and 0-5 cannot merge",
			args: args{SlotRange{7, 10}, SlotRange{0, 5}},
			want: false,
		},
		{
			name: "2-2 and 4-4 cannot merge",
			args: args{SlotRange{2, 2}, SlotRange{4, 4}},
			want: false,
		},
		{
			name: "4-4 and 2-2 cannot merge",
			args: args{SlotRange{4, 4}, SlotRange{2, 2}},
			want: false,
		},
		{
			name: "2-3 and 4-4 can merge",
			args: args{SlotRange{2, 3}, SlotRange{4, 4}},
			want: true,
		},
		{
			name: "4-4 and 2-3 can merge",
			args: args{SlotRange{4, 4}, SlotRange{2, 3}},
			want: true,
		},
		{
			name: "4-4 and 3-3 can merge",
			args: args{SlotRange{4, 4}, SlotRange{3, 3}},
			want: true,
		},
		{
			name: "3-3 and 4-4 can merge",
			args: args{SlotRange{3, 3}, SlotRange{4, 4}},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CanMerge(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("CanMerge() = %v, want %v", got, tt.want)
			}
		})
	}
}
