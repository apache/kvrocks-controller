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
	slotRanges = AddSlotToSlotRanges(slotRanges, *slotRange)
	require.Equal(t, 3, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 0, Stop: 20}, slotRanges[0], slotRanges)

	slotRange, err = NewSlotRange(21, 21)
	require.NoError(t, err)
	slotRanges = AddSlotToSlotRanges(slotRanges, *slotRange)
	require.Equal(t, 3, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 0, Stop: 21}, slotRanges[0], slotRanges)

	slotRange, err = NewSlotRange(50, 50)
	require.NoError(t, err)
	slotRanges = AddSlotToSlotRanges(slotRanges, *slotRange)
	require.Equal(t, 4, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 50, Stop: 50}, slotRanges[1], slotRanges)

	slotRange, err = NewSlotRange(200, 200)
	require.NoError(t, err)
	slotRanges = AddSlotToSlotRanges(slotRanges, *slotRange)
	require.Equal(t, 3, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 101, Stop: 300}, slotRanges[2], slotRanges)

	slotRange, err = NewSlotRange(400, 400)
	require.NoError(t, err)
	slotRanges = AddSlotToSlotRanges(slotRanges, *slotRange)
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
	slotRanges = RemoveSlotFromSlotRanges(slotRanges, *slotRange)
	require.Equal(t, 3, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 1, Stop: 20}, slotRanges[0], slotRanges)

	slotRange, err = NewSlotRange(21, 21)
	require.NoError(t, err)
	slotRanges = RemoveSlotFromSlotRanges(slotRanges, *slotRange)
	require.Equal(t, 3, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 1, Stop: 20}, slotRanges[0], slotRanges)

	slotRange, err = NewSlotRange(20, 20)
	require.NoError(t, err)
	slotRanges = RemoveSlotFromSlotRanges(slotRanges, *slotRange)
	require.Equal(t, 3, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 1, Stop: 19}, slotRanges[0], slotRanges)

	slotRange, err = NewSlotRange(150, 150)
	require.NoError(t, err)
	slotRanges = RemoveSlotFromSlotRanges(slotRanges, *slotRange)
	require.Equal(t, 4, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 101, Stop: 149}, slotRanges[1], slotRanges)

	slotRange, err = NewSlotRange(101, 101)
	require.NoError(t, err)
	slotRanges = RemoveSlotFromSlotRanges(slotRanges, *slotRange)
	require.Equal(t, 4, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 102, Stop: 149}, slotRanges[1], slotRanges)

	slotRange, err = NewSlotRange(199, 199)
	require.NoError(t, err)
	slotRanges = RemoveSlotFromSlotRanges(slotRanges, *slotRange)
	require.Equal(t, 4, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 151, Stop: 198}, slotRanges[2], slotRanges)

	slotRange, err = NewSlotRange(300, 300)
	require.NoError(t, err)
	slotRanges = RemoveSlotFromSlotRanges(slotRanges, *slotRange)
	require.Equal(t, 4, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 201, Stop: 299}, slotRanges[3], slotRanges)

	slotRange, err = NewSlotRange(298, 298)
	require.NoError(t, err)
	slotRanges = RemoveSlotFromSlotRanges(slotRanges, *slotRange)
	require.Equal(t, 5, len(slotRanges), slotRanges)
	require.EqualValues(t, SlotRange{Start: 201, Stop: 297}, slotRanges[3], slotRanges)
	require.EqualValues(t, SlotRange{Start: 299, Stop: 299}, slotRanges[4], slotRanges)

	slotRange, err = NewSlotRange(299, 299)
	require.NoError(t, err)
	slotRanges = RemoveSlotFromSlotRanges(slotRanges, *slotRange)
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
	type fields struct {
		Start int
		Stop  int
	}
	type args struct {
		that *SlotRange
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name:   "0-5 does not overlap 6-7",
			fields: fields{Start: 0, Stop: 5},
			args:   args{&SlotRange{Start: 6, Stop: 7}},
			want:   false,
		},
		{
			name:   "0-5 does overlap 3-4",
			fields: fields{Start: 0, Stop: 5},
			args:   args{&SlotRange{Start: 3, Stop: 4}},
			want:   true,
		},
		{
			name:   "0-5 does overlap 5-8",
			fields: fields{Start: 0, Stop: 5},
			args:   args{&SlotRange{Start: 5, Stop: 8}},
			want:   true,
		},
		{
			name:   "0-5 does overlap 4-8",
			fields: fields{Start: 0, Stop: 5},
			args:   args{&SlotRange{Start: 4, Stop: 8}},
			want:   true,
		},
		{
			name:   "0-100 does not overlap 101-150",
			fields: fields{Start: 0, Stop: 100},
			args:   args{&SlotRange{Start: 101, Stop: 150}},
			want:   false,
		},
		{
			name:   "50-100 does overlap 30-50",
			fields: fields{Start: 50, Stop: 100},
			args:   args{&SlotRange{Start: 30, Stop: 50}},
			want:   true,
		},
		{
			name:   "50-100 does overlap 50-51",
			fields: fields{Start: 50, Stop: 100},
			args:   args{&SlotRange{Start: 50, Stop: 51}},
			want:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slotRange := &SlotRange{
				Start: tt.fields.Start,
				Stop:  tt.fields.Stop,
			}
			if got := slotRange.HasOverlap(tt.args.that); got != tt.want {
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
