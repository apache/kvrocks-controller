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

	"github.com/stretchr/testify/require"
)

func TestSlotRanges_AddAndRemove(t *testing.T) {
	t.Run("Add overlap and merge", func(t *testing.T) {
		ranges := SlotRanges{{Start: 0, Stop: 10}, {Start: 20, Stop: 30}}
		// Add range that bridge the gap
		ranges = AddSlotToSlotRanges(ranges, SlotRange{Start: 11, Stop: 19})
		require.Len(t, ranges, 1)
		require.Equal(t, 0, ranges[0].Start)
		require.Equal(t, 30, ranges[0].Stop)
	})

	t.Run("Add partial overlap", func(t *testing.T) {
		ranges := SlotRanges{{Start: 10, Stop: 20}}
		ranges = AddSlotToSlotRanges(ranges, SlotRange{Start: 15, Stop: 25})
		require.Len(t, ranges, 1)
		require.Equal(t, 10, ranges[0].Start)
		require.Equal(t, 25, ranges[0].Stop)
	})

	t.Run("Remove from middle (split)", func(t *testing.T) {
		ranges := SlotRanges{{Start: 0, Stop: 100}}
		ranges = RemoveSlotFromSlotRanges(ranges, SlotRange{Start: 40, Stop: 60})
		require.Len(t, ranges, 2)
		require.Equal(t, 0, ranges[0].Start)
		require.Equal(t, 39, ranges[0].Stop)
		require.Equal(t, 61, ranges[1].Start)
		require.Equal(t, 100, ranges[1].Stop)
	})

	t.Run("Remove all", func(t *testing.T) {
		ranges := SlotRanges{{Start: 0, Stop: 100}}
		ranges = RemoveSlotFromSlotRanges(ranges, SlotRange{Start: 0, Stop: 100})
		require.Len(t, ranges, 0)
	})

	t.Run("Remove non-existent", func(t *testing.T) {
		ranges := SlotRanges{{Start: 0, Stop: 10}}
		ranges = RemoveSlotFromSlotRanges(ranges, SlotRange{Start: 20, Stop: 30})
		require.Len(t, ranges, 1)
		require.Equal(t, 0, ranges[0].Start)
		require.Equal(t, 10, ranges[0].Stop)
	})

	t.Run("Complex merge and split", func(t *testing.T) {
		ranges := SlotRanges{{Start: 0, Stop: 10}, {Start: 20, Stop: 30}, {Start: 40, Stop: 50}}
		// Remove 5-45
		ranges = RemoveSlotFromSlotRanges(ranges, SlotRange{Start: 5, Stop: 45})
		require.Len(t, ranges, 2)
		require.Equal(t, 0, ranges[0].Start)
		require.Equal(t, 4, ranges[0].Stop)
		require.Equal(t, 46, ranges[1].Start)
		require.Equal(t, 50, ranges[1].Stop)
	})
}
