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
package util

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsUniqueStrings(t *testing.T) {
	// unique case
	listStr := []string{"a", "b", "c"}
	assert.Equal(t, true, IsUniqueSlice(listStr))

	listInt := []int{1, 2, 3}
	assert.Equal(t, true, IsUniqueSlice(listInt))

	// non unique case
	dupListStr := []string{"a", "a", "c"}
	assert.Equal(t, false, IsUniqueSlice(dupListStr))

	dupListStr = []string{"a", "b", "a"}
	assert.Equal(t, false, IsUniqueSlice(dupListStr))

	dupListInt := []int{1, 1, 2, 2}
	assert.Equal(t, false, IsUniqueSlice(dupListInt))
}

func TestRandString(t *testing.T) {
	const allowedChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	tests := []struct {
		name   string
		length int
	}{
		{"ValidLength", 40},
		{"ShortLength", 8},
		{"ZeroLength", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := RandString(tt.length)
			assert.Equal(t, tt.length, len(s), "String length should match requested length")

			for _, char := range s {
				assert.True(t, strings.ContainsRune(allowedChars, char), "Character %c is not in allowed set", char)
			}
		})
	}
}
