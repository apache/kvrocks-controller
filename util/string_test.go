package util

import (
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
