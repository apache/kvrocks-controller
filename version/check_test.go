package version

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckKvrocksVersion(t *testing.T) {
	tests := []struct {
		version string
		isValid bool
	}{
		{"2.0.5", true},
		{"2.0.6", true},
		{"2.1.0", true},
		{"2.1.0-rc1", true},
		{"2.0.4", false},
		{"1.0.0", false},
		{"999.0.0", true},
		{"v2.0.5", true},
		{"v2.0.4", false},
		{"2.0.5.1", true}, // Extra parts ignored but major.minor.patch matches
	}

	for _, test := range tests {
		t.Run(test.version, func(t *testing.T) {
			err := CheckKvrocksVersion(test.version)
			if test.isValid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
