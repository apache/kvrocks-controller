package version

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const (
	// MinKvrocksVersion is the minimal supported kvrocks version
	MinKvrocksVersion = "2.0.5"
)

var (
	ErrVersionNotSupported = errors.New("kvrocks version is not supported")
)

// CheckKvrocksVersion checks if the kvrocks version is supported
// We only check the major, minor and patch version
func CheckKvrocksVersion(version string) error {
	if version == "" {
		return errors.New("kvrocks version is empty")
	}

	// Remove potential prefix like "v"
	version = strings.TrimPrefix(version, "v")

	// Split by dot
	parts := strings.Split(version, ".")
	if len(parts) < 3 {
		// If it's something like "unstable" or "999.0", we might need better handling.
		// For now, if it doesn't look like semver, we assume it's compliant if it's a dev version?
		// Or we reject it.
		// Kvrocks versions are usually x.y.z.
		// Let's print error if not enough parts.
		return fmt.Errorf("invalid version format: %s", version)
	}

	minParts := strings.Split(MinKvrocksVersion, ".")

	for i := 0; i < 3; i++ {
		v, err := strconv.Atoi(parts[i])
		if err != nil {
			// If we can't parse one part (e.g. 2.0.rc1), we might stop.
			// Ideally we strip non-numeric suffix from the last part.
			re := regexp.MustCompile(`^(\d+)`)
			match := re.FindStringSubmatch(parts[i])
			if len(match) > 1 {
				v, _ = strconv.Atoi(match[1])
			} else {
				return fmt.Errorf("invalid version format: %s", version)
			}
		}

		minV, _ := strconv.Atoi(minParts[i])

		if v > minV {
			return nil
		}
		if v < minV {
			return fmt.Errorf("%w: minimal supported version is %s, got %s", ErrVersionNotSupported, MinKvrocksVersion, version)
		}
	}

	// Exactly equal
	return nil
}
