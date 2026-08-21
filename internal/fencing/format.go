package fencing

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"task106/internal/namespace"
	"unicode"
)

func makeToken(path string, sequence int64) string {
	digest := sha256.Sum256([]byte(path + ":" + strconv.FormatInt(sequence, 10)))
	return fmt.Sprintf("f1.%d.%s", sequence, hex.EncodeToString(digest[:12]))
}

// validateResourcePath rejects resource paths that are empty or malformed.
// A valid path has no leading/trailing or duplicate separators (no empty
// segments), and contains no control characters or traversal components.
// Paths that fail this check must never receive a fencing token.
func validateResourcePath(path string) error {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return ErrInvalidResource
	}
	if _, err := namespace.Normalize(trimmed); err != nil {
		return ErrInvalidResource
	}
	segments := strings.Split(trimmed, "/")
	for _, segment := range segments {
		if segment == "" {
			return ErrInvalidResource
		}
		for _, r := range segment {
			if unicode.IsControl(r) {
				return ErrInvalidResource
			}
		}
	}
	return nil
}
