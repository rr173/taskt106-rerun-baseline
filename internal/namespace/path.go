package namespace

import (
	"strings"
	"unicode"
)

func Normalize(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", ErrEmptyPath
	}
	parts := strings.Split(path, "/")
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || part == "." || part == ".." {
			return "", ErrInvalidPath
		}
		for _, r := range part {
			if unicode.IsControl(r) {
				return "", ErrInvalidPath
			}
		}
		clean = append(clean, part)
	}
	return strings.Join(clean, "/"), nil
}

func Parent(path string) (string, bool) {
	idx := strings.LastIndexByte(path, '/')
	if idx < 0 {
		return "", false
	}
	return path[:idx], true
}

func Components(path string) []string {
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}
