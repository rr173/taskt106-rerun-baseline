package namespace

import "sort"

func Ancestors(path string) []string {
	parts := Components(path)
	result := make([]string, 0, len(parts))
	for i := 1; i < len(parts); i++ {
		result = append(result, join(parts[:i]))
	}
	return result
}

func DescendantPaths(paths []string, root string) []string {
	result := make([]string, 0)
	for _, path := range paths {
		if IsSameOrDescendant(path, root) {
			result = append(result, path)
		}
	}
	sort.Strings(result)
	return result
}

func join(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for _, part := range parts[1:] {
		result += "/" + part
	}
	return result
}
