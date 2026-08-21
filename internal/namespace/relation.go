package namespace

import "strings"

func IsSameOrDescendant(path, ancestor string) bool {
	if path == ancestor {
		return true
	}
	return strings.HasPrefix(path, ancestor+"/")
}

func IsAncestor(path, descendant string) bool {
	return path != descendant && IsSameOrDescendant(descendant, path)
}

func Overlaps(left, right string) bool {
	return IsSameOrDescendant(left, right) || IsSameOrDescendant(right, left)
}

func Scope(paths []string, target string) bool {
	for _, path := range paths {
		if IsSameOrDescendant(target, path) {
			return true
		}
	}
	return false
}
