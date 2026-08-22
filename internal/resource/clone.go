package resource

import "task106/internal/model"

// cloneLabels returns a deep copy of labels.
//
// Labels is a map (a Go reference type), so a shallow struct copy of a
// model.Resource still aliases the same underlying map. Without cloning on the
// way out, a caller that mutates the Labels of a returned resource would
// silently corrupt the manager's in-memory state and affect every subsequent
// read. Clone on both read (return paths) and write (registration) so neither
// the caller's input map nor the returned map is shared with internal state.
func cloneLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	cp := make(map[string]string, len(labels))
	for k, v := range labels {
		cp[k] = v
	}
	return cp
}

// cloneResource returns a deep-enough copy of item so that the returned value
// shares no mutable reference-typed fields (Labels) with the caller or with the
// manager's internal map. The caller may freely mutate the returned labels
// without affecting the service's state or any future read.
func cloneResource(item *model.Resource) *model.Resource {
	if item == nil {
		return nil
	}
	cp := *item
	cp.Labels = cloneLabels(item.Labels)
	return &cp
}
