package recovery

import "strings"

func SummarizeIssues(issues []string) string {
	if len(issues) == 0 {
		return "no recovery issues"
	}
	return strings.Join(issues, "; ")
}

func HasBlockingIssue(issues []string) bool {
	for _, issue := range issues {
		if strings.Contains(issue, "retired") || strings.Contains(issue, "no registered") {
			return true
		}
	}
	return false
}
