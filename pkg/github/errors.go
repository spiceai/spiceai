package github

type GitHubCallError struct {
	StatusCode int
	Message    string
}

func (e *GitHubCallError) Error() string {
	return e.Message
}

func NewGitHubCallError(message string, statusCode int) *GitHubCallError {
	return &GitHubCallError{
		Message:    message,
		StatusCode: statusCode,
	}
}
