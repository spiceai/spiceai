package github

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	WorkflowRunsPerPage uint = 100
)

var (
	GitHubApiRuns = "https://api.github.com/repos/%s/%s/actions/runs?per_page=%d"
)

func GetWorkflowRuns(g *GitHubClient, pageNumber uint) (*WorkflowRuns, error) {

	url := fmt.Sprintf(GitHubApiRuns, g.Owner, g.Repo, WorkflowRunsPerPage)
	if pageNumber > 0 {
		url = fmt.Sprintf("%s&amp;page=%d", url, pageNumber)
	}

	workflowRunsBytes, err := g.Get(url, nil)
	if err != nil {
		return nil, err
	}

	workflowRuns, err := UnmarshalWorkflowRuns(workflowRunsBytes)
	if err != nil {
		return nil, err
	}

	return &workflowRuns, nil
}

func UnmarshalWorkflowRuns(data []byte) (WorkflowRuns, error) {
	var r WorkflowRuns
	err := json.Unmarshal(data, &r)
	return r, err
}

func (r *WorkflowRuns) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

type WorkflowRunArray []WorkflowRun

type WorkflowRuns struct {
	TotalCount   int64            `json:"total_count"`
	WorkflowRuns WorkflowRunArray `json:"workflow_runs"`
}

type WorkflowRun struct {
	ID               int64  `json:"id"`
	Name             string `json:"name"`
	HeadBranch       string `json:"head_branch"`
	HeadSHA          string `json:"head_sha"`
	RunNumber        int64  `json:"run_number"`
	Event            string `json:"event"`
	Status           string `json:"status"`
	Conclusion       string `json:"conclusion"`
	WorkflowID       int64  `json:"workflow_id"`
	CheckSuiteID     int64  `json:"check_suite_id"`
	CheckSuiteNodeID string `json:"check_suite_node_id"`
	URL              string `json:"url"`
	HTMLURL          string `json:"html_url"`
	CreatedAt        string `json:"created_at"`
	UpdatedAt        string `json:"updated_at"`
	JobsURL          string `json:"jobs_url"`
	LogsURL          string `json:"logs_url"`
	CheckSuiteURL    string `json:"check_suite_url"`
	ArtifactsURL     string `json:"artifacts_url"`
	CancelURL        string `json:"cancel_url"`
	RerunURL         string `json:"rerun_url"`
	WorkflowURL      string `json:"workflow_url"`
}

func (w WorkflowRunArray) Len() int {
	return len(w)
}

func (w WorkflowRunArray) Less(i, j int) bool {
	one := w[i]
	two := w[j]

	return strings.Compare(one.CreatedAt, two.CreatedAt) == 1
}

func (s WorkflowRunArray) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
