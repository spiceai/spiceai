package github

import "encoding/json"

func GetWorkflowArtifacts(g *GitHubClient, runUrl string) (*WorkflowArtifacts, error) {
	workflowArtifactsBytes, err := g.Get(runUrl, nil)

	if err != nil {
		return nil, err
	}

	workflowArtifacts, err := UnmarshalWorkflowArtifacts(workflowArtifactsBytes)
	if err != nil {
		return nil, err
	}

	return &workflowArtifacts, nil
}

func UnmarshalWorkflowArtifacts(data []byte) (WorkflowArtifacts, error) {
	var r WorkflowArtifacts
	err := json.Unmarshal(data, &r)
	return r, err
}

func (r *WorkflowArtifacts) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

type WorkflowArtifacts struct {
	TotalCount int64      `json:"total_count"`
	Artifacts  []Artifact `json:"artifacts"`
}

type Artifact struct {
	ID                 int64  `json:"id"`
	NodeID             string `json:"node_id"`
	Name               string `json:"name"`
	SizeInBytes        int64  `json:"size_in_bytes"`
	URL                string `json:"url"`
	ArchiveDownloadURL string `json:"archive_download_url"`
	Expired            bool   `json:"expired"`
	CreatedAt          string `json:"created_at"`
	UpdatedAt          string `json:"updated_at"`
	ExpiresAt          string `json:"expires_at"`
}
