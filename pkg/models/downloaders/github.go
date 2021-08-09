package downloaders

import (
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/logrusorgru/aurora"
	"github.com/spiceai/spice/pkg/config"
	"github.com/spiceai/spice/pkg/github"
)

type GitHubDownloader struct {
	GitHub        *github.GitHubClient
	Branch        string
	WorkflowPath  string
	PodName       string
	ModelSavePath string
}

func (g *GitHubDownloader) Fetch() (string, error) {

	workflowRuns, err := g.getSortedMatchingWorkflowRuns()
	if err != nil {
		return "", err
	}

	if workflowRuns == nil {
		return "", nil
	}

	artifact, err := g.getLatestModelArtifactDownloadUrl(workflowRuns)
	if err != nil {
		return "", err
	}

	if artifact == nil {
		return "", nil
	}

	log.Println(aurora.BrightCyan(fmt.Sprintf("Model update %s found!", artifact.Name)))

	sizeInMb := float64(artifact.SizeInBytes) / 1000000.0
	log.Printf("Downloading model %s (%.2f MB)\n", artifact.ArchiveDownloadURL, sizeInMb)

	zipArtifact, err := g.GitHub.Get(artifact.ArchiveDownloadURL, nil)
	if err != nil {
		return "", err
	}

	err = g.GitHub.ExtractTarGzInsideZip(zipArtifact, g.ModelSavePath)
	if err != nil {
		return "", err
	}

	log.Printf("Downloaded model update '%s' to %s\n", "latest", config.GetSpiceAppRelativePath(g.ModelSavePath))

	return "latest", nil
}

func (g *GitHubDownloader) getSortedMatchingWorkflowRuns() (github.WorkflowRunArray, error) {
	var matchingRuns github.WorkflowRunArray

	var page uint = 1
	numSeen := 0
	for {
		workflowRuns, err := github.GetWorkflowRuns(g.GitHub, page)
		if err != nil {
			return nil, err
		}

		for _, workflowRun := range workflowRuns.WorkflowRuns {
			if workflowRun.HeadBranch == g.Branch &&
				workflowRun.Name == g.WorkflowPath &&
				workflowRun.Conclusion == "success" &&
				workflowRun.Status == "completed" {
				matchingRuns = append(matchingRuns, workflowRun)
			}
		}

		numSeen += len(workflowRuns.WorkflowRuns)

		if int(workflowRuns.TotalCount) > numSeen {
			page++
			continue
		}

		break
	}

	if len(matchingRuns) == 0 {
		return nil, nil
	}

	sort.Sort(matchingRuns)

	return matchingRuns, nil
}

func (g *GitHubDownloader) getLatestModelArtifactDownloadUrl(workflowRuns github.WorkflowRunArray) (*github.Artifact, error) {
	for _, workflowRun := range workflowRuns {
		workflowArtifacts, err := github.GetWorkflowArtifacts(g.GitHub, workflowRun.ArtifactsURL)
		if err != nil {
			return nil, err
		}

		for _, artifact := range workflowArtifacts.Artifacts {
			if strings.HasSuffix(artifact.Name, ".model") {
				return &artifact, nil
			}
		}
	}

	return nil, nil
}
