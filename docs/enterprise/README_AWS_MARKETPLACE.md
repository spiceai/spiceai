# AWS Marketplace Publishing Workflow

This document describes how to use the AWS Marketplace publishing workflow (`aws_marketplace_publish.yml`) to publish Spice.ai Docker images and Helm charts to AWS ECR for the AWS Marketplace.

## Prerequisites

The workflow requires the following GitHub secrets to be configured:
- `AWS_MARKETPLACE_ACCESS_KEY_ID`: AWS access key with ECR permissions
- `AWS_MARKETPLACE_SECRET_ACCESS_KEY`: AWS secret access key

## Usage

The workflow is triggered manually via `workflow_dispatch` with the following inputs:

### Required Inputs
- **tag**: Docker image tag to publish (e.g., `1.3.0-models`)

### Optional Inputs
- **repositories**: Target ECR repositories (choice: `spice-ai/spiceai-enterprise-byol`, `spice-ai/spiceai-enterprise-plan`, or both)
- **platforms**: Platforms to publish (default: `linux/amd64,linux/arm64`)
- **publish_docker**: Whether to publish Docker images (default: `true`)
- **publish_helm**: Whether to publish Helm chart (default: `true`)
- **allowed_account_ids**: Comma-separated AWS account IDs to grant read-only pull access (defaults to a pre-configured allowlist; set to empty to skip)

## What the Workflow Does

### Docker Image Publishing
1. Pulls the specified tag from DockerHub (`spiceai/spiceai:${tag}`)
2. Re-tags images for each architecture to AWS ECR format:
   - `709825985650.dkr.ecr.us-east-1.amazonaws.com/${repository}:${tag}-amd64`
   - `709825985650.dkr.ecr.us-east-1.amazonaws.com/${repository}:${tag}-arm64`
3. Pushes architecture-specific images to ECR
4. Creates and pushes a multi-architecture manifest:
   - `709825985650.dkr.ecr.us-east-1.amazonaws.com/${repository}:${tag}`

### Helm Chart Publishing
1. Modifies the Helm chart for enterprise use:
   - Changes chart name from `spiceai` to `spiceai-enterprise`
   - Updates image repository to ECR registry
   - Updates version to `${tag}-helm`
   - Changes description to "Spice.ai Enterprise"
2. Packages the modified chart
3. Pushes to ECR OCI registry: `oci://709825985650.dkr.ecr.us-east-1.amazonaws.com/spice-ai/`

### ECR Pull Access Policy
The workflow sets an ECR repository policy granting read-only pull access (`ecr:GetDownloadUrlForLayer`, `ecr:BatchGetImage`, `ecr:BatchCheckLayerAvailability`, `ecr:DescribeImages`) to each AWS account in `allowed_account_ids`. A pre-configured allowlist is applied by default. To skip setting the policy, clear the `allowed_account_ids` input. The policy is merged with any existing repository policy (preserving statements with different Sids). Account IDs must be 12-digit numbers, comma-separated (e.g., `123456789012,987654321098`). See [AWS Marketplace ECR: Controlling Access and Permissions](https://aws.amazon.com/blogs/awsmarketplace/aws-marketplace-ecr-controlling-access-and-permissions/) for background.

## Example Usage

To publish version `1.3.0-models`:

1. Go to the Actions tab in GitHub
2. Select "Publish to AWS Marketplace" workflow
3. Click "Run workflow"
4. Fill in the inputs:
   - **tag**: `1.3.0-models`
   - **repositories**: `spice-ai/spiceai-enterprise-byol,spice-ai/spiceai-enterprise-plan` (or a single repository)
   - **platforms**: `linux/amd64,linux/arm64` (or leave default)
   - **publish_docker**: `true` (or leave default)
   - **publish_helm**: `true` (or leave default)
5. Click "Run workflow"

## Configuration

The workflow uses these fixed configurations:
- **ECR Registry**: `709825985650.dkr.ecr.us-east-1.amazonaws.com`
- **AWS Region**: `us-east-1`

The repository can be configured through the workflow inputs:
- BYOL repository: `spice-ai/spiceai-enterprise-byol`
- Enterprise Plan repository: `spice-ai/spiceai-enterprise-plan`