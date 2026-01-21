#!/bin/bash
# Test script for AWS Marketplace publishing workflow
# This script simulates the Docker publishing workflow without actually pushing to ECR

set -e

# Configuration (matching the GitHub workflow)
TAG="${1:-1.3.0-models}"
OLD_TAG="spiceai/spiceai:${TAG}"
ECR_REGISTRY="709825985650.dkr.ecr.us-east-1.amazonaws.com"
REPOSITORY="${2:-spice-ai/spiceai-enterprise}"
PLATFORMS="${3:-linux/amd64,linux/arm64}"
NEW_TAG="${ECR_REGISTRY}/${REPOSITORY}:${TAG}"
AWS_REGION="us-east-1"

echo "=== AWS Marketplace Publishing Test ==="
echo "Tag: ${TAG}"
echo "Source image: ${OLD_TAG}"
echo "Target repository: ${REPOSITORY}"
echo "Target image: ${NEW_TAG}"
echo "Platforms: ${PLATFORMS}"
echo ""

# Parse platforms
IFS=',' read -ra PLATFORMS_ARRAY <<< "${PLATFORMS}"

# Validate that we have platforms
if [ ${#PLATFORMS_ARRAY[@]} -eq 0 ]; then
  echo "Error: No platforms specified"
  exit 1
fi

echo "Publishing tag: ${TAG}"
echo "Source image: ${OLD_TAG}"
echo "Target image: ${NEW_TAG}"
echo "Platforms: ${PLATFORMS_ARRAY[*]}"
echo ""

# Create manifest arguments array
MANIFEST_ARGS=""

# Process each platform
for PLATFORM in "${PLATFORMS_ARRAY[@]}"; do
  ARCH=$(echo "${PLATFORM}" | cut -d'/' -f2) # linux/amd64 -> amd64
  ARCH_TAG="${ECR_REGISTRY}/${REPOSITORY}:${TAG}-${ARCH}"
  
  echo "Processing platform: ${PLATFORM}, arch: ${ARCH}"
  
  # Check if image exists (don't pull in test mode)
  echo "Would pull: docker pull --platform \"${PLATFORM}\" \"${OLD_TAG}\""
  
  # Show what would be tagged
  echo "Would tag: docker tag \"${OLD_TAG}\" \"${ARCH_TAG}\""
  
  # Show what would be pushed
  echo "Would push: docker push \"${ARCH_TAG}\""
  
  # Add to manifest arguments
  MANIFEST_ARGS="${MANIFEST_ARGS} --amend ${ARCH_TAG}"
  echo ""
done

echo "Would create manifest with args: ${MANIFEST_ARGS}"
echo "Would run: docker manifest create \"${NEW_TAG}\" ${MANIFEST_ARGS}"
echo "Would run: docker manifest push \"${NEW_TAG}\""
echo "Would run: docker manifest inspect \"${NEW_TAG}\""

echo ""
echo "=== Test completed successfully ==="
echo "To run the actual workflow, trigger it via GitHub Actions with tag: ${TAG} and repository: ${REPOSITORY}"