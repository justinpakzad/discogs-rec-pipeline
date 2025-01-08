#!/bin/bash

# Set default region and account ID if not provided
region="${1:-us-east-1}"  # Default to 'us-east-1'
account_id="${2:-982081066484}"  # Your AWS account ID
repo_name="${3:-discogs-rec-feedback-transformation}"  # Your ECR repository name
image_name="${4:-discogs-rec-feedback-transformation}"  # Local image name
version="${5:-latest}"  # Image version (update here as needed)

# Login to AWS ECR
aws ecr get-login-password --region $region | docker login --username AWS --password-stdin $account_id.dkr.ecr.$region.amazonaws.com

# Tag the image for ECR
docker tag $image_name:$version $account_id.dkr.ecr.$region.amazonaws.com/$repo_name:$version

# Push the image to AWS ECR
docker push $account_id.dkr.ecr.$region.amazonaws.com/$repo_name:$version

echo "$image_name:$version pushed successfully"
