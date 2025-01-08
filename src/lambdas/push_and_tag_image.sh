#!/bin/bash


region="${1:-us-east-1}" 
account_id="${2:-account-id-here}"  
repo_name="${3:-repo-name-here}"  
image_name="${4:-image-name-here}" 
version="${5:-latest}"  

# Login to AWS ECR
aws ecr get-login-password --region $region | docker login --username AWS --password-stdin $account_id.dkr.ecr.$region.amazonaws.com

# Tag the image for ECR
docker tag $image_name:$version $account_id.dkr.ecr.$region.amazonaws.com/$repo_name:$version

# Push the image to AWS ECR
docker push $account_id.dkr.ecr.$region.amazonaws.com/$repo_name:$version

echo "$image_name:$version pushed successfully"
