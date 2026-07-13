#!/bin/bash

set -e  # Exit immediately if any command fails

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
DOCKER_REPO_NAME="datastax/zdm-proxy"
AWS_REPO_NAME="zdm-proxy"
IMAGE_TAG="latest"


# Authenticate with ECR
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com

# Create ECR repository (ignore error if it already exists)
aws ecr create-repository --repository-name ${AWS_REPO_NAME} --region ${REGION} || echo "ECR repo already exists"

# Pull image from Docker Hub
docker pull --platform linux/amd64 ${DOCKER_REPO_NAME}:${IMAGE_TAG}

# Download Amazon Root CA for Keyspaces TLS verification
curl -o AmazonRootCA1.pem https://www.amazontrust.com/repository/AmazonRootCA1.pem

# Build locally
docker build -f Dockerfile --platform linux/amd64 -t zdm-proxy-custom:latest .

docker tag zdm-proxy-custom:${IMAGE_TAG} ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/zdm-proxy:latest

# Push to ECR
docker push ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${AWS_REPO_NAME}:${IMAGE_TAG}

echo ""
echo "✅ Image pushed: ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${AWS_REPO_NAME}:${IMAGE_TAG}"
