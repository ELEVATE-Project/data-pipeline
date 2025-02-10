#!/bin/bash

set -a
source .env
set +a

# Define variables
DOCKERHUB_USERNAME="prashantghub"
IMAGE_NAME="test4shikshalokam"
VERSION="v3.0"

# Full image tag
IMAGE_TAG="$DOCKERHUB_USERNAME/$IMAGE_NAME:$VERSION"

# Step 1: Build the Docker image
echo "Building Docker image..."
sudo docker build -t $IMAGE_TAG .

# Check if the build was successful
if [ $? -ne 0 ]; then
    echo "Docker build failed!"
    exit 1
fi

echo "Docker image built successfully: $IMAGE_TAG"

# Step 2: Log in to Docker Hub (only required if not logged in)
echo "Logging into Docker Hub..."
sudo docker login

# Check if login was successful
if [ $? -ne 0 ]; then
    echo "Docker login failed!"
    exit 1
fi

# Step 3: Push the image to Docker Hub
echo "Pushing Docker image to Docker Hub..."
sudo docker push $IMAGE_TAG

# Check if the push was successful
if [ $? -ne 0 ]; then
    echo "Docker push failed!"
    exit 1
fi

echo "Docker image pushed successfully: $IMAGE_TAG"

# Step 4: Logout (optional)
sudo docker logout

echo "Done!"
