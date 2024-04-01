#!/bin/bash

# Pull latest changes from git
cd ~/projects/mediamtx
git pull

# Run Docker container with golang image, mounting the project directory
sudo docker run -d --name golang_build -v ~/projects/mediamtx:/mediamtx golang:latest /bin/bash -c 'sleep infinity'

# Execute commands inside the container
sudo docker exec golang_build /bin/bash -c '
# Move into the mounted directory
cd /mediamtx

# Run go generate
go generate ./...

# Build the Go application
CGO_ENABLED=0 go build -buildvcs=false .
'

# Stop and remove the container
sudo docker container stop golang_build >/dev/null
sudo docker container rm golang_build >/dev/null
