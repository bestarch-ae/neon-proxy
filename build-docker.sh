#!/bin/sh

if  [ -z "$NEON_PROXY_TAG" ] ||  [ -z "$NEON_PROXY_TAG" ] || [ -z "$NEON_PROXY_TAG" ]; then
  echo "error: some variables are not defined
define variables DOCKERHUB_ORG_NAME, NEON_PROXY_REPO_NAME, NEON_PROXY_TAG before running docker build
for example:
export DOCKERHUB_ORG_NAME=some-organization
export NEON_PROXY_REPO_NAME=neon-proxy
export NEON_PROXY_TAG=latest
"
  exit 1
fi

docker build \
  --build-arg UBUNTU_VERSION=22.04 \
  --build-arg NEON_REVISION=true \
  --tag ${DOCKERHUB_ORG_NAME}/${NEON_PROXY_REPO_NAME}:${NEON_PROXY_TAG} \
  -f Dockerfile .
