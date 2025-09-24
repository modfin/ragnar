#!/usr/bin/env bash

VERSION=$(date +%Y-%m-%dT%H.%M.%S)-$(git log -1 --pretty=format:"%h")
COMMIT_MSG=$(git log -1 --pretty=format:"%s" .)
AUTHOR=$(git log -1 --pretty=format:"%an" .)

docker build \
  --platform linux/amd64 \
  -f ./Dockerfile \
  --label "CommitMsg=${COMMIT_MSG}" \
  --label "Author=${AUTHOR}" \
  -t modfin/ragnar:latest \
  -t modfin/ragnar:${VERSION} \
  . || exit 1

docker push modfin/ragnar:latest || exit 1
docker push modfin/ragnar:${VERSION} || exit 1

docker rmi modfin/ragnar:latest || exit 1
docker rmi modfin/ragnar:${VERSION} || exit 1