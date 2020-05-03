#!/bin/sh
docker login -u $DOCKER_USER -p $DOCKER_PASS
docker push esmartit/live-stream-counters:"$1"
docker push esmartit/live-stream-counters:latest
helm package live-stream-counters --version "$1" --app-version "$1"
touch version.txt
echo "$1" >> version.txt
exit