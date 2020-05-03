#!/bin/sh
docker build -t esmartit/live-stream-counters:"$1" -t esmartit/live-stream-counters:latest .
exit