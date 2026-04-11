#!/bin/bash

docker build -f deploy/Dockerfile -t ccr.ccs.tencentyun.com/zhanhuili/stream-store:latest .
docker push ccr.ccs.tencentyun.com/zhanhuili/stream-store:latest
