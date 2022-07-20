#!/usr/bin/env bash

podman run -d --name localstack \
-p 4566:4566 -p 4571:4571 \
-e AWS_REGION:eu-west-1 \
-e SERVICES=kinesis,dynamodb,cloudwatch \
docker.io/localstack/localstack