#!/usr/bin/env bash
aws --endpoint-url=http://localhost:4566 kinesis create-stream --shard-count 2 --stream-name test