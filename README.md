# Simplest Kinesis

A very simple example of producers and consumers using [kinesis client library](https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html) with clojure and [AWS localstack](https://github.com/localstack/localstack).

## Dependencies

1. AWS cli
2. podman (or docker)
3. clojure cli & tools

# Localstack

## Setup

1. Setup the AWS localstack environment

    ```bash
    ./setup-localstack.sh
    ```

2. Create the test kinesis stream

    ```bash
    ./create-stream.sh
    ```

## Using

You can execute AWS CLI commands against the localstack using the `awslocal.sh` helper script provided.

Examples
```bash
./awslocal.sh kinesis list-streams
./awslocal.sh dynamodb describe-table --table-name simplest-kinesis-app
./awslocal.sh dynamodb scan --table-name simplest-kinesis-app
./awslocal.sh kinesis describe-stream --stream-name test
./awslocal.sh kinesis get-shard-iterator --stream-name test --shard-id shardId-000000000000 --shard-iterator-type TRIM_HORIZON
./awslocal.sh kinesis get-records --shard-iterator SHARDITERATOR 
```

# App

## Using

Start the app using the clojure CLI tools

```bash
clj -M -m app
```