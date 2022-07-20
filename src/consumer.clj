(ns consumer
  (:require [jsonista.core :as j]
            [clojure.tools.logging :as log])
  (:import
   [java.time Duration]
   [software.amazon.awssdk.services.kinesis KinesisAsyncClient]
   [software.amazon.awssdk.services.dynamodb DynamoDbAsyncClient]
   [software.amazon.awssdk.services.cloudwatch CloudWatchAsyncClient]
   [software.amazon.kinesis.processor ShardRecordProcessorFactory ShardRecordProcessor]
   [software.amazon.awssdk.core.client.config ClientOverrideConfiguration]
;;    [software.amazon.kinesis.retrieval.polling PollingConfig]
   [software.amazon.kinesis.exceptions InvalidStateException ShutdownException ThrottlingException]
   [software.amazon.kinesis.common ConfigsBuilder InitialPositionInStreamExtended InitialPositionInStreamExtended InitialPositionInStream]
   [software.amazon.kinesis.coordinator Scheduler]
   [software.amazon.awssdk.regions Region]
   [software.amazon.awssdk.http.nio.netty NettyNioAsyncHttpClient Http2Configuration]
   [software.amazon.awssdk.http Protocol]))

;; https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/kinesis/KinesisAsyncClient.html
;; https://docs.aws.amazon.com/streams/latest/dev/kcl2-standard-consumer-java-example.html

;; https://github.com/aws/aws-sdk-java-v2/blob/master/docs/BestPractices.md

;; This is what a kinesis record looks like
;; https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/retrieval/KinesisClientRecord.java

(def LOCALSTACK_URI (java.net.URI. "http://localhost:4566"))
(def FIVE_MINUTES_IN_MILLIS (int (* 300 1000)))

(defn configure-client [builder {:keys [region api-timeout api-attempt-timeout initial-window-size-bytes health-check-ping-period
                                        http-connection-timeout http-max-concurrency http-connection-max-idle-time http-connection-acquisition-timeout
                                        http-read-timeout]
                                 :or {region "us-east-1"
                                      api-timeout FIVE_MINUTES_IN_MILLIS
                                      api-attempt-timeout FIVE_MINUTES_IN_MILLIS
                                      initial-window-size-bytes (int (* 512 1024)) ;;512kb
                                      health-check-ping-period FIVE_MINUTES_IN_MILLIS
                                      http-connection-timeout FIVE_MINUTES_IN_MILLIS
                                      http-max-concurrency Integer/MAX_VALUE
                                      http-connection-max-idle-time FIVE_MINUTES_IN_MILLIS
                                      http-connection-acquisition-timeout FIVE_MINUTES_IN_MILLIS
                                      http-read-timeout FIVE_MINUTES_IN_MILLIS}
                                 :as options}]
  (log/debug "CONFIGURE-CLIENT " (type builder) " with: "options)
  (-> builder
      (.overrideConfiguration (-> (ClientOverrideConfiguration/builder)
                                  (.apiCallTimeout (Duration/ofMillis api-timeout))  ;; from best practices doc
                                  (.apiCallAttemptTimeout (Duration/ofMillis api-attempt-timeout))
                                  (.build)))
      (.httpClientBuilder (-> (NettyNioAsyncHttpClient/builder)
                              (.maxConcurrency http-max-concurrency)
                              (.connectionTimeout (Duration/ofMillis http-connection-timeout))
                              (.connectionMaxIdleTime (Duration/ofMillis http-connection-max-idle-time))
                              (.connectionAcquisitionTimeout (Duration/ofMillis http-connection-acquisition-timeout))
                              (.readTimeout (Duration/ofMillis http-read-timeout))
                              (.http2Configuration (-> (Http2Configuration/builder)
                                                       (.initialWindowSize initial-window-size-bytes)
                                                       (.healthCheckPingPeriod (Duration/ofMillis health-check-ping-period))
                                                       (.build)))
                              (.protocol Protocol/HTTP2)))
      (.endpointOverride LOCALSTACK_URI)
      (.region (Region/of region))
      (.build)))

(defn async-kinesis-client [& {:as options}]
  (configure-client (KinesisAsyncClient/builder) options))

(defn dynamodb-client [& {:as options}]
  (configure-client (DynamoDbAsyncClient/builder) options))

(defn cloudwatch-client [& {:as options}]
  (configure-client (CloudWatchAsyncClient/builder) options))

(defn- from-bytes [record]
  (let [data (.data record)
        bytes (byte-array (.remaining data))
        _ (.get data bytes)]
    (j/read-value bytes j/default-object-mapper)))

(defn- checkpoint [input]
  (try
    (log/debug "Checkpointing shard processer")
    (-> input 
        (.checkpointer) 
        (.checkpoint))
    (catch ShutdownException e
      (log/info e "Caught shutdown exception, skipping checkpoint"))
    (catch ThrottlingException e
      (log/info e "Caught throttling exception, skipping checkpoint"))
    (catch InvalidStateException e
      (log/error e "Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library"))))

;;; move reify to proxy extend class
(defn- shard-record-processor [record-process-fn]
  (reify ShardRecordProcessorFactory
    (shardRecordProcessor
     [_]
     (reify ShardRecordProcessor
       (initialize [_ initialisation-input]
         (log/info "Initializing Shard Record Processor for shard:" (.shardId initialisation-input)))

       (processRecords [_ process-records-input]
         (let [records (.records process-records-input)]
           (log/debug "processRecords processing: " (.size records) "records")
           (doseq [r records]
             (-> r from-bytes record-process-fn))
           (checkpoint process-records-input)))

       (shardEnded [_ shard-ended-input]
         ;; Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
         (log/info "Reached shard end checkpointing.")
         (checkpoint shard-ended-input))

       (leaseLost [_ lease-lost-input]
         (log/info "Lost lease, so terminating."))

       (shutdownRequested [_ shutdown-requested-input]
         (log/info "Scheduler is shutting down, checkpointing.")
         (checkpoint shutdown-requested-input))))))

;; consider options map with defaults
(defn scheduler! [stream-name record-process-fn & {:keys [kinesis-client app-name at-timestamp from-start from-end]
                                                                 :or {app-name (.toString (java.util.UUID/randomUUID))} :as options}]
  ;; https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/common/ConfigsBuilder.java
  (let [kinesis-client (or kinesis-client (apply async-kinesis-client (apply concat options)))
        config (ConfigsBuilder. stream-name
                                app-name
                                kinesis-client
                                (apply dynamodb-client (apply concat options))
                                (apply cloudwatch-client (apply concat options))
                                (.toString (java.util.UUID/randomUUID))
                                (shard-record-processor record-process-fn))]
    
    (Scheduler. (.checkpointConfig config)
                (.coordinatorConfig config)
                (.leaseManagementConfig config)
                (.lifecycleConfig config)
                (.metricsConfig config)
                (.processorConfig config)
                ;; (-> config (.retrievalConfig) (.retrievalSpecificConfig (PollingConfig. stream-name kinesis-client))) ;; polling
                (cond-> config
                  true (.retrievalConfig)
                  from-start (.initialPositionInStreamExtended (InitialPositionInStreamExtended/newInitialPosition InitialPositionInStream/TRIM_HORIZON))
                  from-end (.initialPositionInStreamExtended (InitialPositionInStreamExtended/newInitialPosition InitialPositionInStream/LATEST))
                  at-timestamp (.initialPositionInStreamExtended (InitialPositionInStreamExtended/newInitialPositionAtTimestamp at-timestamp))))))

(defn start-scheduler [scheduler]
  (doto (Thread. scheduler)
    (.setDaemon true)
    (.start)))

(defn stop-scheduler [scheduler & {:keys [timeout] :or {timeout 20000}}]
  (-> scheduler 
      (.startGracefulShutdown) 
      (deref timeout :timeout)))

;; TODO
;; handle threading in here
