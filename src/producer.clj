(ns producer
  (:require [jsonista.core :as j]
            [clojure.tools.logging :as log])
  (:import [software.amazon.awssdk.services.kinesis.model PutRecordRequest PutRecordsRequest PutRecordsRequestEntry]
           [org.apache.commons.lang3 RandomStringUtils]
           [software.amazon.awssdk.core SdkBytes]))

;; https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/kinesis/KinesisAsyncClient.html
;; putRecord returns a CompletableFuture

;; https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/SdkBytes.html

(defn to-bytes [r]
  (-> r 
      (j/write-value-as-bytes j/default-object-mapper) 
      (SdkBytes/fromByteArray)))

(defn build-record [builder r]
  (-> builder
      (.partitionKey (RandomStringUtils/randomAlphabetic 5 20)) ; randomised partition
      (.data (to-bytes r))
      (.build)))

(defn put-record [kinesis-client stream record]
  (let [request (-> (PutRecordRequest/builder)
                    (.streamName stream)
                    (build-record record))]
    (try
      (log/debugf "putRecord to stream:%s record:%s" stream record)
      (-> kinesis-client
          (.putRecord request) ; returns CompletableFuture
          (.join)) ; get is blocking
      (catch Exception e
        (log/error e "Could not put record")))))

;; (defn put-records [kinesis-client stream records]
;;   (let [request (-> (PutRecordsRequest/builder)
;;                     (.streamName stream)
;;                     (.records (map #(build-record (PutRecordsRequestEntry/builder) %) records))
;;                     (.build))]
;;     (-> kinesis-client
;;         (.putRecords request) ; returns CompletableFuture
;;         (.get))))  ; get is blocking