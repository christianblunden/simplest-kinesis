(ns simple-client
  (:require [consumer :as c]
            [clojure.tools.logging :as log])
  (:import
   [software.amazon.awssdk.services.kinesis.model ListShardsRequest GetShardIteratorRequest GetRecordsRequest]))


(defn- get-shard-iterator [stream-name shard client]
  (-> client
      (.getShardIterator (-> (GetShardIteratorRequest/builder)
                             (.streamName stream-name)
                             (.shardId (.shardId shard))
                             (.shardIteratorType "TRIM_HORIZON")
                             (.build)))
      (.get)
      (.shardIterator)))

(defn- list-shards [stream-name client]
  (-> client
      (.listShards (-> (ListShardsRequest/builder)
                       (.streamName stream-name)
                       (.build)))
      (.get)
      (.shards)))

(defn- get-record-result [shard-iterator client]
  (-> client
      (.getRecords (-> (GetRecordsRequest/builder)
                       (.shardIterator shard-iterator)
                       (.limit (int 25))
                       (.build)))
      (.get)))

(defn- follow-iterator [iterator client]
  (log/debug "following:" iterator)
  (let [result (get-record-result iterator client)
        next (.nextShardIterator result)] ; shard has ended when next is nil
    (when next
      (cons (.records result)
            (lazy-seq (follow-iterator next client))))))

(defn simple-get-records [client stream-name & {:as options}]
  (for [shard (list-shards stream-name client)
        :let [start-iterator (get-shard-iterator stream-name shard client)]
        records (follow-iterator start-iterator client)]
    records))

(comment
  
  (def client (c/async-kinesis-client :region "eu-west-1"))
  (def shards (list-shards "test" client))
  (def iterator (get-shard-iterator "test" (first shards) client))

  (take 2 (follow-iterator iterator client))

  (take 5 (simple-get-records client "test"))

  )
  
  
