(ns app
  (:require [consumer :as c]
            [producer :as p]
            [clojure.tools.logging :as log]))

(def STREAM_NAME "test")
(def REGION "eu-west-1")

(defn process-records [received-record]
  (log/info "received record" received-record))

(defn generate-records [client]
  (future
    (doall
     (for [id (repeatedly #(.toString (java.util.UUID/randomUUID)))
           :while (not (Thread/interrupted))]
       (do (Thread/sleep 1000)
           (p/put-record client STREAM_NAME {:id id :published (.toString (java.time.LocalDateTime/now))}))))))

(defn -main []
  (log/info "Simplest Kinesis")
  (with-open [client (c/async-kinesis-client :region REGION)]
    (let [scheduler (c/scheduler! STREAM_NAME process-records :app-name "simplest-kinesis-app" :region REGION :kinesis-client client )
          generator (generate-records client)]

      (log/debug "starting kinesis client scheduler")
      (c/start-scheduler scheduler)

      (log/info "Started. Press enter to exit")
      (read-line)
      (future-cancel generator)
      (c/stop-scheduler scheduler))))