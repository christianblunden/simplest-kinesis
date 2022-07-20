(ns replay
  (:require [consumer :as c]
            [clojure.tools.logging :as log]))

(def STREAM_NAME "test")
(def REGION "eu-west-1")

(defn process-records [received-record]
  (log/info "received record" received-record))


(defn -main []
  (log/info "Simplest Kinesis")
  (let [scheduler (c/scheduler! STREAM_NAME
                                              process-records
                                              :region REGION
                                              :from-start true
                                            ;;   :from-end true
                                            ;;   :at-timestamp #inst "2021-11-09T00:00"
                                              )]

    (log/debug "starting kinesis client scheduler")
    (c/start-scheduler scheduler)

    (log/info "Started. Press enter to exit")
    (read-line)
    (c/stop-scheduler scheduler)))