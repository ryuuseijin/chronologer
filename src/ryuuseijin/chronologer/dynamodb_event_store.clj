(ns ryuuseijin.chronologer.dynamodb-event-store
  (:require [cognitect.aws.client.api :as aws]
            [clojure.core.async :as async :refer [go <!]]
            [ryuuseijin.chronologer.event-store :as evs]
            [ryuuseijin.chronologer.dynamodb-kv-store :as dynamodb-kvs]
            [ryuuseijin.chronologer.dynamodb-sequence-store :as dynamodb-seqs]))

(defn event-store [client {:keys [meta-table
                                  event-table
                                  serialize
                                  deserialize]}]
  (evs/map->EventStore {:kv-store  (dynamodb-kvs/kv-store client {:table meta-table})
                        :seq-store (dynamodb-seqs/sequence-store client {:table event-table})
                        :serialize serialize
                        :deserialize deserialize}))
