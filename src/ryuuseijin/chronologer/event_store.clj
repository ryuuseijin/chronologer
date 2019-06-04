(ns ryuuseijin.chronologer.event-store
  (:require [clojure.core.async :as async :refer [go]]
            [ryuuseijin.chan-utils.core :refer [go-catchall <?]]
            [ryuuseijin.chronologer.kv-store :as kvs]
            [ryuuseijin.chronologer.sequence-store :as seqs]))

(defprotocol IEventStore
  (initialize [this params])
  (append! [this event])
  (fetch-partition [this partition-number]))

(defn partition-key [hash-key partition-number]
  (str partition-number \- hash-key))

(defrecord EventStore [;; set once during initialization
                       hash-key
                       max-events-per-partition
                       events-info
                       events-info-update-key
                       serialize
                       deserialize

                       ;; changes over the lifetime of the store
                       partition-number
                       sequence-number

                       ;; implemenation details
                       kv-store
                       seq-store]
  IEventStore
  (initialize [this {:keys [hash-key max-events-per-partition] :as params}]
    (go-catchall
     (let [[update-key maybe-serialized-events-info]
           (<? (kvs/fetch kv-store hash-key))

           {:keys [partition-number] :as events-info}
           (or (some-> maybe-serialized-events-info deserialize)
               {:partition-number 0})

           pkey (partition-key hash-key partition-number)
           [[last-sequence-number {:keys [final] :as last-event}]]
           (<? (seqs/fetch-entries seq-store pkey 1 :descending))

           ;; Getting a final event means the events-info stored in the
           ;; kv-store was not updated correctly to the most recent
           ;; partition number after an append!.
           fixed-events-info
           (cond-> events-info final (update :partition-number inc))

           ;; We perform a sort of read-repair so that even if the
           ;; next partition is also final we eventually, after trying to
           ;; initialize repeatedly, get to a partition that is not final.
           new-update-key
           (if final
             (<? (kvs/put! kv-store hash-key update-key
                           (serialize fixed-events-info)))
             update-key)]

       (assoc this
              :hash-key hash-key
              :max-events-per-partition max-events-per-partition
              :events-info fixed-events-info
              :events-info-update-key new-update-key

              :partition-number (:partition-number fixed-events-info)
              :sequence-number (inc (or last-sequence-number -1))))))

  (append! [this event]
    (go-catchall
     (let [final (<= max-events-per-partition (inc sequence-number))
           data (cond-> {:d event} final (assoc :final true))

           pkey (partition-key hash-key partition-number)
           _ (<? (seqs/put! seq-store pkey sequence-number (serialize data)))

           new-events-info (cond-> events-info final (update :partition-number inc))
           new-update-key (when final
                            (<? (kvs/put! kv-store hash-key events-info-update-key
                                          (serialize new-events-info))))]
       (-> this
           (update :sequence-number inc)
           (cond-> final (-> (update :partition-number inc)
                             (assoc :sequence-number 0
                                    :events-info new-events-info
                                    :events-info-update-key new-update-key)))))))

  (fetch-partition [this partition-number]
    (go-catchall
     ;;XX throw exception if state doesn't match?
     (->> (<? (seqs/fetch-entries seq-store (partition-key hash-key partition-number)))
          (map (comp :d deserialize second))))))

(defn in-memory-event-store []
  (map->EventStore {:kv-store  (kvs/in-memory-kv-store)
                    :seq-store (seqs/in-memory-sequence-store)
                    :serialize identity
                    :deserialize identity}))
