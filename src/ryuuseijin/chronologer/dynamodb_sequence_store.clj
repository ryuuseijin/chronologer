(ns ryuuseijin.chronologer.dynamodb-sequence-store
  (:require [ryuuseijin.chan-utils.core :refer [thread-catchall]]
            [ryuuseijin.chronologer.sequence-store :as seqs]
            [ryuuseijin.chronologer.dynamodb-utils :as dynamodb]))

(defn sequence-store [client {:keys [table]}]
  (reify
    seqs/SequenceStore
    (put! [this hash-key range-key data]
      (thread-catchall
       (when-not (dynamodb/put-item client
                                    ;; use single-letter keys to reduce storage space
                                    {"h" hash-key
                                     "r" range-key
                                     "d" data}
                                    {:table table
                                     :not-exists-attr "h"})
         (throw (ex-info "range-key must be unique within a hash-key"
                         {:error-key seqs/duplicate-range-key-error})))
       nil))

    (fetch-entries [this hash-key]
      (seqs/fetch-entries this hash-key nil))
    (fetch-entries [this hash-key limit]
      (seqs/fetch-entries this hash-key limit :ascending))
    (fetch-entries [this hash-key limit direction]
      (thread-catchall
       (->> (dynamodb/query-items client
                                  {"h" hash-key}
                                  {:table table
                                   :limit limit
                                   :direction direction})
            (map (juxt :r :d)))))))
