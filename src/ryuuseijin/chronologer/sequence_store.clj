(ns ryuuseijin.chronologer.sequence-store
  (:require [ryuuseijin.chan-utils.core :refer [go-catchall]]))

(defprotocol SequenceStore
  (put! [this hash-key range-key data])
  (fetch-entries
    [this hash-key]
    [this hash-key limit]
    [this hash-key limit direction]))

(def duplicate-range-key-error ::duplicate-range-key)

(defrecord InMemorySequenceStore [state]
  SequenceStore
  (put! [this hash-key range-key data]
    (go-catchall
     (swap! state update hash-key
            (fn [entries]
              (when (get entries range-key)
                (throw (ex-info "range-key must be unique within a hash-key"
                                {:error-key duplicate-range-key-error})))
              (assoc (or entries (sorted-map)) range-key data)))
     nil))

  (fetch-entries [this hash-key]
    (fetch-entries this hash-key nil))
  (fetch-entries [this hash-key limit]
    (fetch-entries this hash-key limit :ascending))
  (fetch-entries [this hash-key limit direction]
    (go-catchall
     (assert (contains? #{:ascending :descending} direction))
     (some-> @state
             (get hash-key)
             (cond-> (= :descending direction) rseq)
             seq ;; don't return an ordered-map
             (cond-> limit (->> (take limit)))))))

(defn in-memory-sequence-store []
  (InMemorySequenceStore. (atom {})))
