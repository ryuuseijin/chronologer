(ns ryuuseijin.chronologer.kv-store
  (:require [ryuuseijin.chan-utils.core :refer [go-catchall]]))

(defprotocol KvStore
  (put! [this hash-key update-key new-data])
  (fetch [this hash-key]))

(defn swap-some! [state f & f-params]
  (loop []
    (let [old-state @state
          new-state (apply f old-state f-params)]
      (when (some? new-state)
        (if (compare-and-set! state old-state new-state)
          new-state
          (recur))))))

(defrecord InMemoryKvStore [state]
  KvStore
  (put! [this hash-key update-key new-data]
    (go-catchall
     (when-let [m (swap-some!
                   state
                   (fn [m]
                     (let [[old-update-key _] (get m hash-key)
                           new-update-key (inc (or old-update-key -1))]
                       (when (= old-update-key update-key)
                         (assoc m hash-key [new-update-key new-data])))))]
       (let [[new-update-key _] (get m hash-key)]
         new-update-key))))
  (fetch [this hash-key]
    (go-catchall
     (get @state hash-key))))

(defn in-memory-kv-store []
  (InMemoryKvStore. (atom {})))
