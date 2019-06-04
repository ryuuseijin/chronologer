(ns ryuuseijin.chronologer.dynamodb-kv-store
  (:require [ryuuseijin.chan-utils.core :refer [thread-catchall]]
            [ryuuseijin.chronologer.kv-store :as kvs]
            [ryuuseijin.chronologer.dynamodb-utils :as dynamodb]))

(defn kv-store [client {:keys [table]}]
  (reify
    kvs/KvStore
    (put! [this hash-key update-key new-data]
      (thread-catchall
        (let [new-update-key (inc (or update-key -1))]
          (when (dynamodb/put-item client
                                   {"key" hash-key
                                    "val" new-data
                                    "guard" new-update-key}
                                   (if (some? update-key)
                                     {:table table
                                      :equals-spec {:attr "guard"
                                                    :val update-key}}
                                     {:table table
                                      :not-exists-attr "key"}))
            new-update-key))))
    (fetch [this hash-key]
      (thread-catchall
       ;;XX accepts string keys but returns keyword keys?
       (when-let [{:keys [guard val] :as result}
                  (dynamodb/get-item client {"key" hash-key} {:table table})]
         [guard val])))))
