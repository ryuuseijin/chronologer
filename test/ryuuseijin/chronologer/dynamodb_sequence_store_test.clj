(ns ryuuseijin.chronologer.dynamodb-sequence-store-test
  (:require [clojure.test :refer :all]
            [ryuuseijin.chronologer.sequence-store-test :as seqs-test]
            [ryuuseijin.chronologer.dynamodb-sequence-store :as dynamodb-seqs]
            [ryuuseijin.chronologer.dynamodb-utils :as dynamodb]))

(defn with-dynamodb-sequence-store [f]
  (let [table-name "kv-store-test"
        client (dynamodb/make-client "127.0.0.1" 8000)]
    (binding [seqs-test/*seq-store* (dynamodb-seqs/sequence-store client {:table table-name})]
      (dynamodb/delete-table client table-name)
      (dynamodb/create-simple-table client {:table table-name
                                            :hash-key {:name "h" :type "S"}
                                            :range-key {:name "r" :type "N"}})
      (f))))

(use-fixtures :each with-dynamodb-sequence-store)

(deftest put!-and-fetch
  (seqs-test/put!-and-fetch))
