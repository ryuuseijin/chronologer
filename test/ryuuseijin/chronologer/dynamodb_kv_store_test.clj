(ns ryuuseijin.chronologer.dynamodb-kv-store-test
  (:require [clojure.test :refer :all]
            [ryuuseijin.chronologer.kv-store-test :as kvs-test]
            [ryuuseijin.chronologer.dynamodb-kv-store :as dynamodb-kvs]
            [ryuuseijin.chronologer.dynamodb-utils :as dynamodb]))

(defn with-dynamodb-kv-store [f]
  (let [table-name "kv-store-test"
        client (dynamodb/make-client "127.0.0.1" 8000)]
    (binding [kvs-test/*kv-store* (dynamodb-kvs/kv-store client {:table table-name})]
      (dynamodb/delete-table client table-name)
      (dynamodb/create-simple-table client {:table table-name :key-column "key"})
      (f))))

(use-fixtures :each with-dynamodb-kv-store)

(deftest put!
  (kvs-test/put!))

(deftest fetch
  (kvs-test/fetch))

(deftest put!-with-guard
  (kvs-test/put!-with-guard))
