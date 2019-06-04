(ns ryuuseijin.chronologer.dynamodb-event-store-test
  (:require [clojure.test :refer :all]
            [ryuuseijin.chronologer.event-store-test :as event-store-test]
            [ryuuseijin.chronologer.dynamodb-event-store :as dynamodb-event-store]
            [ryuuseijin.chronologer.dynamodb-utils :as dynamodb]
            [ryuuseijin.chronologer.transit-utils :as transit-utils]))

(defn with-dynamodb-event-store-constructor [f]
  (let [meta-table "meta-test"
        event-table "event-test"
        client (dynamodb/make-client "127.0.0.1" 8000)]
    (binding [event-store-test/*event-store-constructor*
              #(dynamodb-event-store/event-store client
                                                 {:meta-table meta-table
                                                  :event-table event-table
                                                  :serialize transit-utils/serialize
                                                  :deserialize transit-utils/deserialize})]
      (dynamodb/delete-table client event-table)
      (dynamodb/create-simple-table client {:table event-table
                                            :hash-key {:name "h" :type "S"}
                                            :range-key {:name "r" :type "N"}})
      (dynamodb/delete-table client meta-table)
      (dynamodb/create-simple-table client {:table meta-table
                                            :hash-key {:name "key" :type "S"}})
      (f))))

(use-fixtures :each with-dynamodb-event-store-constructor)

(deftest initialize-empty
  (event-store-test/initialize-empty))

(deftest appending-events
  (event-store-test/appending-events))

(deftest initialize-existing
  (event-store-test/initialize-existing))

(deftest fetching-partition
  (event-store-test/fetching-partition))
