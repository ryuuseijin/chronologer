(ns ryuuseijin.chronologer.kv-store-test
  (:require [clojure.test :refer :all]
            [ryuuseijin.chan-utils.core :refer [<!?]]
            [ryuuseijin.chronologer.kv-store :as kvs]
            [ryuuseijin.chronologer.dynamodb-kv-store :as dynamodb-kvs]
            [ryuuseijin.chronologer.dynamodb-utils :as dynamodb]))

(def ^:dynamic *kv-store* nil)

(defn with-in-memory-kv-store [f]
  (binding [*kv-store* (kvs/in-memory-kv-store)]
    (f)))

(use-fixtures :each with-in-memory-kv-store)

(deftest put!
  (testing "putting values into a kv-store"
    (is (= 0 (<!? (kvs/put! *kv-store* "key" nil "data"))))
    (is (= 1 (<!? (kvs/put! *kv-store* "key" 0   "data"))))
    (is (= 2 (<!? (kvs/put! *kv-store* "key" 1   "data"))))))

(deftest fetch
  (testing "fetching values from a kv-store"
    (<!? (kvs/put! *kv-store* "key1" nil "data1"))
    (<!? (kvs/put! *kv-store* "key2" nil "data2"))
    (<!? (kvs/put! *kv-store* "key3" nil "data3"))
    (is (= [0 "data1"] (<!? (kvs/fetch *kv-store* "key1"))))
    (is (= [0 "data2"] (<!? (kvs/fetch *kv-store* "key2"))))
    (is (= [0 "data3"] (<!? (kvs/fetch *kv-store* "key3"))))))

(deftest put!-with-guard
  (testing "invalid update-key with non-existing key prevents update"
    (is (nil? (<!? (kvs/put! *kv-store* "non-existing" 0 "data"))))
    (is (nil? (<!? (kvs/put! *kv-store* "non-existing" 1 "data"))))
    (is (nil? (<!? (kvs/fetch *kv-store* "non-existing")))))

  (testing "with existing key"
    (let [update-key (<!? (kvs/put! *kv-store* "key" nil "data"))]

      (testing "invalid update-key prevents update"
        (is (nil? (<!? (kvs/put! *kv-store* "key" nil "ignored"))))
        (is (nil? (<!? (kvs/put! *kv-store* "key" 12345 "ignored")))) ;; invalid update key
        (is (= [0 "data"] (<!? (kvs/fetch *kv-store* "key")))))

      (testing "valid update-key allows update"
        (let [update-key (<!? (kvs/put! *kv-store* "key" update-key "data1"))]
          (is (some? (<!? (kvs/put! *kv-store* "key" update-key "data2"))))
          (is (= [2 "data2"] (<!? (kvs/fetch *kv-store* "key")))))))))
