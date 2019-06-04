(ns ryuuseijin.chronologer.sequence-store-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [<!!]]
            [ryuuseijin.chan-utils.core :refer [<!?]]
            [ryuuseijin.chronologer.sequence-store :as seqs]))

(def ^:dynamic *seq-store* nil)

(defn with-in-memory-sequence-store [f]
  (binding [*seq-store* (seqs/in-memory-sequence-store)]
    (f)))

(use-fixtures :each with-in-memory-sequence-store)

(deftest put!-and-fetch

  (testing "putting values into a sequence-store"
    (is (nil? (<!? (seqs/put! *seq-store* "key" 0 "data0"))))
    (is (nil? (<!? (seqs/put! *seq-store* "key" 1 "data1"))))
    (is (nil? (<!? (seqs/put! *seq-store* "key" 2 "data2")))))

  (testing "overwriting values results in a duplicate-key exception"
    (is (= ::seqs/duplicate-range-key
           (-> (seqs/put! *seq-store* "key" 1 "data3")
               <!!
               ex-data
               :error-key))))

  (testing "fetching all values from a sequence-store"
    (is (= [[0 "data0"]
            [1 "data1"]
            [2 "data2"]]
           (<!? (seqs/fetch-entries *seq-store* "key")))))

  (testing "fetching a limited number of values"
    (is (= [[0 "data0"]
            [1 "data1"]]
           (<!? (seqs/fetch-entries *seq-store* "key" 2)))))

  (testing "fetching a number of values in reverse"
    (is (= [[2 "data2"]
            [1 "data1"]
            [0 "data0"]]
           (<!? (seqs/fetch-entries *seq-store* "key" nil :descending))))))
