(ns ryuuseijin.chronologer.event-store-test
  (:require [clojure.test :refer :all]
            [ryuuseijin.chan-utils.core :refer [<!?]]
            [ryuuseijin.chronologer.event-store :as evs]))

(def ^:dynamic *event-store-constructor* nil)

(defn with-in-memory-event-store-constructor [f]
  (binding [*event-store-constructor* evs/in-memory-event-store]
    (f)))

(use-fixtures :once with-in-memory-event-store-constructor)

(def default-params {:hash-key "some-event-history"
                     :max-events-per-partition 2})

(deftest initialize-empty
  (testing "initializing an event-store with empty event history"
    (let [event-store (evs/in-memory-event-store)
          expected (merge
                    default-params
                    {:events-info {:partition-number 0}
                     :events-info-update-key nil
                     :partition-number 0
                     :sequence-number 0})]
      (is (= expected
             (-> (<!? (evs/initialize event-store default-params))
                 (select-keys (keys expected))))))))

(defn make-event-store [events]
  (let [empty-event-store (-> (*event-store-constructor*)
                              (evs/initialize default-params)
                              <!?)]
    (reduce (fn [event-store event]
              (<!? (evs/append! event-store event)))
            empty-event-store
            events)))

(deftest appending-events
  (testing "appending events"
    (let [event-store (make-event-store
                       [{:data 1} ;; partition 0 sequence 0
                        {:data 2} ;; partition 0 sequence 1
                        {:data 3} ;; partition 1 sequence 0
                        ])
          expected (merge
                    default-params
                    {:events-info {:partition-number 1}
                     :events-info-update-key 0
                     :partition-number 1
                     :sequence-number 1})]
      (is (= expected
             (select-keys event-store (keys expected)))))))

(deftest initialize-existing
  (let [event-store (make-event-store
                     [{:data 1} ;; partition 0 sequence 0
                      {:data 2} ;; partition 0 sequence 1
                      {:data 3} ;; partition 1 sequence 0
                      ])]

    (testing "initializing an event-store with non-empty event history"
      (let [expected (merge
                      default-params
                      {:events-info {:partition-number 1}
                       :events-info-update-key 0
                       :partition-number 1
                       :sequence-number 1})]
        (is (= expected
               (-> (<!? (evs/initialize event-store default-params))
                   (select-keys (keys expected)))))))

    (testing "append to re-initialized event history"
      (let [expected (merge
                      default-params
                      {:events-info {:partition-number 2}
                       :events-info-update-key 1
                       :partition-number 2
                       :sequence-number 0})]
        (is (= expected
               (-> (evs/initialize event-store default-params)
                   <!?
                   (evs/append! {:data 4}) ;; partition 1 sequence 1
                   <!?
                   (select-keys (keys expected)))))))))

(deftest fetching-partition
  (let [event-store (make-event-store
                     [{:data 1} ;; partition 0 sequence 0
                      {:data 2} ;; partition 0 sequence 1
                      {:data 3} ;; partition 1 sequence 0
                      ])]
    (testing "fetching a partition of event history"
      (is (= [{:data 1}
              {:data 2}]
             (<!? (evs/fetch-partition event-store 0))))
      (is (= [{:data 3}]
             (<!? (evs/fetch-partition event-store 1)))))))
