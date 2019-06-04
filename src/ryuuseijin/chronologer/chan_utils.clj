(ns ryuuseijin.chronologer.chan-utils
  (:require [clojure.core.async :refer [promise-chan put! take!]]))

(defn all [& chs]
  (let [result-promise (promise-chan)
        result (atom (vec (repeat (count chs) nil)))
        results-processed (atom 0)]
    (doseq [[ch i] (map vector chs (range))]
      (take! ch (fn [ch-result]
                  (swap! result assoc i ch-result)
                  (when (= (swap! results-processed inc)
                           (count @result))
                    (put! result-promise @result)))))
    result-promise))
