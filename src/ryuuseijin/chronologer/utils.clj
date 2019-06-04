(ns ryuuseijin.chronologer.utils)

(defn map-vals [f m]
  (->> m
       (map (fn [[k v]] [k (f v)]))
       (into (empty m))))

(defn map-keys [f m]
  (->> m
       (map (fn [[k v]] [(f k) v]))
       (into (empty m))))
