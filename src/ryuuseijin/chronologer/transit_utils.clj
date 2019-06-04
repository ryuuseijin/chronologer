(ns ryuuseijin.chronologer.transit-utils
  (:require [cognitect.transit :as transit])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream]))

(defn serialize [data]
  (with-open [out (ByteArrayOutputStream. 4096)]
    (transit/write (transit/writer out :msgpack) data)
    (.toByteArray out)))

(defn deserialize [bytes]
  (with-open [in (ByteArrayInputStream. bytes)]
    (transit/read (transit/reader in :msgpack))))
