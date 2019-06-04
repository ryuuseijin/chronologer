(ns ryuuseijin.chronologer.dynamodb-utils
  (:require [clojure.string :as string]
            [clojure.java.io :as io]
            [cognitect.aws.client.api :as aws]
            [ryuuseijin.chronologer.utils :refer [map-vals]]))

(defn make-client [host port]
  (let [client (aws/client {:api :dynamodb
                            :region "us-west-1"
                            :endpoint-override {:protocol :http
                                                :hostname host
                                                :port port}})]
    (aws/validate-requests client true) ;;XX only in dev mode
    client))

(defn delete-table [client table]
  (aws/invoke client
              {:op      :DeleteTable
               :request {:TableName table}}))

(defn create-simple-table [client {:keys [table]
                                   {hash-key-type :type
                                    hash-key-name :name :as hash-key} :hash-key
                                   {range-key-type :type
                                    range-key-name :name :as range-key} :range-key}]
  (let [key-attribute   {:AttributeName hash-key-name
                         :AttributeType hash-key-type}
        key-schema      {:AttributeName hash-key-name
                         :KeyType "HASH"}
        range-attribute (when range-key
                          {:AttributeName range-key-name
                           :AttributeType range-key-type})
        range-schema    (when range-key
                          {:AttributeName range-key-name
                           :KeyType "RANGE"})]
    (aws/invoke client
                {:op      :CreateTable
                 :request {:TableName table
                           :AttributeDefinitions
                           (-> [key-attribute]
                               (cond-> range-attribute (conj range-attribute)))
                           :KeySchema
                           (-> [key-schema]
                               (cond-> range-schema (conj range-schema)))
                           :ProvisionedThroughput {:ReadCapacityUnits  1
                                                   :WriteCapacityUnits 1}}})))

(defn is-conditional-check-failed-error? [aws-result]
  (when-let [error-type (:__type aws-result)]
    (let [[namespace error] (string/split error-type #"#")]
      (= error "ConditionalCheckFailedException"))))

;; Incomplete, see: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html
(defn make-dynamodb-value-spec [v]
  (cond
    (string? v) {:S v}
    (number? v) {:N (str v)}
    (bytes?  v) {:B v}
    :else (throw (ex-info "unknown value type"
                          {:error-type ::unknown-value-type
                           :error-info {:value-type (type v)}}))))

(defn input-stream-to-byte-array [input-stream]
  (with-open [out (java.io.ByteArrayOutputStream.)]
    (io/copy input-stream out)
    (.toByteArray out)))

(defn parse-dynamodb-value-spec [spec]
  (let [k (first (keys spec))
        v (get spec k)] 
    (case k
      :S v
      :N (Long/parseLong v)
      :B (cond-> v (instance? java.io.InputStream v) input-stream-to-byte-array)
      (throw (ex-info "unknown value type"
                      {:error-type ::unknown-value-type
                       :error-info {:value-type k}})))))

;; Why are spec validation errors not exceptions?
(defn ensure-no-spec-issues [result]
  (when (:clojure.spec.alpha/problems result)
    (throw (ex-info "spec problems" {:error-info result}))))

(defn put-item [client item {:keys [table not-exists-attr]
                            {equals-attr :attr
                             equals-val :val
                             :as equals-spec} :equals-spec}]
  (assert (not (and not-exists-attr equals-spec))
          "only one of not-exists-attr or equals-spec allowed")
  (let [request {:TableName table
                 :Item (map-vals make-dynamodb-value-spec item)}
        request-with-condition
        (cond-> request
          not-exists-attr
          (assoc :ConditionExpression "attribute_not_exists(#key)"
                 :ExpressionAttributeNames {"#key" not-exists-attr})
          equals-attr
          (assoc :ConditionExpression "#guard = :guard"
                 :ExpressionAttributeNames {"#guard" equals-attr}
                 :ExpressionAttributeValues
                 {":guard" (make-dynamodb-value-spec equals-val)}))
        result (aws/invoke client {:op :PutItem :request request-with-condition})]
    (ensure-no-spec-issues result)
    (cond
      (= result {}) true ;; success
      (is-conditional-check-failed-error? result) false
      :default (throw (ex-info "error putting item"
                               {:error-type ::dynamodb-put-failed
                                :error-info result})))))

(defn get-item [client item-keys {:keys [table]}]
  (let [result (aws/invoke client
                           {:op :GetItem
                            :request {:TableName table
                                      :Key (map-vals make-dynamodb-value-spec
                                                     item-keys)
                                      :ConsistentRead true}})]
    (ensure-no-spec-issues result)
    ;; {:Item {:key {:S "..."} ...}} -> {:key "..."}
    (when-let [item-specs (not-empty (:Item result))]
      (map-vals parse-dynamodb-value-spec item-specs))))

(defn query-items [client item-key {:keys [table limit direction]}]
  (assert (contains? #{:ascending :descending} direction))
  (let [[key-name key-val] (first item-key)
        result (aws/invoke client
                           {:op :Query
                            :request (-> {:TableName table
                                          :ScanIndexForward (case direction
                                                              :ascending true
                                                              :descending false)
                                          :KeyConditionExpression "#key = :val"
                                          :ExpressionAttributeNames {"#key" key-name}
                                          :ExpressionAttributeValues
                                          {":val" (make-dynamodb-value-spec key-val)}}
                                         (cond-> limit (assoc :Limit limit)))})]
    (ensure-no-spec-issues result)
    ;; {:Items [{:key {:S "..."} ...}}] ->  [{:key "..."}]
    (map (fn [item-specs]
           (map-vals parse-dynamodb-value-spec item-specs))
         (:Items result))))
