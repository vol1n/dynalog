(ns vol1n.dynalog.core
  (:require [com.grzm.awyeah.client.api :as aws]
            [clojure.stacktrace]
            [clojure.core.async :as async]
            [vol1n.dynalog.schema :as schema]))

(defn client [{:keys [aws-region]}]
  {:dynamo (aws/client {:api :dynamodb :region aws-region})})

(defn- table-exists? [client table-name]
  (let [response (aws/invoke (:dynamo client) {:op :DescribeTable
                                               :request {:TableName table-name}})]
    (cond 
      (= (:cognitect.aws.error/code response) "AccessDeniedException") (throw (ex-info "Access Denied" {:message (:Message response) }))
      (:Table response) true
      :else false)))

(defn- wait-for-delete
  "Retries checking if a table exists with exponential backoff.
   Throws if the table still doesn't exist after `max-retries` attempts."
  [client table-name max-retries]
  (async/<!!
   (async/go-loop [attempt 1]
     (if (not (table-exists? client table-name))
       (println "✅ Table deleted" table-name)  ;; Exit successfully
       (if (< attempt max-retries)
         (do
           (async/<! (async/timeout (* 100 (Math/pow 2 attempt))))  ;; Exponential backoff
           (recur (inc attempt)))
         (throw (ex-info (str "🚨 Table " table-name " exists " max-retries " retries!")
                         {:table table-name})))))))

(defn- table-active?
  "Checks if a DynamoDB table is in the ACTIVE state."
  [client table-name]
  (let [response (aws/invoke (:dynamo client) {:op :DescribeTable
                                     :request {:TableName table-name}})]
    (when (contains? response :cognitect.aws.error/code)
      (throw (ex-info "AWS error" {:aws-error response :message (:message response)})))
    (when (not (contains? response :cognitect.anomalies/category))
      (= "ACTIVE" (get-in response [:Table :TableStatus])))))  ;; Check if table is active

(defn- wait-for-table-create
  "Waits for a DynamoDB table to become ACTIVE, with exponential backoff."
  [client table-name max-retries]
  (async/<!!
   (async/go-loop [attempt 1]
     (if (table-active? client table-name)
       (println "✅ Table created" table-name)  ;; Exit successfully
       (if (< attempt max-retries)
         (do
           (async/<! (async/timeout (* 100 (Math/pow 2 attempt))))  ;; Exponential backoff
           (recur (inc attempt)))
         (throw (ex-info (str "🚨 Table " table-name " not active after " max-retries " retries!")
                         {:table table-name})))))))
  

(defn- delete-table! [client table-name]
  (let [response (aws/invoke (:dynamo client)
                             {:op :DeleteTable
                              :request {:TableName table-name}})]
    (cond
      (= (:cognitect.aws.error/code response) "AccessDeniedException") (throw (ex-info "Access Denied" {:message (:Message response)}))
      :else false)))

(defn- create-table! [client table-name]
  (let [payload {:TableName table-name
                 :KeySchema [{:AttributeName "entity-id" :KeyType "HASH"}
                             {:AttributeName "attribute-tx-value" :KeyType "RANGE"}]
                 :AttributeDefinitions [{:AttributeName "entity-id" :AttributeType "S"}
                                        {:AttributeName "attribute" :AttributeType "S"}
                                        {:AttributeName "value" :AttributeType "S"}
                                        {:AttributeName "tx-id" :AttributeType "S"}
                                        {:AttributeName "attribute-tx-value" :AttributeType "S"}
                                        {:AttributeName "attribute-value" :AttributeType "S"}
                                        {:AttributeName "latest" :AttributeType "N"} ;; 1 or 0
                                        {:AttributeName "is-schema" :AttributeType "N"}
                                        {:AttributeName "value-as-number" :AttributeType "N"}
                                        {:AttributeName "entity-id-attribute" :AttributeType "S"}
                                        {:AttributeName "entity-id-attribute-value" :AttributeType "S"}]
                 :GlobalSecondaryIndexes [{:IndexName "latest-index"
                                           :KeySchema [{:AttributeName "entity-id" :KeyType "HASH"}
                                                       {:AttributeName "latest" :KeyType "RANGE"}]
                                           :Projection {:ProjectionType "ALL"}}
                                          {:IndexName "schema-index"
                                           :KeySchema [{:AttributeName "is-schema" :KeyType "HASH"}]
                                           :Projection {:ProjectionType "ALL"}}
                                          {:IndexName "attribute-value-index"
                                           :KeySchema [{:AttributeName "attribute-value" :KeyType "HASH"}]
                                           :Projection {:ProjectionType "ALL"}}
                                          {:IndexName "tx-id-index"
                                           :KeySchema [{:AttributeName "tx-id" :KeyType "HASH"}]
                                           :Projection {:ProjectionType "ALL"}}
                                          {:IndexName "unique-attr-index"
                                           :KeySchema [{:AttributeName "attribute" :KeyType "HASH"}
                                                       {:AttributeName "value" :KeyType "RANGE"}]
                                           :Projection {:ProjectionType "ALL"}}
                                          {:IndexName "entity-id-index"
                                           :KeySchema [{:AttributeName "entity-id" :KeyType "HASH"}]
                                           :Projection {:ProjectionType "ALL"}}
                                          {:IndexName "value-as-number-index"
                                           :KeySchema [{:AttributeName "value-as-number" :KeyType "HASH"}]
                                           :Projection {:ProjectionType "ALL"}}
                                          {:IndexName "entity-id-attribute-index"
                                           :KeySchema [{:AttributeName "entity-id-attribute" :KeyType "HASH"}]
                                           :Projection {:ProjectionType "ALL"}}
                                          {:IndexName "attribute-index"
                                           :KeySchema [{:AttributeName "attribute" :KeyType "HASH"}]
                                           :Projection {:ProjectionType "ALL"}}
                                          {:IndexName "entity-id-attribute-value-index"
                                           :KeySchema [{:AttributeName "entity-id-attribute-value" :KeyType "HASH"}]
                                           :Projection {:ProjectionType "ALL"}}]
                 :BillingMode "PAY_PER_REQUEST"}
        response (aws/invoke (:dynamo client)
                             {:op :CreateTable
                              :request payload})]
    (cond
      (= (:cognitect.aws.error/code response) "AccessDeniedException") (throw (ex-info "Access Denied" {:message (:Message response)}))
      (= (:cognitect.aws.error/code response) "ValidationException") (throw (ex-info "Critical Error" {:message (:message response)}))
      (:Table response) true
      :else false)
    response))

(defn clear-table! [conn table-name]
  (when (table-exists? conn table-name)
    (delete-table! conn table-name)
    (wait-for-delete conn table-name 10)
    (create-table! conn table-name)
    (try
      (wait-for-table-create conn table-name 10)
      (catch Exception e
        (println "🔥 Exception during wait:" (.getMessage e))))))

(defn- create-table-if-not-exists! [client table-name]
  (when (not (table-exists? client table-name))
    (println "creating table" table-name)
    (create-table! client table-name)
    (wait-for-table-create client table-name 10)))

(defn connect [client db-name]
  (create-table-if-not-exists! client db-name)
  (schema/update-schema! client db-name)
  (assoc client :connected true :table-name db-name))

(defn db [conn]
  (when (:connected conn)
    {:db/status "connected"
     :aws-region (:aws-region conn)
     :table-name (:table-name conn)}))