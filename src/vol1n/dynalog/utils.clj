(ns vol1n.dynalog.utils
  (:require [com.grzm.awyeah.client.api :as aws]
            [vol1n.dynalog.schema :as schema]))

(def cache
  (atom {:attr-value {} 
         :attribute {}
         :entity-id-attribute {}
         :entity-id {}
         :tx-id {}}))

(defn keywordize-leading-colon [s]
  (keyword (subs s 1)))

(defn get-attribute-type [attribute]
  (let [constraints (get @schema/schema-cache attribute)]
    (:db/valueType constraints)))

(defn cast-value [attribute value]
  (let [type (get-attribute-type attribute)]
    (if (nil? value)
      nil
      (case type
        :db.type/keyword (keywordize-leading-colon value)
        :db.type/string value
        :db.type/boolean (if (= value "true") true false)
        :db.type/long (Long/parseLong value)
        :db.type/instant (java.time.Instant/parse value)
        :db.type/ref value))))

(defn- format-response [dynamo-response]
  (println "format-response" dynamo-response)
  (keep (fn [item]
          (let [entity-id (get-in item [:entity-id :S])
                attribute (get-in item [:attribute :S])
                value (get-in item [:value :S])
                tx-id (get-in item [:tx-id :S])
                retraction-tx (get-in item [:retracted :S])]
            (if (nil? retraction-tx)
              {:attribute (keywordize-leading-colon attribute)
               :entity-id entity-id
               :value (cast-value (keywordize-leading-colon attribute) value)
               :tx-id tx-id}
              nil))) (:Items dynamo-response)))

(defn fetch-entity [dynamo table-name entity-id]
  (let [dynamo-response (aws/invoke dynamo
              {:op :Query
               :request {:TableName table-name
                         :IndexName "entity-id-index"
                         :KeyConditionExpression "#eid = :refval"
                         :ExpressionAttributeNames {"#eid" "entity-id"}
                         :ExpressionAttributeValues {":refval" {:S entity-id}}}})]
    (format-response dynamo-response)))

(defn fetch-by-attribute [dynamo table-name attribute]
  (let [dynamo-response (aws/invoke dynamo
              {:op :Query
               :request {:TableName table-name
                         :IndexName "attribute-index"
                         :KeyConditionExpression "#attr = :attr"
                         :ExpressionAttributeNames {"#attr" "attribute"}
                         :ExpressionAttributeValues {":attr" {:S (str attribute)}}}})]
    (format-response dynamo-response)))

(defn fetch-by-attribute-value [dynamo table-name attribute value] 
  (let [dynamo-response (aws/invoke dynamo
                                    {:op :Query
                                     :request {:TableName table-name
                                               :IndexName "attribute-value-index"
                                               :KeyConditionExpression "#attr = :attrval"
                                               :ExpressionAttributeNames {"#attr" "attribute-value"}
                                               :ExpressionAttributeValues {":attrval" {:S (str attribute "#" value)}}}})
        formatted (format-response dynamo-response)]
    formatted))

(defn fetch-by-entity-id-attribute [dynamo table-name entity-id attribute]
  (let [dynamo-response (aws/invoke dynamo
              {:op :Query
               :request {:TableName table-name
                         :IndexName "entity-id-attribute-index"
                         :KeyConditionExpression "#eidattr = :val"
                         :ExpressionAttributeNames {"#eidattr" "entity-id-attribute"}
                         :ExpressionAttributeValues {":val" {:S (str entity-id "#" attribute)}}}})]
    (format-response dynamo-response)))

(defn fetch-by-entity-id-attribute-value [dynamo table-name entity-id attribute value]
  (let [dynamo-response (aws/invoke dynamo
                                    {:op :Query
                                     :request {:TableName table-name
                                               :IndexName "entity-id-attribute-value-index"
                                               :KeyConditionExpression "#eidattr = :val"
                                               :ExpressionAttributeNames {"#eidattr" "entity-id-attribute-value"}
                                               :ExpressionAttributeValues {":val" {:S (str entity-id "#" attribute "#" value)}}}})
        formatted (format-response dynamo-response)]
    formatted))

