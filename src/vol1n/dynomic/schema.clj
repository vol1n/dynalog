(ns vol1n.dynomic.schema
    (:require [com.grzm.awyeah.client.api :as aws]))

(def schema-cache (atom {}))

(def reserved-attributes
  {:db/valueType   #{:db.type/keyword :db.type/string :db.type/boolean :db.type/long :db.type/instant :db.type/ref}
   :db/cardinality #{:db.cardinality/one :db.cardinality/many}
   :db/unique      #{:db.unique/identity :db.unique/value}
   :db/ident       keyword?
   :valid-refs     coll?
   :db/id          #(or (string? %) (uuid? %))})

(def value-type-predicates
  {:db.type/keyword keyword?
   :db.type/string string?
   :db.type/boolean boolean?
   :db.type/long #(instance? Long %)
   :db.type/instant inst?
   :db.type/ref string?})

(defn keywordize-leading-colon [s]
  (keyword (subs s 1)))

(defn parse-schema [schema-facts]
  (let [eid-to-constraints (reduce (fn [acc item]
                                     (update acc (get-in item [:entity-id :S]) #(assoc % (keywordize-leading-colon (get-in item [:attribute :S])) (keywordize-leading-colon (get-in item [:value :S]))))) {} schema-facts)
        ident-to-constraints (reduce (fn [acc [_ v]]
                                      (assoc acc (:db/ident v) (dissoc v :db/ident))) {} (seq eid-to-constraints))]
    ident-to-constraints))

(defn get-schema [client table-name]
  (let [response
        (aws/invoke (:dynamo client)
                    {:op :Query
                     :request {:TableName table-name
                               :IndexName "schema-index" ;; Your new GSI
                               :KeyConditionExpression "#isschema = :trueval"
                       :ExpressionAttributeNames {"#isschema" "is-schema"}
                       :ExpressionAttributeValues {":trueval" {:N "1"}}}})]
    (parse-schema (:Items response))))

(defn update-schema! [client table-name]
  (reset! schema-cache (get-schema client table-name)))

(defn- valid-value? [value constraints]
  (((:db/valueType constraints) value-type-predicates) value))

(defn- valid-attribute? [attr value] 
  (let [attributes @schema-cache
        is-valid? (cond 
                    (contains? reserved-attributes attr) ((attr reserved-attributes) value)
                    (contains? attributes attr) (valid-value? value (attr attributes))
                    :else false)]
    is-valid?))

(defn valid-entity? [entity] 
  (if (map? entity)
    (every? (fn [[k v]] (valid-attribute? k v)) entity)
    true))

(defn validate [tx-data]
  (remove #(valid-entity? %) tx-data))
