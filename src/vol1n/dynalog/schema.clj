(ns vol1n.dynalog.schema
    (:require [com.grzm.awyeah.client.api :as aws]))

(def schema-cache (atom {}))
(def ident-cache (atom {}))

(def reserved-attributes
  {:db/valueType   #{:db.type/keyword :db.type/string :db.type/boolean :db.type/long :db.type/instant :db.type/ref :db.type/float}
   :db/cardinality #{:db.cardinality/one :db.cardinality/many}
   :db/unique      #{:db.unique/identity :db.unique/value}
   :db/ident       keyword?
   :valid-refs     coll?
   :db/id          #(or (string? %) (uuid? %))
   :db/doc         string?})

(defn float? [x]
  (or (instance? Double x)   ;; Standard float type in Clojure
      (instance? Float x)))

(defn long? [x]
  (or (instance? Long x)
      (instance? Integer x)))

(def value-type-predicates 
  {:db.type/keyword #(or (keyword? %) (and (coll? %) (every? keyword? %)))
   :db.type/string #(or (string? %) (and (coll? %) (every? string? %)))
   :db.type/boolean #(or (boolean? %) (and (coll? %) (every? boolean? %)))
   :db.type/long #(or (long? %) (and (coll? %) (every? (fn [long] (long? long)) %)))
   :db.type/float #(or (float? %) (and (coll? %) (every? float? %)))
   :db.type/instant #(or (inst? %) (and (coll? %) (every? inst? %)))
   :db.type/ref #(or (string? %) (uuid? %) (and (coll? %) (= (count %) 2) (keyword? (first %))) (and (coll? %) (every? (fn [ref] (or (string? ref) (uuid? ref))) %)))})

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

(defn get-ident-facts [client table-name]
  (let [response (aws/invoke (:dynamo client)
                             {:op :Query
                              :request {:TableName table-name
                                        :IndexName "is-ident-index"
                                        :KeyConditionExpression "#isident = :trueval"
                                        :ExpressionAttributeNames {"#isident" "is-ident"}
                                        :ExpressionAttributeValues {":trueval" {:N "1"}}}})]
    (into {} (mapv (fn [%] [(keywordize-leading-colon (get-in % [:value :S])) (get-in % [:entity-id :S])]) (:Items response)))))

(defn update-schema! [client table-name]
  (reset! schema-cache (get-schema client table-name)))

(defn update-ident-cache! [client table-name]
  (reset! ident-cache (get-ident-facts client table-name)))

(defn- valid-value? [value constraints] 
  (if-let [value-type (:db/valueType constraints)]
    ((value-type value-type-predicates) value)
    false))

(defn- valid-attribute? [attr value] 
  
  (let [attributes @schema-cache
        is-valid? (cond 
                    (contains? reserved-attributes attr) ((attr reserved-attributes) value)
                    (contains? attributes attr) (valid-value? value (attr attributes))
                    :else false)]
    is-valid?))

(defn valid-entity? [entity] 
  (let [is-valid (if (map? entity)
                 (every? (fn [[k v]] (valid-attribute? k v)) entity)
                 true)]
  is-valid))
  

(defn validate [tx-data]
  (let [result (remove #(valid-entity? %) tx-data)]
    result))
