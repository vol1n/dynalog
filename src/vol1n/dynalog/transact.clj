(ns vol1n.dynalog.transact
  (:require [com.grzm.awyeah.client.api :as aws]
            [clojure.core.async :as async]
            [vol1n.dynalog.schema :as schema]
            [vol1n.dynalog.utils :as utils]))

(defn- generate-id []
  (str (random-uuid)))

(defn- generate-tx-id []
  (let [millis (System/currentTimeMillis)
        suffix (subs (str (random-uuid)) 0 8)]
    (str "tx-" millis "-" suffix)))

(defn- is-schema? [entity]
  (contains? entity :db/ident))

(defn uuid-str? [s]
  (if (keyword? s)
    false
    (boolean (re-matches #"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$" s))))

(defn assign-ids [data]
  (let [attributes @schema/schema-cache
        ident-map @schema/ident-cache
        ref-fields (keep (fn [[attribute constraints]] (when (= (:db/valueType constraints) :db.type/ref) attribute)) attributes)
        id-fields (conj ref-fields :db/id)
        temp-id-in-tx? (fn [temp-id]
                         (some #(if (= (:db/id %) temp-id)
                                     true
                                     nil) data))]
    (reduce (fn [acc item]
              (if (not (map? item))
                {:final (conj (:final acc) item) :assigned (:assigned acc)}
              (let [item-id-fields (filter (fn [k] (contains? item k)) id-fields)
                    no-temp-ids (reduce (fn [item-acc k] 
                                          (cond
                                            (and (coll? (k item)) 
                                                 (keyword? (first (k item)))
                                                 (= (:db/unique (get attributes (first (k item)))) :db.unique/identity)) (assoc-in item-acc [:item k] (k item)) ;; lookup ref, because the leading keyword is not an ident
                                            (coll? (k item)) (let [reduced (reduce (fn [coll-acc v]
                                                                                     (cond 
                                                                                       (or (uuid? v) (uuid-str? v)) (if (temp-id-in-tx? v)
                                                                                                                        (-> coll-acc
                                                                                                                          (update :new-list conj v)
                                                                                                                          (update :new-valid-refs conj [k v]))
                                                                                                                      (update coll-acc :new-list conj v))
                                                                                       (keyword? v) (-> coll-acc
                                                                                                        (update :new-list conj (get ident-map v))
                                                                                                        (update :new-valid-refs conj [k (get ident-map v)])) ;; ident ref
                                                                                       (string? v) (if (contains? (:assigned item-acc) v)
                                                                                                     (-> coll-acc
                                                                                                         (update :new-list conj (get (:assigned item-acc) v))
                                                                                                         (update :new-valid-refs conj [k (get (:assigned item-acc) v)]))
                                                                                                     (if (temp-id-in-tx? v)
                                                                                                       (let [new-id (generate-id) 
                                                                                                             assigned-new-id (update coll-acc :new-list #(conj % new-id))
                                                                                                             added-to-assigned-map (assoc-in assigned-new-id [:new-assigned v] new-id)]
                                                                                                         (update added-to-assigned-map :new-valid-refs conj [k new-id]))
                                                                                                       (throw (ex-info "Temp id used as ref but not assigned in tx" {:item item})))))) 
                                                                                   {:new-list [] :new-assigned {} :new-valid-refs []} (k item))
                                                                   with-resolved-list (assoc-in item-acc [:item k] (:new-list reduced))
                                                                   with-new-assignments (update with-resolved-list :assigned merge (:new-assigned reduced))]
                                                               (update-in
                                                                with-new-assignments
                                                                [:item :valid-refs] (fnil concat []) (:new-valid-refs reduced)))
                                            (or (uuid? (k item)) (uuid-str? (k item))) (if 
                                                                                        (temp-id-in-tx? (k item))
                                                                                        (update-in item-acc [:item :valid-refs] conj [k (k item)])
                                                                                        item-acc) ;; UUID -> user is referencing an item in the db  
                                            (keyword? (k item)) (-> item-acc
                                                                    (assoc-in [:item k] (get ident-map (k item)))
                                                                    (update-in [:item :valid-refs] (fnil conj []) [k (get ident-map (k item))]))
                                            (and (string? (k item)) (contains? (:assigned item-acc) (k item))) (cond
                                                                                                                 (= k :db/id) (assoc-in item-acc [:item k] (get (:assigned item-acc) (k item)))
                                                                                                                 :else (update-in
                                                                                                                        (assoc-in item-acc [:item k] (get (:assigned item-acc) (k item)))
                                                                                                                        [:item :valid-refs] (fnil conj []) [k (get (:assigned item-acc) (k item))])) ;; This is a ref, but it must be valid because it has been assigned this tx
                                            (and (not (= k :db/id)) (string? (k item)) (temp-id-in-tx? (k item))) (let [temp-id (k item)
                                                                                                                        new-id (generate-id)
                                                                                                                        assigned-new-id (assoc-in item-acc [:item k] new-id)
                                                                                                                        added-to-assigned-map (assoc-in assigned-new-id [:assigned temp-id] new-id)]
                                                                                                                    (update-in
                                                                                                                     added-to-assigned-map
                                                                                                                     [:item :valid-refs] (fnil conj []) [k new-id]))
                                                      ;; Temp ID that needs to be assigned, generate and keep track of it
                                            (string? (k item))
                                            (let [temp-id (k item)
                                                  new-id (generate-id)]
                                              {:item (assoc (:item item-acc) k new-id) :assigned (assoc (:assigned item-acc) temp-id new-id)})))
                                        {:item item :assigned (:assigned acc)} item-id-fields)
                    ent (:item no-temp-ids)
                    assigned-id-if-omitted (if (not (contains? ent :db/id)) 
                                             (if (contains? ent :db/ident)
                                               (assoc ent :db/id (or (get ident-map (get ent :db/ident)) (generate-id)))
                                               (assoc ent :db/id (generate-id)))
                                             ent)]
                {:final (conj (:final acc) assigned-id-if-omitted) :assigned (:assigned no-temp-ids)})))
            {:final [] :assigned {}} data)))

(defn- query-by-tx-id [conn tx-id]
  (let [response (aws/invoke (:dynamo conn)
                             {:op :Query
                              :request {:TableName (:table-name conn)
                                        :IndexName "tx-id-index" 
                                        :KeyConditionExpression "#txid = :tx"
                                        :ExpressionAttributeNames {"#txid" "tx-id"}
                                        :ExpressionAttributeValues {":tx" {:S (str tx-id)}}}})]
    (:Items response)))

(defn- delete-items! [conn items]
  (let [table-name (:table-name conn)
        delete-requests (map (fn [item]
                               {:DeleteRequest {:Key {"entity-id" {:S (str (get-in item [:entity-id :S]))}
                                                      "attribute-tx" {:S (str (get-in item [:attribute-tx :S]))}}}})
                             items)
        response (aws/invoke (:dynamo conn)
                            {:op :BatchWriteItem
                             :request {:RequestItems {table-name delete-requests}}})]
    response))

(defn- rollback-tx! [conn tx-id]
  (let [items (query-by-tx-id conn tx-id)]
    (doseq [batch (partition-all 25 items)]
      (delete-items! conn batch))))

(defn build-item [fact tx-id]
  (let [item {"entity-id" {:S (str (:entity-id fact))}
              "attribute" {:S (str (:attribute fact))}
              "attribute-tx-value" {:S (str (:attribute fact) "#" tx-id "#" (:value fact))}
              "attribute-value" {:S (str (:attribute fact) "#" (:value fact))}
              "value" {:S (str (:value fact))}
              "tx-id" {:S (str tx-id)}
              "latest" {:N "1"}
              "is-schema" {:N (if (:is-schema fact) "1" "0")}
              "is-ident" {:N (if (= (:attribute fact) :db/ident) "1" "0")}
              "entity-id-attribute" {:S (str (:entity-id fact) "#" (:attribute fact))}
              "entity-id-attribute-value" {:S (str (:entity-id fact) "#" (:attribute fact) "#" (:value fact))}}]
    (if (:is-numerical item)
      (assoc item "value-as-number" {:N (str (:value fact))})
      item)))

(defn with-retries [f initial-items retry-fn max-retries]
  (async/<!!
   (async/go-loop [attempt 1
                   items initial-items]
     (let [result (f items)  ;; Call the function
           dynalog-error (:vol1n.dynalog/error result) 
           aws-error (:vol1n.dynalog.error/aws result)
           transaction-canceled? (= (:vol1n.dynalog.error/code result) "TransactionCanceledException")]  ;; Check for AWS API error
       (if dynalog-error
         (if (and (not transaction-canceled?) aws-error (< attempt max-retries))
           (let [new-items (retry-fn result items)]
             (async/<! (async/timeout (* 50 (Math/pow 2 attempt))))  ;; Exponential backoff
             (recur (inc attempt) new-items))
           result)  ;; Stop retrying <- result is a dynomic error
         result)))))  ;; If no AWS error, return the result

(defn unique-fact? [conn fact] 
  (aws/validate-requests (:dynamo conn) true)
  (let [table-name (:table-name conn)
        dynamo (:dynamo conn)
        response (utils/fetch-by-attribute-value dynamo table-name (:attribute fact) (:value fact)) 
        unique? (empty? response)
        dupe? (= (:entity-id fact) (:entity-id (first response)))]
    (if dupe?
      :duplicate
      unique?)))

(defn valid-ref? [conn fact]
  (aws/validate-requests (:dynamo conn) true)
  (if (:is-valid-ref fact)
    true
    (let [table-name (:table-name conn)
          dynamo (:dynamo conn)
          ref (:value fact)
          response (utils/fetch-entity dynamo table-name ref)
          valid? (seq response)]
      valid?)))

(defn put-batch! [tx-id conn facts]
  (aws/validate-requests (:dynamo conn) true)
  (let [payload {:op :BatchWriteItem
                 :request {:RequestItems
                           {(:table-name conn)
                            (mapv (fn [fact]
                                    (let [item (build-item fact tx-id)]
                                      {:PutRequest {:Item item}})) facts)}}}
        response
        (aws/invoke (:dynamo conn)
                    payload)]
    (if (:cognitect.aws.error/code response)
      {:vol1n.dynalog/error true :vol1n.dynalog.error/aws true :vol1n.dynalog.error/code (:cognitect.aws.error/code response) :vol1n.dynalog.error/message (str (:cognitect.aws.error/code response) response)}
      (if (seq (:UnprocessedItems response))
        {:vol1n.dynalog/error true :vol1n.dynalog.error/aws true :vol1n.dynalog.error/code "ItemsUnprocessed" :vol1n.dynalog.error/items (:UnprocessedItems response)}
        true))))

(defn valid-schema-fact? [conn fact]
  (aws/validate-requests (:dynamo conn) true)
  (let [table-name (:table-name conn)
        dynamo (:dynamo conn)
        entity-id (:entity-id fact)
        response (aws/invoke dynamo
                             {:op :Query
                              :request {:TableName table-name
                                        :IndexName "entity-id-index"
                                        :KeyConditionExpression "#eid = :entityid"
                                        :ExpressionAttributeValues {"entity-id" {:S entity-id}
                                                                    ":ts"  {:S "2024-02-28T12:00:00Z"}}
                                        :ExpressionAttributeNames {"#eid" "entity-id"}}})
        valid? (empty? (:Items response))]
    valid?))

(defn validate-and-put-schema-facts! [tx-id conn facts]
  (let [valid-schema-facts (vec (pmap #(valid-schema-fact? conn %) facts))]
    (if (every? identity valid-schema-facts)
      (let [inserts (pmap
                     (fn [batch]
                       (with-retries #(put-batch! tx-id conn %) batch (fn [result items] (if (= (:vol1n.dynalog.error/code result) "ItemsUnprocessed")
                                                                                            (:vol1n.dynalog.error/items result)
                                                                                            items)) 3))
                     (partition-all 25 facts))]
        (vec inserts))
      {:vol1n.dynalog/error true :vol1n.dynalog.error/aws true :vol1n.dynalog.error/message (str "The following facts are not unique: " (vec (keep-indexed #(when (false? %2) (nth facts %1)) valid-schema-facts)))})))

(defn dissoc-in [m [k & ks]]
  (if ks
    (let [updated (dissoc-in (get m k) ks)]
      (if (empty? updated)
        (dissoc m k)  ;; Remove the key entirely if empty
        (assoc m k updated)))  ;; Otherwise, update with the new value
    (dissoc m k)))  ;; Remove the key when no more keys are left

(defn remove-facts-from-cache! [facts] 
  (swap! utils/cache #(reduce (fn [acc fact]
                                (if (map? fact)
                                  (-> acc
                                      (dissoc-in [:attr-value (str (get-in fact [:attribute :S]) "#" (get-in fact [:value :S]))])
                                      (dissoc-in [:attribute (get-in fact [:attribute :S])])
                                      (dissoc-in [:entity-id-attribute (str (get-in fact [:entity-id :S]) "#" (get-in fact [:attribute :S]))])
                                      (dissoc-in [:entity-id (get-in fact [:entity-id :S])]))
                                  acc)) % (:Items facts))))

(defn retract-entity! [r tx-id conn]
  (let [entity-id (last r)
        facts (aws/invoke (:dynamo conn) {:op :Query
                                          :request {:TableName (:table-name conn)
                                                    :IndexName "entity-id-index"
                                                    :KeyConditionExpression "#eid = :refval"
                                                    :ExpressionAttributeNames {"#eid" "entity-id"}
                                                    :ExpressionAttributeValues {":refval" {:S entity-id}}}})
        updates (pmap #(aws/invoke (:dynamo conn) {:op :UpdateItem
                                                    :request {:TableName (:table-name conn)
                                                              :Key {"entity-id" {:S (str (get-in % [:entity-id :S]))}
                                                                    "attribute-tx-value" {:S (str (get-in % [:attribute-tx-value :S]))}}
                                                              :UpdateExpression "SET #ret = :tx"
                                                              :ExpressionAttributeNames {"#ret" "retracted"}
                                                              :ExpressionAttributeValues {":tx" {:S (str tx-id)}}}}) (:Items facts))]
    (remove-facts-from-cache! facts)
    (vec updates)))

(defn retract-attribute! [r tx-id conn]
  (let [[entity-id attribute] (rest r)
        facts (aws/invoke (:dynamo conn)
                          {:op :Query
                           :request {:TableName (:table-name conn)
                                     :IndexName "entity-id-attribute-index"
                                     :KeyConditionExpression "#eidattr = :val"
                                     :ExpressionAttributeNames {"#eidattr" "entity-id-attribute"}
                                     :ExpressionAttributeValues {":val" {:S (str entity-id "#" attribute)}}}})
        updates (pmap #(let [payload {:op :UpdateItem
                                      :request {:TableName (:table-name conn)
                                                :Key {"entity-id" {:S (str (get-in % [:entity-id :S]))}
                                                      "attribute-tx-value" {:S (str (get-in % [:attribute-tx-value :S]))}}
                                                :UpdateExpression "SET #ret = :tx"
                                                :ExpressionAttributeNames {"#ret" "retracted"}
                                                :ExpressionAttributeValues {":tx" {:S (str tx-id)}}}}]
                         (aws/invoke (:dynamo conn) payload)) (:Items facts))]
    (remove-facts-from-cache! facts)
    (vec updates)))

(defn retract-attribute-value! [r tx-id conn] 
  (let [[entity-id attribute value] (rest r)
        facts (aws/invoke (:dynamo conn)
                          {:op :Query
                           :request {:TableName (:table-name conn)
                                     :IndexName "entity-id-attribute-value-index"
                                     :KeyConditionExpression "#eidattrval = :val"
                                     :ExpressionAttributeNames {"#eidattrval" "entity-id-attribute-value"}
                                     :ExpressionAttributeValues {":val" {:S (str entity-id "#" attribute "#" value)}}}})
        updates (pmap #(let [payload  {:op :UpdateItem
                                      :request {:TableName (:table-name conn)
                                                :Key {"entity-id" {:S (str (get-in % [:entity-id :S]))}
                                                      "attribute-tx-value" {:S (str (get-in % [:attribute-tx-value :S]))}}
                                                :UpdateExpression "SET #ret = :tx"
                                                :ExpressionAttributeNames {"#ret" "retracted"}
                                                :ExpressionAttributeValues {":tx" {:S (str tx-id)}}}}] 
                         (aws/invoke (:dynamo conn) payload)) (:Items facts))] 
    (remove-facts-from-cache! facts)
    (vec updates)))

(defn do-retractions! [to-retract tx-id conn] ;; [:db/retract eid attribute value]
  (let [{entity-retractions 2
         attribute-retractions 3
         attribute-value-retractions 4} (group-by count to-retract)
        retractions (concat (pmap (fn [r] [r (retract-entity! r tx-id conn)]) entity-retractions)
                                    (pmap (fn [r] [r (retract-attribute! r tx-id conn)]) attribute-retractions)
                                    (pmap (fn [r] [r (retract-attribute-value! r tx-id conn)]) attribute-value-retractions))
        failed (filter #(:vol1n.dynalog/error (second %)) retractions)]
    (if (seq failed)
      {:vol1n.dynalog/error true :vol1n.dynalog.error/aws true :vol1n.dynalog.error/code "InternalError" :vol1n.dynalog.error/message "Retraction failed" :vol1n.dynalog.error/failed (mapv first failed)}
      (map second (vec retractions)))))

(defn dynalog-error? [item]
  (:vol1n.dynalog/error item))

(defn put-unique-facts! [tx-id conn facts]
  (let [unique (vec (pmap #(unique-fact? conn %) facts))
        dupes-filtered (filter #(not= (first %) :duplicate) (map vector unique facts))]
    (if (every? identity unique)
      (let [inserts (pmap
                     (fn [batch]
                       (with-retries #(put-batch! tx-id conn %) batch (fn [result items] 
                                                                         (if (= (:vol1n.dynalog.error/code result) "ItemsUnprocessed")
                                                                                      (:vol1n.dynalog.error/items result)
                                                                                      (mapv last dupes-filtered))) 3))
                     (partition-all 25 facts))]
        (vec inserts))
      {:vol1n.dynalog/error true :vol1n.dynalog.error/aws true :vol1n.dynalog.error/message (str "The following facts are not unique: " (vec (keep-indexed #(when (false? %2) (nth facts %1)) unique)))})))

(defn put-ref-facts! [tx-id conn facts]
  (let [valid-refs (vec (pmap #(valid-ref? conn %) facts))]
    (if (every? identity valid-refs)
      (let [inserts (pmap
                     (fn [batch]
                       (with-retries #(put-batch! tx-id conn %) batch (fn [result items] (if (= (:vol1n.dynalog.error/code result) "ItemsUnprocessed")
                                                                                            (:vol1n.dynalog.error/items result)
                                                                                            items)) 3))
                     (partition-all 25 facts))]
        (vec inserts))
      (let [invalid-refs (vec (keep-indexed #(when (not %2) (nth facts %1)) valid-refs))]
        {:vol1n.dynalog/error true :vol1n.dynalog.error/aws true :vol1n.dynalog.error/message (str "The following facts are not valid refs: " invalid-refs)}))))


(defn put-schema-facts! [tx-id conn facts]
  (let [{ident-facts :ident schema-facts :schema} (group-by #(if (= (:attribute %) :db/ident)
                                                               :ident
                                                               :schema) facts)
        ident-inserts (future (put-unique-facts! tx-id conn ident-facts))
        schema-inserts (future (validate-and-put-schema-facts! tx-id conn schema-facts))
        results (concat (deref schema-inserts) (deref ident-inserts))]
    results))

(defn is-unique? [fact]
  (= (:db/unique ((:attribute fact) @schema/schema-cache)) :db.unique/value))

(defn is-ref? [fact]
  (= (:db/valueType ((:attribute fact) @schema/schema-cache)) :db.type/ref))

(defn is-schema-fact? [fact]
  (:is-schema fact))

(defn put-all-facts! [tx-id conn facts] 
  (let [{schema-facts :schema unique-facts :unique facts :normal ref-facts :ref to-retract :retraction} (group-by #(cond
                                                                                                                      (and (vector? %) (or (= (first %) :db/retract) (= (first %) :db/retractEntity))) :retraction
                                                                                                                      (is-schema-fact? %) :schema
                                                                                                                      (is-unique? %) :unique
                                                                                                                      (is-ref? %) :ref  
                                                                                                                      (or (= (first %) :db/retract) (= (first %) :db/retractEntity)) :retraction
                                                                                                                      :else :normal) facts)
        inserts (pmap
                 (fn [batch]
                   (with-retries #(put-batch! tx-id conn %) batch (fn [result items] (if (= (:vol1n.dynalog.error/code result) "ItemsUnprocessed")
                                                                                        (:vol1n.dynalog.error/items result)
                                                                                        items)) 3))
                 (partition-all 25 facts))
        unique-inserts (future (put-unique-facts! tx-id conn unique-facts))
        schema-inserts (future (put-schema-facts! tx-id conn schema-facts))
        ref-inserts (future (put-ref-facts! tx-id conn ref-facts))
        retractions (future (do-retractions! to-retract tx-id conn))
        results (doall (conj (doall inserts) (deref unique-inserts) (deref schema-inserts) (deref ref-inserts) (deref retractions)))]
    (if (some dynalog-error? (flatten results))
      (do
        (rollback-tx! conn tx-id)
        {:vol1n.dynalog/error true :vol1n.dynalog.error/message (str "The following errors occured:" (vec (filter dynalog-error? results)))})
      (do
        (when (not (= (count (deref schema-inserts)) 0))
          (schema/update-schema! conn (:table-name conn))
          (schema/update-ident-cache! conn (:table-name conn)))
        {:items-written (count facts)}))))

(def numerical-types #{:db.type/long
                       :db.type/float
                       :db.type/double 
                       :db.type/instant})

(defn conflicting-ident-fact [conn attribute value]
  (let [conflict (first (utils/fetch-by-attribute-value (:dynamo conn) (:table-name conn) attribute value))]
    conflict))

(defn handle-ident-attributes [conn entities attributes]
  (let [is-ident-attr? #(= :db.unique/identity (:db/unique (% attributes)))
        modified (vec (pmap (fn [ent]
                              (if (map? ent)
                                (if-let [ident-attrs (seq (keep (fn [[k _]] (when (is-ident-attr? k) k)) ent))]
                                  (if-let [new-eid (reduce (fn [matching-eid attr]
                                                             (if-let [conflicting-ent (conflicting-ident-fact conn attr (attr ent))]
                                                               (if (or (nil? matching-eid) (= matching-eid (:entity-id conflicting-ent)))
                                                                 (:entity-id conflicting-ent)
                                                                 (throw (ex-info "Conflicting ident facts" {:conflicting-eid (:entity-id conflicting-ent) :matching-eid matching-eid :attr attr :value (attr ent)})))
                                                               matching-eid)) nil ident-attrs)]
                                    [(assoc ent :db/id new-eid) {:old (:entity-id ent) :new new-eid}]
                                    [ent {}])
                                  [ent {}])
                                [ent {}]))
                            entities))
        replacements (reduce #(merge %1 (second %2)) {} modified) ;; These are the entity ids that are being merged
        replaced (map (fn [[ent _]]
                        (if (map? ent)
                        (reduce-kv (fn [acc attribute value] 
                                     (if (contains? replacements value)
                                       (assoc acc attribute (get replacements value))
                                       (assoc acc attribute value))) {} ent)
                          ent)) modified)]
    replaced))

(defn retract-previous [conn fact]
  (if-let [old-fact (first (utils/fetch-by-entity-id-attribute (:dynamo conn) (:table-name conn) (:entity-id fact) (:attribute fact)))]
    (if (= (:value old-fact) (:value fact))
      ::dupe
      [:db/retract (:entity-id old-fact) (:attribute fact) (:value old-fact)])
    nil))

(defn retract-cardinality-one-violations! [tx-id conn facts attributes]
  (let [[retractions facts]  (apply map vector (vec (pmap (fn [fact]
                                  (if (map? fact)
                                    (if-not (:is-schema fact)
                                      (let [attribute (:attribute fact)
                                            constraints (attribute attributes)
                                            cardinality (or (:db/cardinality constraints) :db.cardinality/one)]
                                        (if (= cardinality :db.cardinality/one) 
                                          (let [new-retraction (retract-previous conn fact)]
                                            (if (= new-retraction ::dupe)
                                              [nil nil]
                                              [new-retraction fact]))
                                          [nil fact]))
                                      [nil fact])
                                    [nil fact]))
                                facts)))]
    (do-retractions! (keep identity retractions) tx-id conn)
    (keep identity facts)))

(defn find-lookup-ref [conn attribute value]
  (let [fetch-result (utils/fetch-by-attribute-value (:dynamo conn) (:table-name conn) attribute value)]
    (first fetch-result)))

(defn entities-to-facts [tx-id conn entities]
  (let [attributes @schema/schema-cache
        ident-attributes-handled (handle-ident-attributes conn entities attributes)
        facts (vec (mapcat
                    (fn [ent]
                      (if (map? ent)
                        (reduce-kv (fn [facts attribute value] 
                                     (cond ;; filter db/id as it is implicit in all entities
                                       (= attribute :valid-refs) facts
                                       (= attribute :db/id) facts
                                       (coll? value) (if (= (:db/cardinality (attribute attributes)) :db.cardinality/many) ;; collection of values for a many attribute, simple just add more facts
                                                       (concat facts (reduce (fn [acc val]
                                                                               (conj acc {:entity-id (:db/id ent)
                                                                                          :attribute attribute
                                                                                          :value val
                                                                                          :is-schema (is-schema? ent)
                                                                                          :is-valid-ref (boolean ((set (:valid-refs ent)) [attribute val]))
                                                                                          :as-number (when (numerical-types (:db/valueType (attribute attributes)))
                                                                                                       (cond
                                                                                                         (= attribute :db.type/instant) (.getTime val)
                                                                                                         :else val))})) [] value))
                                                       (if (and (keyword? (first value))
                                                                (= (:db/valueType (attribute attributes)) :db.type/ref) ;; This must be a lookup ref or is invalid
                                                                (= (count value) 2) ;; lookup refs have 2 items in them
                                                                (= (:db/unique ((first value) attributes)) :db.unique/identity)) ;; The attribute being used to reference MUST be :db.unique/identity
                                                         (if-let [matching (find-lookup-ref conn (first value) (last value))]
                                                           (conj facts {:entity-id (:db/id ent)
                                                                        :attribute attribute
                                                                        :value (:entity-id matching)
                                                                        :is-schema (is-schema? ent)
                                                                        :is-valid-ref true
                                                                        :as-number (when (numerical-types (:db/valueType (attribute attributes)))
                                                                                     (cond
                                                                                       (= attribute :db.type/instant) (.getTime (first value))
                                                                                       :else (first value)))})
                                                           (throw (ex-info "Invalid lookup ref" {:value value})))
                                                         (throw (ex-info "Invalid coll in tx-data" {:value value}))
                                                         ))
                                       :else (conj facts {:entity-id (:db/id ent)
                                                          :attribute attribute
                                                          :value value
                                                          :is-schema (is-schema? ent)
                                                          :is-valid-ref (boolean ((set (:valid-refs ent)) [attribute value]));;(boolean (some #{attribute} (:valid-refs ent)))
                                                          :as-number (when (numerical-types (:db/valueType (attribute attributes)))
                                                                       (cond
                                                                         (= attribute :db.type/instant) (.getTime value)
                                                                         :else value))})))
                                   [] ent)
                        [ent])) ident-attributes-handled))]
        (retract-cardinality-one-violations! tx-id conn facts attributes)))

(defn deduplicate-tx-data [tx-data]
  (let [unique-facts (into #{} (map #(select-keys % [:db/id :attribute :value]) tx-data))]
    (filter #(contains? unique-facts (select-keys % [:db/id :attribute :value])) tx-data)))  

(defn duplicate-schema-ent? [conn new-entity existing]
  (let [ent-facts (utils/fetch-entity (:dynamo conn) (:table-name conn) (:entity-id existing))
        full-ent-schema (into {} (map (fn [fact] [(:attribute fact) (:value fact)]) ent-facts))
        compared-ent (reduce-kv (fn [mismatched k v]
                                  (if (not (= (get full-ent-schema k) v))
                                    (conj mismatched k)
                                    mismatched)) #{} new-entity)
        compared-both (reduce-kv (fn [mismatched k v]
                                   (if (not (= (get new-entity k) v))
                                     (conj mismatched k)
                                     mismatched)) compared-ent full-ent-schema)]

    (if (empty? compared-both)
      true
      false)))

(defn existing-schema-ent [conn entity]
  (aws/validate-requests (:dynamo conn))
  (let [existing (utils/fetch-by-attribute-value (:dynamo conn) (:table-name conn) :db/ident (:db/ident entity))]
    (if (seq existing) 
      (first existing)
      nil)))

(defn validate-schema-entities [conn entities] 
  (let [{schema-entities true other false} (group-by is-schema? entities)
        new-list (reduce (fn [new ent]
                                   (if-let [existing (existing-schema-ent conn ent)]
                                     (if (duplicate-schema-ent? conn ent existing)
                                       new
                                       (conj new :invalid))
                                     (conj new ent))) [] schema-entities)]
    (if (contains? new-list :invalid)
      nil
      (concat schema-entities other))))

(defn transact [conn tx]
  (let [tx-id (generate-tx-id)
        deduped (deduplicate-tx-data (:tx-data tx))
        cleaned (validate-schema-entities conn deduped)]
    (if (nil? cleaned)
      {:vol1n.dynalog/error true :vol1n.dynalog.error/message (str "Transaction data is invalid: " "schema facts are idempotent")}
      (let [{data-with-ids :final assigned :assigned} (assign-ids cleaned)
            invalid-entities (schema/validate data-with-ids)
            facts (entities-to-facts tx-id conn data-with-ids)]
        (if (seq invalid-entities)
          {:vol1n.dynalog/error true :vol1n.dynalog.error/message (str "Transaction data is invalid: " (vec invalid-entities))}
          (let [put-result (put-all-facts! tx-id conn facts)]
            (if (dynalog-error? put-result)
                put-result
              (do
                (swap! utils/cache #(reduce (fn [acc fact]
                                              (if (map? fact)
                                                (-> acc
                                                    (dissoc-in [:attr-value (str (:attribute fact) "#" (:value fact))])
                                                    (dissoc-in [:attribute (:attribute fact)])
                                                    (dissoc-in [:entity-id-attribute (str (:entity-id fact) "#" (:attribute fact))])
                                                    (dissoc-in [:entity-id (:entity-id fact)]))
                                                acc)) % facts))
                (assoc put-result :tempids assigned :success true :tx-id tx-id)))))))))