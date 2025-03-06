(ns vol1n.dynomic.transact
  (:require [com.grzm.awyeah.client.api :as aws]
            [clojure.core.async :as async]
            [vol1n.dynomic.schema :as schema]
            [vol1n.dynomic.utils :as utils]))

(defn- generate-id []
  (str (random-uuid)))

(defn- generate-tx-id []
  (let [millis (System/currentTimeMillis)
        suffix (subs (str (random-uuid)) 0 8)]
    (str "tx-" millis "-" suffix)))

(defn- is-schema? [entity]
  (contains? entity :db/ident))

(defn assign-ids [data]
  (let [attributes @schema/schema-cache
        ref-fields (keep (fn [[attribute constraints]] (when (= (:db/valueType constraints) :db.type/ref) attribute)) attributes)
        id-fields (conj ref-fields :db/id)
        temp-id-in-tx? (fn [temp-id] (some? (mapv #(if (= (:db/id %) temp-id) true nil) data)))]
    (reduce (fn [acc item]
              (let [item-id-fields (filter (fn [k] (contains? item k)) id-fields)
                    no-temp-ids (reduce (fn [item-acc k]
                                          (cond
                                            (uuid? (k item)) item-acc ;; UUID -> user is referencing an item in the db
                                                      ;; Temp ID that has already been assigned
                                            (and (string? (k item)) (contains? (:assigned item-acc) (k item))) (cond
                                                                                                                 (= k :db/id) (assoc-in item-acc [:item k] (get (:assigned item-acc) (k item)))
                                                                                                                 :else (update-in
                                                                                                                        (assoc-in item-acc [:item k] (get (:assigned item-acc) (k item)))
                                                                                                                        [:item :valid-refs] (fnil conj []) k)) ;; This is a ref, but it must be valid because it has been assigned this tx
                                            (and (not (= k :db/id)) (string? (k item)) (temp-id-in-tx? (k item))) (let [temp-id (k item)
                                                                                                                        new-id (generate-id)
                                                                                                                        assigned-new-id (assoc-in item-acc [:item k] new-id)
                                                                                                                        added-to-assigned-map (assoc-in assigned-new-id [:assigned temp-id] new-id)]
                                                                                                                    (update-in
                                                                                                                     added-to-assigned-map
                                                                                                                     [:item :valid-refs] (fnil conj []) k))
                                                      ;; Temp ID that needs to be assigned, generate and keep track of it
                                            (string? (k item))
                                            (let [temp-id (k item)
                                                  new-id (generate-id)]
                                              {:item (assoc (:item item-acc) k new-id) :assigned (assoc (:assigned item-acc) temp-id new-id)})))
                                        {:item item :assigned (:assigned acc)} item-id-fields)]
                {:final (conj (:final acc) (:item no-temp-ids)) :assigned (:assigned no-temp-ids)}))
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
           dynomic-error (:vol1n.dynomic/error result) 
           aws-error (:vol1n.dynomic.error/aws result)
           transaction-canceled? (= (:vol1n.dynomic.error/code result) "TransactionCanceledException")]  ;; Check for AWS API error
       (if dynomic-error
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
        unique? (empty? (:Items response))]
    unique?))

(defn valid-ref? [conn fact]
  (aws/validate-requests (:dynamo conn) true)
  (if (:is-valid-ref fact)
    true
    (let [table-name (:table-name conn)
          dynamo (:dynamo conn)
          ref (:value fact)
          response (utils/fetch-entity dynamo table-name ref)
          valid? (seq (:Items response))]
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
      {:vol1n.dynomic/error true :vol1n.dynomic.error/aws true :vol1n.dynomic.error/code (:cognitect.aws.error/code response) :vol1n.dynomic.error/message (str (:cognitect.aws.error/code response) response)}
      (if (seq (:UnprocessedItems response))
        {:vol1n.dynomic/error true :vol1n.dynomic.error/aws true :vol1n.dynomic.error/code "ItemsUnprocessed" :vol1n.dynomic.error/items (:UnprocessedItems response)}
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
                       (with-retries #(put-batch! tx-id conn %) batch #(fn [result items] (if (= (:vol1n.dynomic.error/code result) "ItemsUnprocessed")
                                                                                            (:vol1n.dynomic.error/items result)
                                                                                            items)) 3))
                     (partition-all 25 facts))]
        inserts)
      {:vol1n.dynomic/error true :vol1n.dynomic.error/aws true :vol1n.dynomic.error/message (str "The following facts are not unique: " (vec (keep-indexed #(when (false? %2) (nth facts %1)) valid-schema-facts)))})))

(defn dissoc-in [m [k & ks]]
  (println "dissoc-in" m k ks)
  (if ks
    (update m k #(let [res (dissoc-in % ks)]
                   (if (empty? res) nil res)))
    (dissoc m k)))

(defn remove-facts-from-cache! [facts] 
  (swap! utils/cache #(reduce (fn [acc fact]
                                (println "fact" fact)
                                (println "removing attr-val" (str (get-in fact [:attribute :S]) "#" (get-in fact [:value :S])))
                                (println "removing attribute" (get-in fact [:attribute :S]))
                                (println "removing entity-id-attribute" (str (get-in fact [:entity-id :S]) "#" (get-in fact [:attribute :S])))
                                (println "removing entity-id" (get-in fact [:entity-id :S]))
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
    updates))

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
                         (println "payload" payload)
                         (println %)
                         (aws/invoke (:dynamo conn) payload)) (:Items facts))]
    (println "facts" facts)
    (println "updates" updates)
    (remove-facts-from-cache! facts)
    updates))

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
                         (println "fax" facts)
                         (println "payload" payload)
                         (println %)
                         (aws/invoke (:dynamo conn) payload)) (:Items facts))] 
        (println "facts" facts)
    (println "updates" updates "setadpu")
    (remove-facts-from-cache! facts)
    updates))

(defn do-retractions! [to-retract tx-id conn] ;; [:db/retract eid attribute value]
  (let [{entity-retractions 2 
         attribute-retractions 3
         attribute-value-retractions 4} (group-by count to-retract)
        retractions (concat (pmap (fn [r] [r (retract-entity! r tx-id conn)]) entity-retractions)
                                    (pmap (fn [r] [r (retract-attribute! r tx-id conn)]) attribute-retractions)
                                    (pmap (fn [r] [r (retract-attribute-value! r tx-id conn)]) attribute-value-retractions))
        failed (filter #(:vol1n.dynomic/error (second %)) retractions)]
    (if (seq failed)
      {:vol1n.dynomic/error true :vol1n.dynomic.error/aws true :vol1n.dynomic.error/code "InternalError" :vol1n.dynomic.error/message "Retraction failed" :vol1n.dynomic.error/failed (mapv first failed)}
      (map second retractions))))

(defn dynomic-error? [item]
  (:vol1n.dynomic/error item))

(defn put-unique-facts! [tx-id conn facts]
  (let [unique (vec (pmap #(unique-fact? conn %) facts))]
    (if (every? identity unique)
      (let [inserts (pmap
                     (fn [batch]
                       (with-retries #(put-batch! tx-id conn %) batch #(fn [result items] (if (= (:vol1n.dynomic.error/code result) "ItemsUnprocessed")
                                                                                      (:vol1n.dynomic.error/items result)
                                                                                      items)) 3))
                     (partition-all 25 facts))]
        inserts)
      {:vol1n.dynomic/error true :vol1n.dynomic.error/aws true :vol1n.dynomic.error/message (str "The following facts are not unique: " (vec (keep-indexed #(when (false? %2) (nth facts %1)) unique)))})))

(defn put-ref-facts! [tx-id conn facts]
  (let [valid-refs (vec (pmap #(valid-ref? conn %) facts))]
    (if (every? identity valid-refs)
      (let [inserts (pmap
                     (fn [batch]
                       (with-retries #(put-batch! tx-id conn %) batch #(fn [result items] (if (= (:vol1n.dynomic.error/code result) "ItemsUnprocessed")
                                                                                            (:vol1n.dynomic.error/items result)
                                                                                            items)) 3))
                     (partition-all 25 facts))]
        inserts)
      (let [invalid-refs (vec (keep-indexed #(when (nil? %2) (nth facts %1)) valid-refs))]
        {:vol1n.dynomic/error true :vol1n.dynomic.error/aws true :vol1n.dynomic.error/message (str "The following facts are not valid refs: " (vec (keep-indexed #(when (false? %2) (nth facts %1)) valid-refs)))}))))

(defn put-schema-facts! [tx-id conn facts]
  (let [{ident-facts :ident schema-facts :schema} (group-by #(if (= (:attribute %) :db/ident)
                                                               :ident
                                                               :schema) facts)
        ident-inserts (future (put-unique-facts! tx-id conn ident-facts))
        schema-inserts (pmap
                        (fn [batch]
                          (with-retries #(validate-and-put-schema-facts! tx-id conn batch) batch (fn [_ items] items) 3))
                        (partition-all 25 schema-facts))
        results (doall (conj (doall schema-inserts) (deref ident-inserts)))]
    results))


(defn is-unique? [fact]
  (= (:db/unique ((:attribute fact) @schema/schema-cache)) :db.unique/value))

(defn is-ref? [fact]
  (= (:db/valueType ((:attribute fact) @schema/schema-cache)) :db.type/ref))


(defn put-all-facts! [tx-id conn facts]
  
  (let [{schema-facts :schema unique-facts :unique facts :normal ref-facts :ref to-retract :retraction} (group-by #(cond
                                                                                                                      (and (vector? %) (or (= (first %) :db/retract) (= (first %) :db/retractEntity))) :retraction
                                                                                                                      (is-schema? %) :schema
                                                                                                                      (is-unique? %) :unique
                                                                                                                      (is-ref? %) :ref  
                                                                                                                      (or (= (first %) :db/retract) (= (first %) :db/retractEntity)) :retraction
                                                                                                                      :else :normal) facts)
        inserts (pmap
                 (fn [batch]
                   (with-retries #(put-batch! tx-id conn %) batch #(fn [result items] (if (= (:vol1n.dynomic.error/code result) "ItemsUnprocessed")
                                                                                        (:vol1n.dynomic.error/items result)
                                                                                        items)) 3))
                 (partition-all 25 facts))
        unique-inserts (future (put-unique-facts! tx-id conn unique-facts))
        schema-inserts (future (put-schema-facts! tx-id conn schema-facts))
        ref-inserts (future (put-ref-facts! tx-id conn ref-facts))
        retractions (future (do-retractions! to-retract tx-id conn))
        results (doall (conj (doall inserts) (deref unique-inserts) (deref schema-inserts) (deref ref-inserts) (deref retractions)))]
    (if (some dynomic-error? results)
      (do
        (rollback-tx! conn tx-id)
        {:vol1n.dynomic/error true :vol1n.dynomic.error/message (str "The following facts failed to insert: " (vec (keep-indexed #(when (dynomic-error? %2) (nth facts %1)) results)))})
      (do
        (when (not (= (count (deref schema-inserts)) 0))
          (schema/update-schema! conn (:table-name conn)))
        {:items-written (count facts)}))))

(def numerical-types #{:db.type/long
                       :db.type/float
                       :db.type/double
                       :db.type/instant})

(defn entities-to-facts [entities]
  (let [attributes @schema/schema-cache]
    (println entities)
    (vec (mapcat
     (fn [ent]
       (println "ent" ent)
       (println (map? ent))
       (if (map? ent) 
        (keep (fn [[k v]]
                (cond ;; filter db/id as it is implicit in all entities
                  (= k :valid-refs) nil
                  (= k :db/id) nil
                  :else {:entity-id (:db/id ent)
                         :attribute k
                         :value v
                         :is-schema (is-schema? ent)
                         :is-valid-ref (boolean (some #{k} (:valid-refs ent)))
                         :as-number (when (numerical-types (:db/valueType (k attributes)))
                                      (cond 
                                        (= k :db.type/instant) (.getTime v)
                                        :else v))}))
              (seq ent))
        [ent])) entities))))

(defn deduplicate-tx-data [tx-data]
  (let [unique-facts (into #{} (map #(select-keys % [:db/id :attribute :value]) tx-data))]
    (filter #(contains? unique-facts (select-keys % [:db/id :attribute :value])) tx-data)))  

(defn transact [conn tx]
  (let [tx-id (generate-tx-id)
        deduped (deduplicate-tx-data (:tx-data tx))
        {data-with-ids :final assigned :assigned} (assign-ids deduped)
        invalid-entities (schema/validate data-with-ids)
        facts (entities-to-facts data-with-ids)]
    (if (seq invalid-entities)
      {:vol1n.dynomic/error true :vol1n.dynomic.error/message (str "Transaction data is invalid: " (vec (keep-indexed #(when (nil? %2) (nth data-with-ids %1)) data-with-ids)))}
      (let [put-result (put-all-facts! tx-id conn facts)] 
        (if (dynomic-error? put-result)
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
          (assoc put-result :tempids assigned :success true :tx-id tx-id)))))))