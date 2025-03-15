(ns vol1n.dynalog.pull
    (:require [vol1n.dynalog.utils :as utils]
              [vol1n.dynalog.schema :as schema]))

;; (d/pull db [:person/age {:person/friend [:person/name {:person/friend [:person/name]}]}] entity-id)
(defn pull [conn pattern entity-id]
  (println "entity-id" entity-id)
  (let [{direct-attrs false refs true} (group-by map? pattern)
        ref-keys (mapv (fn [map] (first (first map))) refs)
        keep-facts (set (concat direct-attrs ref-keys))
        entity-facts (utils/fetch-entity (:dynamo conn) (:table-name conn) entity-id)
        fetched-attrs (reduce (fn [acc fact] 
                                (let [attribute (get fact :attribute)
                                      schema @schema/schema-cache
                                      eid-to-ident (into {} (map (fn [[k v]] [v k]) @schema/ident-cache))
                                      cardinality (or (get-in schema [attribute :db/cardinality]) :db.cardinality/one)
                                      is-ref (= (get-in schema [attribute :db/valueType]) :db.type/ref)]
                                (if (keep-facts attribute)
                                  (if (= cardinality :db.cardinality/many)
                                    (update acc attribute (fnil conj #{}) (get fact :value))
                                    (if (and is-ref (not (contains? (mapv first refs) attribute)))
                                      (assoc acc attribute (or (get eid-to-ident (get fact :value)) (get fact :value)))
                                      (assoc acc attribute (get fact :value))))
                                  acc))) {} entity-facts)
        with-id-if-needed (if (contains? keep-facts :db/id) 
                            (assoc fetched-attrs :db/id entity-id) 
                            fetched-attrs)
        ref-resolutions (mapv (fn [map] (let [k (first (first map))
                                              v (last (first map))
                                              schema @schema/schema-cache
                                              cardinality (or (get-in schema [k :db/cardinality]) :db.cardinality/one)]
                                          [k (future (if (= cardinality :db.cardinality/many)
                                                       (set (pmap #(pull conn v %) (k with-id-if-needed)))
                                                      (pull conn v (k with-id-if-needed))))])) refs)]
    (into {} (vec (concat (mapv (fn [attr] [attr (attr with-id-if-needed)]) direct-attrs)
                     (mapv (fn [[k v]] [k @v]) ref-resolutions))))))