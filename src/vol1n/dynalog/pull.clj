(ns vol1n.dynalog.pull
    (:require [vol1n.dynalog.utils :as utils]))

;; (d/pull db [:person/age {:person/friend [:person/name {:person/friend [:person/name]}]}] entity-id)
(defn pull [conn pattern entity-id]
  (let [{direct-attrs false refs true} (group-by map? pattern)
        ref-keys (mapv (fn [map] (first (first map))) refs)
        keep-facts (set (concat direct-attrs ref-keys))
        entity-facts (utils/fetch-entity (:dynamo conn) (:table-name conn) entity-id)
        fetched-attrs (reduce (fn [acc fact]
                                (if (keep-facts (get fact :attribute))
                                  (assoc acc (get fact :attribute) (get fact :value))
                                  acc)) {} entity-facts)
        ref-resolutions (mapv (fn [map] (let [k (first (first map))
                                              v (last (first map))]
                                          [k (future (pull conn v (k fetched-attrs)))])) refs)]
    (into {} (vec (concat (mapv (fn [attr] [attr (attr fetched-attrs)]) direct-attrs)
                     (mapv (fn [[k v]] [k @v]) ref-resolutions))))))