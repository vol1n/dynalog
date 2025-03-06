(ns vol1n.dynomic.pull
    (:require [vol1n.dynomic.utils :as utils]))

;; (d/pull db [:person/age {:person/friend [:person/name {:person/friend [:person/name]}]}] entity-id)
(defn pull [conn pattern entity-id]
  (println "pull called" pattern)
  (println (map? (first pattern)))
  (let [{direct-attrs false refs true} (group-by map? pattern)
        ref-keys (mapv (fn [map] (first (first map))) refs)
        keep-facts (set (concat direct-attrs ref-keys))
        entity-facts (:Items (utils/fetch-entity (:dynamo conn) (:table-name conn) entity-id))
        fetched-attrs (reduce (fn [acc fact]
                                (println "fact" fact) 
                                (println keep-facts)
                                (if (keep-facts (utils/keywordize-leading-colon (get-in fact [:attribute :S])))
                                  (assoc acc (utils/keywordize-leading-colon (get-in fact [:attribute :S])) (get-in fact [:value :S]))
                                  acc)) {} entity-facts)
        ref-resolutions (mapv (fn [map] (let [k (first (first map))
                                              v (last (first map))]
                                          [k (future (pull conn v (k fetched-attrs)))])) refs)]
    (println "keep-facts" keep-facts)
    (println "entity-facts" entity-facts)
    (println "fetched-attrs" fetched-attrs) 
    (println "ref-resolutions" ref-resolutions)
    (println "direct " direct-attrs)
    (into {} (vec (concat (mapv (fn [attr] [attr (attr fetched-attrs)]) direct-attrs)
                     (mapv (fn [[k v]] [k @v]) ref-resolutions))))))