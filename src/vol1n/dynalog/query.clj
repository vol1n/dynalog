(ns vol1n.dynalog.query
   (:require [vol1n.dynalog.utils :as utils]
             [clojure.set :as set]
             [vol1n.dynalog.pull :as p]))

(defn parse-query [query]
  (let [sections (partition-by keyword? query)
        section-map (into {} (map (fn [[k & v]] [(first k) (first v)]) (partition 2 sections)))]
    section-map))


(defn replace-variables [known-variables condition]
  (mapv #(if (and (symbol? %) (contains? known-variables %))
           (% known-variables)
           %) condition))

(defn replace-all-variables [known-variables conditions]
  (mapv #(replace-variables known-variables %) conditions))
 
 
(defn get-by-attribute [attribute conn]
  (let [cache-hit (get-in @utils/cache [:attribute attribute])
        result (if (nil? cache-hit)
                 (utils/fetch-by-attribute (:dynamo conn) (:table-name conn) attribute)
                 cache-hit)
        attribute-value-cache-updates (reduce (fn [acc fact] (update acc (str attribute "#" (:value fact)) (fnil conj #{}) fact)) {} result)]
    (swap! utils/cache #(assoc-in % [:attribute attribute] result)) 
    (swap! utils/cache #(reduce (fn [acc [k v]] (update acc k (fnil conj #{}) (set v))) % attribute-value-cache-updates))
    result)) 

(defn get-by-entity-id-attribute [attribute entity-id conn]
  (let [cache-hit (get-in @utils/cache [:entity-id-attribute (str entity-id "#" attribute)])
        result (if (nil? cache-hit)
                 (utils/fetch-by-entity-id-attribute (:dynamo conn) (:table-name conn) entity-id attribute)
                 cache-hit)
        attribute-value-cache-updates (reduce (fn [acc fact] (update acc (str attribute "#" (:value fact)) (fnil conj #{}) fact)) {} result)]
    (swap! utils/cache #(assoc-in % [:entity-id-attribute (str entity-id "#" attribute)] result)) 
    (swap! utils/cache #(reduce (fn [acc [k v]] (update acc k (fnil conj #{}) (set v))) % attribute-value-cache-updates))
    result))

(defn get-by-attrval [attribute value conn]
  (let [cache-hit (get-in @utils/cache [:attr-value (str attribute "#" value)]) 
        result (if (nil? cache-hit)
                 (utils/fetch-by-attribute-value (:dynamo conn) (:table-name conn) attribute value)
                 cache-hit)] 
    (swap! utils/cache #(assoc-in % [:attr-value (str attribute "#" value)] result))
    result))
 
(defn lookup [bind-to attribute ents conn] ;; items can be a map or a set or nil
  (let [ents-set (if (map? ents)
                   (set (mapcat identity (vals ents)))
                   ents)
        result (if ents-set
                 (apply concat (pmap #(get-by-entity-id-attribute attribute % conn) ents-set))
                 (get-by-attribute attribute conn))
        parsed (reduce (fn [acc item] 
                           (update acc {bind-to (:entity-id item)} (fnil conj #{}) (:value item))) {} result)]
    parsed)) 

(defn lookup-eids [attribute values conn] 
  (let [result (if (nil? values)
                 (get-by-attribute attribute conn)
                 (apply concat (pmap #(get-by-attrval attribute % conn) (cond 
                                                                          (and (sequential? values) (sequential? (first values))) (set (mapcat identity values))
                                                                          (sequential? values) (set values)
                                                                          (map? values) (set (mapcat identity (vals values)))
                                                                          :else [values]))))
        eids (reduce (fn [acc item] (conj acc (:entity-id item))) #{} result)] 
    eids))

(defn inverse-map [m]
  (if (set? m)
    m
    (let [inverse (reduce-kv (fn [acc k values] (reduce (fn [acc2 v] (update acc2 v (fnil conj #{}) k)) acc values)) {} m)]
      inverse)))

(defn merge-constraints [m1 m2]
  (if (nil? m2)
    m1
    (if (and (set? m1) (set? m2))
      (set/intersection m1 m2)
      (if (and (map? m1) (map? m2))
        (let [im1 (inverse-map m1)
              im2 (inverse-map m2)
              values (set/intersection (set (keys im1)) (set (keys im2)))]
          (reduce (fn [acc v]
                    (merge acc
                           (reduce (fn [update-map new-k]
                                     (update update-map new-k (fnil conj #{}) v))
                                   {}
                                   (for [x (get im1 v)
                                         y (get im2 v)]
                                     (merge x y)))))
                  {}
                  values))
        (let [m (if (map? m1) m1 m2)
              s (if (set? m1) m1 m2)
              im (inverse-map m)
              values (set/intersection (set (keys im)) s)]
          (reduce (fn [acc v]
                    (reduce (fn [acc2 k]
                              (update acc2 k (fnil conj #{}) v))
                            acc
                            (get im v)))
                  {}
                  values))))))

(defn resolve-all [bindings resolvers queue] 
  (loop [b bindings
         q queue]
    (let [resolved ((get resolvers (first q)) b)
          new-bindings (assoc b (first q) resolved)] 
      (cond 
        (and (= new-bindings b) (seq (rest q))) (recur new-bindings (rest q))
        (= new-bindings b) new-bindings
        :else (recur new-bindings queue)))))

(defn new-binding-iterative [attribute bind-to current-resolver conn]
  (fn [bindings]
    (let [resolved-bind-to (if (symbol? bind-to) (get bindings bind-to) bind-to)
          new-mapping (lookup bind-to attribute resolved-bind-to conn)
          old (if current-resolver (current-resolver bindings) nil)
          merged (merge-constraints new-mapping old)]
      merged)))

(defn new-constraint-iterative [attribute constrained-by current-resolver conn]
  (fn
    [bindings]
    (let [constrained-by-val (if (symbol? constrained-by) (get bindings constrained-by) constrained-by)
          new-set (lookup-eids attribute constrained-by-val conn) ;; constrained-by resolves to a map or set or nothing
          old (if current-resolver (current-resolver bindings) nil)
          merged (merge-constraints new-set old)]
      merged)))

(defn to-named-tuples [var values]
  (if (set? values)
    (set (map (fn [v] {var v}) values))
    (set (mapcat (fn [[k v]]
                   (if (set? v)
                     (map (fn [x] (assoc k var x)) v)
                     [(assoc k var v)]))
                 values))))

(defn join-no-extra-cartesians [sets]
  (let [keys-match? (fn [group set] (some #(contains? (first group) %) (keys (first set))))
        new (reduce (fn [acc set]
                      (let [group-match (first (keep-indexed (fn [idx group] (when (keys-match? group set) [idx group])) acc))]
                        (if group-match
                          (update acc (first group-match) #(set/join % set))
                          (conj acc set)))) [] sets)]
    (cond
      (= (count new) 1) (first new)
      (= (count sets) (count new)) (reduce set/join sets)
      :else (recur new))))

(defn get-fn-deps [fn-clause]
  (set (filter #(and (symbol? %) (not (resolve %))) (flatten fn-clause))))

(defn to-fn [call]
  (let [replace-symbols (fn replace-symbols [expr in] 
                          (let [resolved (cond
                            (list? expr)  ;; If it's a nested function call, process recursively
                            (let [fn (resolve (first expr)) 
                                  args (mapv #(replace-symbols % in) (rest expr))
                                  apply-result (apply fn args)]
                              apply-result)
                            (and (symbol? expr) (resolve expr)) expr  ;; Global function, keep it
                            (and (symbol? expr) (not (contains? in expr))) (throw (ex-info "Variable not found" {:var expr}))
                            (symbol? expr) (get in expr)  ;; Replace query var with actual value
                            :else expr)]
                            resolved))];; Keep literals unchanged
    (fn [in]
      (replace-symbols call in))))

(defn apply-fn [f bindings deps] 
  (when (not (set/superset? (set (keys bindings)) (set deps)))
    (throw (ex-info "Unbound variables not allowed in function call" {:deps deps :bindings bindings})))
  (let [input-combinations (join-no-extra-cartesians (keep (fn [var] (to-named-tuples var (get bindings var))) deps))
        final (into {} (map (fn [tup] [tup (f tup)]) input-combinations))]
    final))

(defn new-fn-binding [call]
  (fn [bindings]
    (apply-fn (to-fn call) bindings (get-fn-deps call))))

(defn build-resolvers [in where]
  (let [conn ('$ in)
        clauses (replace-all-variables in where)
        resolvers (reduce (fn [acc clause]
                            (cond
                              (= (count clause) 3)
                              (let [[constrained attribute bound] clause]
                                (cond-> acc
                                  (symbol? constrained) (update constrained #(new-constraint-iterative attribute bound % conn))
                                  (symbol? bound) (update bound #(new-binding-iterative attribute constrained % conn))))
                              (= (count clause) 2)
                              (let [[call bound] clause
                                    assoced (assoc acc bound (new-fn-binding call))]
                                assoced)
                              (= (count clause) 1)
                              acc)) {} clauses)]
    resolvers))

;; At this point, we will have ONE set of homogenous named tuples 
(defn apply-predicates [tuples predicates] 
  (reduce (fn [remaining predicate]
            (let [f (to-fn (first predicate))] ;; predicates have one "word"
              (filter f remaining)))
          tuples
          predicates))

(defn into-vectors 
  ([conn result-bindings var-order]
   (into-vectors conn result-bindings var-order []))
  ([conn result-bindings var-order predicates]
   (let [sets (mapv (fn [[k v]] (to-named-tuples k v)) result-bindings) ;; vector of sets of named tuples
         unified (join-no-extra-cartesians sets)
         filtered (apply-predicates unified predicates)
         result (vec (doall (pmap #(reduce (fn [acc var] 
                                             (println "i-v reduce var" var)
                                             (println "%" %)
                                             (println "second var" (when (coll? var) (second var)))
                                             (cond 
                                               (symbol? var) (conj acc (get % var))
                                               (and (coll? var) (= (first var) 'pull)) 
                                               (conj acc (p/pull conn (last var) (get % (second var)))))) [] var-order) filtered)))]
     result)))

(defn build-dependency-graph [clauses]
  (let [var-dependencies (reduce (fn [acc clause]
                                   (let [assigning (last clause)  ;; Variable being assigned
                                         deps (get-fn-deps clause)]  ;; Variables needed before execution
                                     (assoc acc assigning deps)))  ;; Track var → dependencies
                                 {} clauses)]
    (reduce (fn [acc clause]
              (let [deps (reduce clojure.set/union
                                 (map #(get var-dependencies % #{}) (butlast clause)))]  ;; Get all dependencies for vars used in clause
                (assoc acc clause deps)))  ;; Store clause → its dependencies
            {} clauses)))

(defn topo-sort [clause-dependencies]
  (let [no-deps (into #{} (keep (fn [[clause deps]]
                                  (when (empty? deps) clause)))
                      clause-dependencies)]  ;; Find clauses that can run immediately
    (loop [sorted []
           available no-deps
           remaining clause-dependencies]
      (if (empty? available)
        (if (empty? remaining)
          sorted  ;; Successfully sorted
          (throw (ex-info "Circular dependency detected!" {:remaining remaining})))
        (let [next (first available)
              new-remaining (reduce (fn [deps-map [clause deps]]
                                      (let [updated-deps (disj deps next)]  ;; Remove resolved dependency
                                        (if (empty? updated-deps)
                                          (dissoc deps-map clause)  ;; Clause is now ready
                                          (assoc deps-map clause updated-deps))))
                                    remaining
                                    remaining)]
          (recur (conj sorted next) (disj available next) new-remaining))))))

(defn sort-fn-clauses [clauses]
  (let [clause-deps (build-dependency-graph clauses)]
    (topo-sort clause-deps)))

(defn q [body & inputs]
  (let [parsed (parse-query body)
        input-map (zipmap (:in parsed) inputs)
        conn ('$ input-map)
        resolvers (build-resolvers input-map (:where parsed))
        fn-clauses (filter (fn [clause] (= (count clause) 2)) (:where parsed))
        sorted-fn-clause (sort-fn-clauses fn-clauses) 
        attribute-clauses (filter (fn [clause] (= (count clause) 3)) (:where parsed))
        predicate-clauses (filter (fn [clause] (= (count clause) 1)) (:where parsed))
        sorted-attribute-clauses (sort-by (fn [clause] 
                                               (count (filter symbol? clause)))
                                                attribute-clauses) ;; predicate clauses after fn clauses
        sorted-clauses (concat sorted-attribute-clauses sorted-fn-clause)
        sorted-vars (reduce (fn [acc word]
                              (if (and (symbol? word) (not (contains? (set acc) word)))
                                (conj acc word)
                                acc)) [] (mapcat identity sorted-clauses))
        resolved (resolve-all {} resolvers sorted-vars)]
    (into-vectors conn resolved (:find parsed) predicate-clauses)))