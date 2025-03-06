(ns vol1n.dynalog.api
    (:require [vol1n.dynalog.core :as core]
              [vol1n.dynalog.transact :as tx]
              [vol1n.dynalog.query :as q]))

(def transact tx/transact)
(def client core/client)
(def q q/q)
(def connect core/connect)
(def db core/db)
(def clear-table! core/clear-table!)