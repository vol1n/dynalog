(ns vol1n.dynomic.api
    (:require [vol1n.dynomic.core :as core]
              [vol1n.dynomic.transact :as tx]))

(def transact tx/transact)
(def client core/client)
(def connect core/connect)
(def db core/db)
(def clear-table! core/clear-table!)