(ns xt-om.db
  "exposes xtdb node `xtdb-node`"
  (:require [clojure.java.io :as io]
            [xtdb.api :as xt]
            [mount.core :as mount]))


(defn start-xtdb! []
  (letfn [(kv-store [dir]
            {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                        :db-dir (io/file dir)
                        :sync? true}})]
    (xt/start-node
     {:xtdb/tx-log (kv-store ".dev-db/tx-log")
      :xtdb/document-store (kv-store ".dev-db/doc-store")
      :xtdb/index-store (kv-store ".dev-db/index-store")})))

(defn stop-xtdb! [node] 
  (.close node))

(mount/defstate 
  xtdb-node
  :start (start-xtdb!)
  :stop (stop-xtdb! xtdb-node))
