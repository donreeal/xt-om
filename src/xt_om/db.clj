(ns xt-om.db
  "TODO: 
     * expose only used node (try mount?)
     * move placement of order to separate ns"
  (:require [clojure.java.io :as io]
            [xtdb.api :as xt]
            [xt-om.warehouse :as warehouse]))

(defn start-xtdb! []
  (letfn [(kv-store [dir]
            {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                        :db-dir (io/file dir)
                        :sync? true}})]
    (xt/start-node
     {:xtdb/tx-log (kv-store ".dev-db/tx-log")
      :xtdb/document-store (kv-store ".dev-db/doc-store")
      :xtdb/index-store (kv-store ".dev-db/index-store")})))

(def xtdb-node (start-xtdb!))
;; note that attempting to eval this expression more than once 
;; before first calling `stop-xtdb!` will throw a RocksDB locking error.
;; this is because a node that depends on native libraries must be `.close`'d explicitly

(defn stop-xtdb! []
  (.close xtdb-node))

(defn audit-trail [session-id request-id time subject]
  {:request-id request-id
   :session-id session-id
   :at time
   :subject subject})

(defn- order->sku-quantities [order]
  (->> (:order-items order)
       (group-by :product-id)
       (map (fn [[sku positions]]
              [sku (reduce + (map :quantity positions))]))
       (into {})))

(defn place-order [order]
  (let [session-id         (str (random-uuid))
        req-id             (str (random-uuid))
        req-time           (java.time.Instant/now)
        audit-trail        (audit-trail session-id req-id req-time (:customer-id order))
        reservation-report (warehouse/reserve-order (order->sku-quantities order))]
    (if (:success? reservation-report)
      (do (xt/submit-tx
           xtdb-node
           [[::xt/put (assoc audit-trail :xt/id (str (random-uuid)) :db/doc-type :audit-outbox-event)]
            [::xt/put (assoc order :xt/id (str (random-uuid)) :db/doc-type :order)]])
          {:status 200})
      (do (xt/submit-tx
           xtdb-node
           [[::xt/put (assoc audit-trail :xt/id (str (random-uuid)) :db/doc-type :audit-outbox-event)]])
          {:status 500
           :type "xtdb-playground.order/out-of-stock"
           :detail (-> (select-keys reservation-report [:short]))}))))


(defn find-all-by-doc-type [doc-types]
  (->> (xt/q
        (xt/db xtdb-node)
        '{:find [(pull ?doc [*])]
          :where [[?doc :db/doc-type doc-type]]
          :in [[doc-type]]}
        doc-types)
       (mapcat identity)))

(->> (find-all-by-doc-type [:order])
     (map #(assoc % :db/version 1))
     (first))

(stop-xtdb!)

#_((def order {:customer-id :customer/bob
               :order-items [{:product-id :dog-food :quantity 2}]})
   (place-order order))















