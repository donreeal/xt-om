(ns xt-om.core
  (:require [xtdb.api :as xt]
            [xt-om.db :as db]
            [xt-om.warehouse :as warehouse]
            [mount.core :as mount]))

(mount/start)

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
           db/xtdb-node
           [[::xt/put (assoc audit-trail :xt/id (str (random-uuid)) :db/doc-type :audit-outbox-event)]
            [::xt/put (assoc order :xt/id (str (random-uuid)) :db/doc-type :order)]])
          {:status 200})
      (do (xt/submit-tx
           db/xtdb-node
           [[::xt/put (assoc audit-trail :xt/id (str (random-uuid)) :db/doc-type :audit-outbox-event)]])
          {:status 500
           :type "xtdb-playground.order/out-of-stock"
           :detail (-> (select-keys reservation-report [:short]))}))))


(defn find-all-by-doc-type [doc-types]
  (->> (xt/q
        (xt/db db/xtdb-node)
        '{:find [(pull ?doc [*])]
          :where [[?doc :db/doc-type doc-type]]
          :in [[doc-type]]}
        doc-types)
       (mapcat identity)))

(->> (find-all-by-doc-type [:order])
     (map #(assoc % :db/version 1))
     (first))

#_((def order {:customer-id :customer/bob
               :order-items [{:product-id :dog-food :quantity 2}]})
   (place-order order))













