(ns xt-om.warehouse)

(defn- check-availability [sku qty]
  (let [available-balance (get {:dog-food 10 :cat-food 10} sku)
        new-balance (- available-balance qty)]
    (if (neg? new-balance)
      {:available? false
       :available-balance available-balance}
      {:available? true})))

(defn reserve-order [sku-quantities]
  (let [availability (map (fn [[sku qty]]
                            (assoc (check-availability sku qty) :sku sku))
                          sku-quantities)
        unavailable (filter #(not (:available? %)) availability)]
    {:success? (empty? unavailable)
     :short (->> unavailable
                (map #(select-keys % [:sku :available-balance]))
                (into []))}))