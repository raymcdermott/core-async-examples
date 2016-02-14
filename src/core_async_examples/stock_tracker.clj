(ns core-async-examples.stock-tracker
  (:require [core-async-examples.core :refer :all]
            [clojure.core.async :as async
             :refer [>! <! >!! <!! alts! go go-loop chan close! onto-chan put! timeout]]))

; fake call to a REST API
(defn supplier-api-call [order]
  (println "Calling REST API with order")
  (clojure.pprint/pprint order))

; call the API when a message comes on the channel
(defn supplier-notify [order-ch]
  (let []
    (go-loop []
             (if-let [order (<! order-ch)]
               (do
                 (supplier-api-call order)
                 (recur))))))


(def supplier-order-ch (chan))

(def supplier-order-ch-mult (async/mult supplier-order-ch))

(supplier-notify (async/tap supplier-order-ch-mult (chan)))

; this should result in a REST call
(println "Putting a message on supplier-order-ch - emit REST API call")
(put! supplier-order-ch {:id (gensym) :supplier "Acme Supplies" :quantity 100 })


(defn stock-tracker [low-water-mark stock-levels]
  "Show where stock levels fall below a low water mark "
  (let [out (chan 1 (filter (fn [stock-level-for-part]
                              (let [backlog (:quantity stock-level-for-part)]
                                (< backlog low-water-mark)))))]
    (go-loop []
             (if-let [stock-level-for-part (<! stock-levels)]
               (do
                 (>! out stock-level-for-part)
                 (recur))
               (close! out)))
    out))

; hook up the tracker and the notifications

(def stock-tracker-ch (chan))
(def stock-tracker-ch-mult (async/mult stock-tracker-ch))

(def low-water-mark 50)

(stock-tracker low-water-mark (async/tap stock-tracker-ch-mult (chan)))

(println "Putting a message on stock-tracker-ch - emit nothing")

(put! stock-tracker-ch {:id (gensym) :supplier "Acme Supplies" :quantity 10 })

(println "Putting a message on stock-tracker-ch - emit REST API call")
(put! supplier-order-ch {:id (gensym) :supplier "Acme Supplies" :quantity 100 })
