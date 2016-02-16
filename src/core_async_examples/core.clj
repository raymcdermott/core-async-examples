(ns core-async-examples.core
  (:require [clojure.core.async :as async
             :refer [>! <! >!! <!! alts! go go-loop chan close! onto-chan put! timeout]]))

(defn stop-after-n-seconds [run-length]
  (let [out (chan)]
    (go
      (do
        (<! (timeout (* 1000 run-length)))
        (>! out :stop)))
    out))

(defn infinite-printer [data-ch]
  (go-loop []
    (if-let [data (<! data-ch)]
      (do
        (clojure.pprint/pprint data)
        (recur)))))

(defn finite-printer [termination-ch data-ch]
  (go-loop []
    (if-let [[data chan] (alts! [termination-ch data-ch])]
      (condp = chan
        termination-ch (println data)                       ; could check for val but, meh
        data-ch (do
                  (clojure.pprint/pprint data)
                  (recur))))))
