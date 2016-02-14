(ns core-async-examples.time-series
  (:require [core-async-examples.core :refer :all]
            [core-async-examples.stock-management-tracker :refer :all]
            [clojure.core.async :as async :refer [>! <! >!! <!! alts! go go-loop chan close! onto-chan put! timeout]]
            [clj-time.core :as t]))

(defn- gen-window [start-time open-duration]
  {:pre [(> open-duration 0)]}
  (let [to-time (t/plus start-time (t/seconds open-duration))]
    (assoc {} :from start-time :to to-time :closed false :items [])))

(defn create-time-series-windows
  "Create a window for X seconds that refresh every Y seconds. 1 <= Y <= X"
  ([open-duration slide-interval]
   {:pre [(> open-duration 0) (> slide-interval 0) (>= open-duration slide-interval)]}
   (let [out (chan)]
     (go-loop [start-time (t/now)]
              (let [window (gen-window start-time open-duration)]
                (do
                  (>! out window)
                  (<! (timeout (* 1000 slide-interval)))
                  (if (= open-duration slide-interval)
                    (recur (:to window))
                    (recur (t/now))))))
     out))

  ([open-duration]
   {:pre [(> open-duration 0)]}
   (create-time-series-windows open-duration open-duration)))


; first demo
(finite-printer (stop-after-n-seconds 4) (create-time-series-windows 2))

(defn- maintain-active-windows [windows]
  (let [now (t/now)
        retention-period (t/millis 500)
        retention-boundary (t/minus now retention-period)
        retained (filter #(t/after? (:to %) retention-boundary) windows)
        to-be-closed (filter #(and (t/before? (:to %) now) (false? (:closed %))) windows)
        closing (map #(assoc % :closed true) to-be-closed)]
    (concat retained closing)))

(defn- within-interval? [from to time]
  {:pre [(t/before? from to)]}
  "Check whether a time is within an interval"
  (let [interval (t/interval from to)]
    (t/within? interval time)))

(defn- add-timed-item-to-windows [timed-item windows]
  "Add an item to the windows where the time intervals match"
  (if-let [[time item] timed-item]
    (let [matching-windows (filter #(within-interval? (:from %) (:to %) time) windows)
          updated-windows (map #(assoc % :items (conj (:items %) item)) matching-windows)]
      updated-windows)))

(defn data-in-timed-series
  "Add timed data from item-ch to the time series windows produced in the window-ch;
   emit the window once only, after it is closed"
  [item-ch window-ch]
  (let [out-ch (chan 1 (comp (map (fn [windows]
                                    (filter #(:closed %) windows)))
                             (filter (fn [windows] (> (count windows) 0)))))]
    (go-loop [active-windows ()]
             (if-let [[data chan] (alts! [item-ch window-ch])]
               (condp = chan
                 window-ch (if-let [windows (maintain-active-windows (conj active-windows data))]
                             (do
                               (>! out-ch windows)
                               (recur windows)))

                 item-ch (recur (add-timed-item-to-windows data active-windows)))
               (close! out-ch)))
    out-ch))

; second demo
(finite-printer (stop-after-n-seconds 10) (data-in-timed-series (gen-timed-orders 1000 parts) (create-time-series-windows 4)))


(defn interval-aggregator [aggregator in]
  "Execute the provided aggregator against the :items property from the maps on the channel"
  (let [out (chan)]
    (go-loop []
             (if-let [window (first (<! in))]
               (do
                 (let [results (aggregator (:items window))]
                   (>! out results))
                 (recur))
               (close! out)))
    out))

; aggregation demo
(finite-printer (stop-after-n-seconds 10) (interval-aggregator count (data-in-timed-series (gen-timed-orders 1000 parts) (create-time-series-windows 4))))