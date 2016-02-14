(ns core-async-examples.stock-management-tracker
  (:require [core-async-examples.core :refer :all]
            [clojure.core.async :as async :refer [>! <! >!! <!! alts! go go-loop chan close! onto-chan put! timeout]]
            [clj-time.core :as t]))

; Wikipedia sourced list of body and main parts
(def manifest ["Bonnet/hood" "Bonnet/hood latch" "Bumper" "Unexposed bumper" "Exposed bumper"
               "Cowl screen" "Decklid" "Fascia rear and support" "Fender (wing or mudguard)"
               "Front clip" "Front fascia and header panel" "Grille (also called grill)"
               "Pillar and hard trim" "Quarter panel" "Radiator core support"
               "Rocker" "Roof rack" "Spoiler" "Front spoiler (air dam)" "Rear spoiler (wing)" "Rims" "Hubcap"
               "Tire/Tyre" "Trim package" "Trunk/boot/hatch" "Trunk/boot latch" "Valance" "Welded assembly"
               "Outer door handle" "Inner door handle" "Door control module" "Door seal"
               "Door watershield" "Hinge" "Door latch" "Door lock and power door locks" "Center-locking"
               "Fuel tank (or fuel filler) door" "Window glass" "Sunroof" "Sunroof motor" "Window motor"
               "Window regulator" "Windshield (also called windscreen)" "Windshield washer motor"
               "Window seal"])

; magic up 460 items with variants for each part (adjust the range to create more / less)
(def parts (mapcat (fn [part]
                     (map #(assoc {} :id (gensym) :description (str part %)) (range 10)))
                   manifest))

(defn gen-random-deliveries [max-wait-millis coll]
  (let [out (chan)]
    (go-loop []
             (do
               (>! out [(t/now) (rand-nth coll)])
               (<! (timeout (rand-int max-wait-millis)))
               (recur)))
    out))

(def deliveries (gen-random-deliveries 1000 parts))

(defn gen-timed-orders [frequency-millis coll]
  (let [out (chan)]
    (go-loop []
             (do
               (>! out [(t/now) (rand-nth coll)])
               (<! (timeout frequency-millis))
               (recur)))
    out))

(def orders (gen-timed-orders 200 parts))

(defn modify-stock [count-adjust-fn stock item]
  (let [current-value (first (clojure.set/select #(= (:id %) (:id item)) stock))
        new-value (assoc current-value :count (count-adjust-fn (:count current-value)))
        updated-stock (conj stock new-value)]
    [updated-stock new-value]))

(defn stock-levels [orders deliveries]
  "Stock levels - also includes demand (negative stock numbers)"
  (let [out (chan)]
    (go-loop [stock (into #{} (map #(assoc {} :id (:id %) :count 0) parts))]
             (if-let [[data chan] (alts! [orders deliveries])]
               (let [[_ item] data
                     update-operation (condp = chan
                                        orders (partial modify-stock dec)
                                        deliveries (partial modify-stock inc))]
                 (if-let [[modified-stock updated-item] (update-operation stock item)]
                   (do
                     (>! out updated-item)
                     (recur modified-stock))))
               (close! out)))
    out))

(def stock-levels-ch (stock-levels orders deliveries))

(finite-printer (stop-after-n-seconds 2) stock-levels-ch)

(defn detect-backlogs [low-water-mark stock-levels]
  "Show where stock levels fall below a low water mark "
  (let [out (chan 1 (filter (fn [stock-level-for-part]
                              (let [backlog (:count stock-level-for-part)]
                                (< backlog low-water-mark)))))]
    (go-loop []
             (if-let [stock-level-for-part (<! stock-levels)]
               (do
                 (>! out stock-level-for-part)
                 (recur))
               (close! out)))
    out))


(def order-chan (async/mult (gen-timed-orders 20 parts)))  ; use mult to allow many readers

(def deliveries-chan (gen-random-deliveries 1000 parts))

; show what's happening with stock
(def stock-chan (stock-levels (async/tap order-chan (chan)) deliveries-chan))
(def backlogs-chan (detect-backlogs -4 stock-chan))
(finite-printer (stop-after-n-seconds 20) backlogs-chan)

; show what's happening with order peaks
;(def windows-chan (create-time-series-windows 5))
;(def time-series (time-series-data (async/tap order-chan (chan)) windows-chan))
;(def peaks (detect-order-peaks 2 time-series))
;(finite-printer (stop-after-n-seconds 30) peaks)



(defn detect-order-peaks [high-water-mark in]
  "Show orders that occur more often a high water mark within any given window"
  (let [out (chan 1 (comp (mapcat (fn [window]
                                    (frequencies (:items window))))
                          (filter (fn [order]
                                    (let [[_ freq] order]
                                      (> freq high-water-mark))))))]
    (go-loop []
             (if-let [windows (<! in)]
               (do
                 (>! out windows)
                 (recur))
               (close! out)))
    out))

