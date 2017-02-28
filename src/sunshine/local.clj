(ns sunshine.local
  (:require [cheshire.core :as json]
            [abracad.avro :as avro]
            [iota]
            [tesser.core :as t]))

(def fname "data/json/training/train.json")

(def lines (iota/seq fname))

(first lines)

;; try where reducer is the one making freqs, vs combiner

(defn log2 [n]
  (/ (Math/log n) (Math/log 2)))

(defn entropy [freqs count-of-values]
  (if (= 1 count-of-values)
    0.0
    (/ (* -1
          (reduce (fn [acc [value freq]]
                    (let [prob (/ (float freq)
                                  count-of-values)]
                      (+ acc (* prob (log2 prob)))))

                  0
                  freqs))
       (log2 count-of-values))))

(time
  (def blah
    (->> (t/map json/parse-string)
         (t/map (fn [instance]
                  {:property (clojure.string/join " " (get instance "question_string_sequence"))
                   :values   (get instance "raw_answers")}))
         (t/group-by :property)
         (t/mapcat :values)
         (t/frequencies)
         (t/post-combine (fn [acc]
                           (into {} (for [[prop freqs] acc
                                          :let [num-vals (reduce + (vals freqs))]]
                                      [prop (entropy freqs num-vals)]))))
         ;(t/into [])

         ;(t/fold {:reducer-identity (constantly {})
         ;         :reducer          (fn [acc new-value]
         ;                             (println new-value)
         ;                             (concat acc new-value))})
         (t/tesser (t/chunk 1024 lines)))))

;
;(time
;  (def blah
;    (doall
;      (reduce (fn [wc instance]
;                (let [prop (get-in instance ["statement" 0 "property"])]
;                  (assoc wc prop (inc (or (get wc prop)
;                                          0)))))
;              {}
;              (json/parsed-seq (clojure.java.io/reader "testing.json-00000-of-00001"))))))
;
;
;(time
;  (def blah
;    (doall
;      (reduce (fn [wc instance]
;                (let [prop (get-in instance ["statement" 0 "property"])]
;                  (assoc wc prop (inc (or (get wc prop)
;                                          0)))))
;              {}
;              (avro/data-file-reader "test.avro")))))
;
;(first (json/parsed-seq (clojure.java.io/reader "testing.json-00000-of-00001")))