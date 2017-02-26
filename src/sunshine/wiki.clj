(ns sunshine.wiki
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [datasplash
             [api :as ds]]

            [clojure.edn :as edn])
  (:import [java.util UUID]
           [com.google.datastore.v1 Query PropertyFilter$Operator]
           [com.google.datastore.v1.client DatastoreHelper])
  (:gen-class))

(ds/defoptions WordCountOptions
               {:dataDir   {:type        String
                            :default     "gs://wikireading-atroche/data"
                            :description "Path of the file to read from"}
                :numShards {:type        Long
                            :description "Number of output shards (0 if the system should choose automatically)"
                            :default     1}})

(defn log2 [n]
  (/ (Math/log n) (Math/log 2)))

(defn entropy [values]
  (let [freqs (frequencies values)
        count-of-values (count values)]
    (if (= 1 count-of-values)
      0.0
      (/ (* -1
            (reduce (fn [acc [value freq]]
                      (let [prob (/ (float freq)
                                    count-of-values)]
                        (+ acc (* prob (log2 prob)))))

                    0
                    freqs))
         (log2 count-of-values)))))

;; get all the JSON in, as seperate collections
;; parse them into instances
;; combine to get entropy
;; output as seperate text files

(defn run-word-count
  [str-args]
  (let [p (ds/make-pipeline 'WordCountOptions str-args)
        {:keys [dataDir numShards]} (ds/get-pipeline-configuration p)
        instances (into {}
                        (for [dataset-name [:training]]
                          (let [fname (str dataDir "/short-json/" (name dataset-name) "*")
                                instances (->> p
                                               (ds/read-json-file fname
                                                                  {:name  (str "Read" (name dataset-name))
                                                                   :keyfn true})
                                               (ds/map (fn [json]
                                                         {:document (get json "document")
                                                          :property (get json "property")
                                                          :value    (get json "value")})
                                                       {:name (str "Instancing" (name dataset-name))}))]
                            [dataset-name instances])))
        ;all-instances (apply ds/concat (vals instances))
        prop-val-map (->> (:training instances)
                          (ds/map-kv (fn [instance]
                                       [(:property instance) (:value instance)])
                                     {:name "MakingIntoKV"})
                          (ds/group-by-key {:name "GroupingByValue"}))
        ;entropy-map (->> prop-val-map
        ;                 (ds/map-kv (fn [[property values]]
        ;                              [property (entropy values)])
        ;                            {:name "CalculateEntropyMap"}))
        prop-freq-map (->> prop-val-map
                           (ds/map-kv (fn [[property values]]
                                        [property (count values)])
                                      {:name "CalculateFreqMap"}))]

    (->> prop-freq-map
         (ds/map (fn [[prop ent]]
                   (format "%s: %s"
                           (clojure.string/replace prop #"[^a-zA-Z]" "_")
                           ent))
                 {:name "FormattingEntropyMap"})
         (ds/write-text-file (str dataDir "/prop-freqs-training.txt")
                             {:num-shards numShards
                              :name       "WritingFreqMap"}))
    ;
    ;(->> entropy-map
    ;     (ds/map (fn [[prop ent]]
    ;               (format "%s: %s"
    ;                       (clojure.string/replace prop #"[^a-zA-Z]" "_")
    ;                       ent))
    ;             {:name "FormattingEntropyMap"})
    ;     (ds/write-text-file (str dataDir "/entropy.txt")
    ;                         {:num-shards numShards
    ;                          :name       "WritingEntropyMap"}))
    ;
    ;(doseq [dataset-name [:training :testing :validation]]
    ;  (->> (dataset-name instances)
    ;       (ds/filter (fn [instance]
    ;                    (let [property (:property instance)
    ;                          prop-entropy (get-in (ds/side-inputs)
    ;                                               [:entropy-map property])]
    ;                      (< prop-entropy 0.5)))
    ;                  {:side-inputs {:entropy-map (ds/view {:type :map} entropy-map)}
    ;                   :name        "FilteringForLowEntropy"})
    ;       (ds/map-kv (fn [{:keys [property document value]}]
    ;                    [document (format "%s===%s"
    ;                                      (clojure.string/replace property
    ;                                                              #"[^a-zA-Z]" "_")
    ;                                      (clojure.string/replace value
    ;                                                              #"[^a-zA-Z]" "_"))])
    ;                  {:name "FormattingPropValuePairs"})
    ;       (ds/group-by-key {:name "GroupDocByPropValue"})
    ;       (ds/map (fn [[document prop-val-pairs]]
    ;                 (clojure.string/join " " (concat (for [pair prop-val-pairs]
    ;                                                    (str "__label__" pair))
    ;                                                  [document])))
    ;               {:name "FormatFastTextOutput"})
    ;       (ds/write-text-file (str dataDir "/fasttext/" (name dataset-name) "/" (name dataset-name) ".txt")
    ;                           {:num-shards numShards
    ;                            :name       (str (name dataset-name)
    ;                                             "WriteFastText")})))
    p))



(defn -main
  [& args]
  (println args)
  (println (apply str args))
  (compile 'sunshine.wiki)
  (ds/run-pipeline (run-word-count args)))
