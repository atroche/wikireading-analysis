(ns sunshine.splash
  (:require [clojure.string :as str]
    [clojure.tools.logging :as log]
    [datasplash
     [api :as ds]
     [bq :as bq]
     [datastore :as dts]
     [pubsub :as ps]]
    [clojure.edn :as edn])
  (:import [java.util UUID]
    [com.google.datastore.v1 Query PropertyFilter$Operator]
    [com.google.datastore.v1.client DatastoreHelper])
  (:gen-class))


(defn tokenize
  [l]
  (remove empty? (.split (str/trim l) "[^a-zA-Z']+")))

(defn count-words
  [p]
  (ds/->> :count-words p
          (ds/mapcat tokenize {:name :tokenize})
          (ds/frequencies)))

(defn format-count
  [[k v]]
  (format "%s: %d" k v))

(ds/defoptions WordCountOptions
               {:input {:type String
                        :default "gs://clojure-beam/short-shakespeare.txt"
                        :description "Path of the file to read from"}
                :output {:type String
                         :default "gs://clojure-beam/out-splash.txt"
                         :description "Path of the file to write to"}
                :numShards {:type Long
                            :description "Number of output shards (0 if the system should choose automatically)"
                            :default 0}})

(defn run-word-count
  [str-args]
  (let [p (ds/make-pipeline 'WordCountOptions str-args)
        {:keys [input output numShards]} (ds/get-pipeline-configuration p)]
    (->> p
         (ds/read-text-file input {:name "King-Lear"})
         (count-words)
         (ds/map format-count {:name :format-count})
         (ds/write-text-file output {:num-shards numShards}))))

(defn -main
  [& args]
  (println args)
  (println (apply str args))
  (compile 'sunshine.splash)
  (ds/run-pipeline (run-word-count args)))
