(ns sunshine.less-bloat
  (:require [datasplash.api :as ds]
            [abracad.avro :as avro])
  (:import (org.apache.avro.generic GenericData$Record GenericData$Array)
           (com.google.cloud.dataflow.sdk.io AvroIO$Write)))

;; just used for parsing command line options
(ds/defoptions LessBloatOptions
               ;; (these options don't support kebab case)
               ;; dataDir is used for running this against my local data as a way to test quickly:
               {:dataDir {:type        String
                          :default     "gs://wikireading-atroche/data"
                          :description "Path where files are stored"}})

(defn json-files->clj [pipeline data-dir dataset-name]
  (let [fname (str data-dir "/short-json/" (name dataset-name) "*")]
    ;; e.g. /data/short-json/training.json-00028-of-00157
    (->> pipeline
         (ds/read-json-file fname
                            ;; name to show in the monitoring interface in Dataflow
                            {:name   (str "ReadJSONof" (name dataset-name))
                             ;; parse keys like "property" -> :property
                             :key-fn true}))))

(defn group-by-document [instances]
  (->> instances
       ;; need to go from {doc, prop, values} map to -> [doc {prop, values}]
       ;; tuple for Dataflow's group-by-key to work
       (ds/map-kv (fn [{:keys [property document value]}]
                    [document {:property property
                               :value    value}])
                  {:name "KeyByDocument"})
       (ds/group-by-key {:name "GroupByDocument"})
       (ds/map (fn [instance]
                 (let [[doc statements] instance]
                   {:document   doc
                    :statements statements})))))

(defn less-bloat-pipeline [args]
  (let [p (ds/make-pipeline 'LessBloatOptions args)
        {:keys [dataDir]} (ds/get-pipeline-configuration p)]

    ;; we want different files for each subset,
    ;; not to aggregate over whole dataset:
    (doseq [dataset-name [:training :validation :testing]
            :let [instances (json-files->clj p dataDir dataset-name)]]
      (->> instances
           group-by-document
           (ds/write-json-file (str dataDir "/" (name dataset-name) ".json")
                               {:num-shards 1})))

    ;; need to return the pipeline object so it can be run
    p))

(def statement-schema
  (avro/parse-schema
    {:name   :statement
     :type   :record
     :fields [{:name "property"
               :type "string"}
              {:name "value"
               :type "string"}]}))

(def statements-schema
  (avro/parse-schema
    {:type  :array
     :items statement-schema}))

(def schema
  (avro/parse-schema
    {"namespace" "sunshine.avro",
     "type"      "record",
     "name"      "Instance",
     "fields"    [{"name" "document",
                   "type" "string",}
                  {"name" "statements",
                   "type" statements-schema}]}))


(defn statement->avro [statement]
  (doto (GenericData$Record. statement-schema)
    (.put "property" (:property statement))
    (.put "value" (:value statement))))

(defn instance->avro [instance]
  (let [statements (GenericData$Array. statements-schema
                                       (map statement->avro
                                            (:statements instance)))]
    (doto (GenericData$Record. schema)
      (.put "document" (:document instance))
      (.put "statements" statements))))

(instance->avro {:document   "asdfasdf"
                 :statements [{:property "instance of" :value "human"}]})

(defn convert-to-avro [args]
  (let [p (ds/make-pipeline 'LessBloatOptions args)
        {:keys [dataDir]} (ds/get-pipeline-configuration p)]

    ;; we want different files for each subset,
    ;; not to aggregate over whole dataset:
    (doseq [dataset-name [:training :validation :testing]
            :let [instances (json-files->clj p dataDir dataset-name)]]
      (.apply (->> instances
                (ds/map instance->avro {:name "ToAvro"}))
              (-> (AvroIO$Write/named "WriteToAvro")
                (.to (str dataDir "/" (name dataset-name)))
                (.withSchema schema)
                (.withNumShards 1)
                (.withSuffix ".avro"))))

    ;; need to return the pipeline object so it can be run
    p))


(defn -main
  [& args]
  (compile 'sunshine.less-bloat)
  (ds/run-pipeline (convert-to-avro args)))
