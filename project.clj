(defproject sunshine "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :java-source-paths ["src"]
  :jvm-opts ["-Xmx10000M" "-d64"]
  :dependencies [[org.clojure/clojure "1.9.0-alpha14"]
                 [org.apache.beam/beam-sdks-java-core "0.5.0"]
                 [org.apache.beam/beam-runners-direct-java "0.5.0"]
                 [org.apache.beam/beam-runners-google-cloud-dataflow-java "0.5.0"]
                 [org.apache.beam/beam-sdks-java-io-google-cloud-platform "0.5.0"]
                 [datasplash "0.4.0"]
                 [com.taoensso/nippy "2.13.0"]
                 [com.damballa/abracad "0.4.13"]
                 [iota "1.1.3"]
                 [tesser.core "1.0.2"]
                 [cheshire "5.7.0"]])
