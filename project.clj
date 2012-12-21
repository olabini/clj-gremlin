(defproject clj-gremlin "0.0.3"
  :description "Implementation of TinkerPop Gremlin 2.0"
  :url "https://github.com/olabini/gremlin-clj"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [com.tinkerpop.gremlin/gremlin-java "2.2.0"]
                 [com.tinkerpop.blueprints/blueprints-core "2.2.0"]]
  :aot [clj-gremlin.pipeline]
;;  :warn-on-reflection true
  )
