(ns clj-gremlin.core
  (:import (clj-gremlin.pipeline GremlinClojurePipeline)
           (com.tinkerpop.blueprints Graph)))

(defn clojure-pipeline [starts]
  (GremlinClojurePipeline. starts))

(defn V [^Graph g]
  (clojure-pipeline (.getVertices g)))

(defn props [^GremlinClojurePipeline p]
  (.map p))
