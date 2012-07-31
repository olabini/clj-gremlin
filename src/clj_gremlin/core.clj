(ns clj-gremlin.core
  (:import (clj-gremlin.pipeline GremlinClojurePipeline)
           (com.tinkerpop.blueprints Graph
                                     Element)
           (com.tinkerpop.gremlin.java GremlinPipeline)
           ))

(defn clojure-pipeline [starts]
  (GremlinClojurePipeline. starts))

(defn V [^Graph g]
  (clojure-pipeline (.getVertices g)))

(defn E [^Graph g]
  (clojure-pipeline (.getEdges g)))

 (defn v
   ([^Graph g i] (.getVertex g i))
   ([^Graph g i & is] (map #(.getVertex g %) (cons i is))))

(defn props [^GremlinClojurePipeline p]
  (.map p))

(defn prop [^Element e k]
  (.getProperty e (name k)))

(defprotocol Out
  (iout [self labels])
  (ioutE [self labels])
  )

(extend-protocol Out
  GremlinPipeline
  (iout  [self labels] (.out  self (into-array String (map name labels))))
  (ioutE [self labels] (.outE self (into-array String (map name labels))))
  Element
  (iout  [self labels] (iout  (clojure-pipeline self) labels))
  (ioutE [self labels] (ioutE (clojure-pipeline self) labels))
)

(defn out [o & labels]
  (iout o labels))

(defn outE [o & labels]
  (ioutE o labels))
