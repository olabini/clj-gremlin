(ns clj-gremlin.core
  (:import (clj-gremlin.pipeline GremlinClojurePipeline)
           (com.tinkerpop.pipes PipeFunction)
           (com.tinkerpop.blueprints Graph
                                     Element)
           (com.tinkerpop.gremlin.java GremlinPipeline)
           ))

(defn clojure-pipeline [starts]
  (GremlinClojurePipeline. starts))

(defn clojure-pipe-function [f]
  (proxy [PipeFunction] []
      (compute [input] (f input))))

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

(defprotocol Steps
  (iout [self labels])
  (ioutE [self labels])
  (step  [self f])
  )

(extend-protocol Steps
  GremlinPipeline
  (iout  [self labels] (.out  self (into-array String (map name labels))))
  (ioutE [self labels] (.outE self (into-array String (map name labels))))
  (step  [self f]      (.step self (clojure-pipe-function f)))
  Element
  (iout  [self labels] (iout  (clojure-pipeline self) labels))
  (ioutE [self labels] (ioutE (clojure-pipeline self) labels))
  (step  [self f]      (step  (clojure-pipeline self) f))
)

(defn out [o & labels]
  (iout o labels))

(defn outE [o & labels]
  (ioutE o labels))
