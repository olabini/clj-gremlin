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

(defn first-class [f & rest] (class f))

(defmulti out first-class)

(defmethod out GremlinPipeline
  [p & labels] (.out p (into-array String (map name labels))))

(defmethod out Element
  [e & labels] (apply out (clojure-pipeline e) labels))


(defmulti outE first-class)

(defmethod outE GremlinPipeline
  [p & labels] (.outE p (into-array String (map name labels))))

(defmethod outE Element
  [e & labels] (apply outE (clojure-pipeline e) labels))
