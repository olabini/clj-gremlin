(ns clj-gremlin.core
  (:import (clj-gremlin.pipeline GremlinClojurePipeline)
           (com.tinkerpop.pipes PipeFunction)
           (com.tinkerpop.pipes.util.structures Pair)
           (com.tinkerpop.blueprints Graph
                                     Element)
           (com.tinkerpop.gremlin.java GremlinPipeline)
           ))

(defn clojure-pipeline [starts]
  (GremlinClojurePipeline. starts))

(defn clojure-pipe-function [f]
  (proxy [PipeFunction] []
      (compute [input] (f input))))

(defn clojure-pipe-function-pair-to-int [f]
  (proxy [PipeFunction] []
      (compute [^Pair input] (int (f (.getA input) (.getB input))))))

(defn V [^Graph g]
  (clojure-pipeline (.getVertices g)))

(defn E [^Graph g]
  (clojure-pipeline (.getEdges g)))

 (defn v
   ([^Graph g i] (.getVertex g i))
   ([^Graph g i & is] (map #(.getVertex g %) (cons i is))))

 (defn e
   ([^Graph g i] (.getEdge g i))
   ([^Graph g i & is] (map #(.getEdge g %) (cons i is))))

(defn props [^GremlinClojurePipeline p]
  (.map p))

(defn prop [^Element e k]
  (.getProperty e (name k)))

(defprotocol Steps
  (internal-out [self labels])
  (internal-outE [self labels])
  (internal-in [self labels])
  (internal-inE [self labels])
  (internal-both [self labels])
  (internal-bothE [self labels])
  (step  [self f])
  (transform  [self f])
  (id  [self])
  (label  [self])
  (outV [self])
  (inV [self])
  (bothV [self])
  (internal-gather [self f])
  (scatter [self])
  (internal-order [self f])
  (as [self label])
  (internal-select [self a f])
  )

(extend-protocol Steps
  GremlinPipeline
  (internal-out  [self labels] (.out  self (into-array String (map name labels))))
  (internal-outE [self labels] (.outE self (into-array String (map name labels))))
  (internal-in  [self labels] (.in  self (into-array String (map name labels))))
  (internal-inE [self labels] (.inE self (into-array String (map name labels))))
  (internal-both  [self labels] (.both  self (into-array String (map name labels))))
  (internal-bothE  [self labels] (.bothE  self (into-array String (map name labels))))
  (step  [self f]      (.step self (clojure-pipe-function f)))
  (transform [self f]  (.transform self (clojure-pipe-function f)))
  (id [self] (.id self))
  (label [self] (.label self))
  (outV [self] (.outV self))
  (inV [self] (.inV self))
  (bothV [self] (.bothV self))
  (internal-gather [self f]
    (if f
      (.gather self (clojure-pipe-function f))
      (.gather self)))
  (scatter [self] (.scatter self))
  (internal-order [self f]
    (if f
      (.order self (clojure-pipe-function-pair-to-int f))
      (.order self)))
  (as [self label] (.as self label))
  (internal-select [self a f]
    (if (or a f)
      (.select self a (into-array PipeFunction (if f (map clojure-pipe-function f) [])))
      (.select self)))

  Element
  (internal-out  [self labels] (internal-out  (clojure-pipeline self) labels))
  (internal-outE [self labels] (internal-outE (clojure-pipeline self) labels))
  (internal-in  [self labels] (internal-in  (clojure-pipeline self) labels))
  (internal-inE [self labels] (internal-inE (clojure-pipeline self) labels))
  (internal-both  [self labels] (internal-both  (clojure-pipeline self) labels))
  (internal-bothE  [self labels] (internal-bothE  (clojure-pipeline self) labels))
  (step  [self f]      (step  (clojure-pipeline self) f))
  (transform  [self f]      (transform  (clojure-pipeline self) f))
  (id [self] (id (clojure-pipeline self)))
  (label [self] (label (clojure-pipeline self)))
  (outV  [self] (outV (clojure-pipeline self)))
  (inV  [self] (inV (clojure-pipeline self)))
  (bothV  [self] (bothV (clojure-pipeline self)))
  (internal-gather  [self f] (internal-gather (clojure-pipeline self) f))
  (scatter  [self] (scatter (clojure-pipeline self)))
  (internal-order  [self f] (internal-order (clojure-pipeline self) f))
  (as [self label] (as (clojure-pipeline self) label))
  (internal-select [self a f] (internal-select (clojure-pipeline self) a f))
  )

(defn out [o & labels]
  (internal-out o labels))

(defn outE [o & labels]
  (internal-outE o labels))

(defn in [o & labels]
  (internal-in o labels))

(defn inE [o & labels]
  (internal-inE o labels))

(defn both [o & labels]
  (internal-both o labels))

(defn bothE [o & labels]
  (internal-bothE o labels))

(defn gather
  ([o]   (gather o nil))
  ([o f] (internal-gather o f)))

(defn order
  ([o]   (order o nil))
  ([o f] (internal-order o f)))

(defn select
  ([o] (select o nil nil))
  ([o f-or-a]
     (if (or (fn? f-or-a) (and (seq? f-or-a) (fn? (first f-or-a))))
       (select o nil f-or-a)
       (select o f-or-a nil)))
  ([o a f]
     (if (seq? f)
       (internal-select o a f)
       (internal-select o a (keep identity [f]))))
  )
