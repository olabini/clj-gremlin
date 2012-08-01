(ns clj-gremlin.core
  (:import (clj-gremlin.pipeline GremlinClojurePipeline)
           (com.tinkerpop.pipes PipeFunction)
           (com.tinkerpop.pipes.util.structures Pair)
           (com.tinkerpop.blueprints Graph
                                     Element)
           (com.tinkerpop.gremlin.java GremlinPipeline)
           (com.tinkerpop.gremlin Tokens$T)
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

(def tokens {
             =    Tokens$T/eq
             >    Tokens$T/gt
             >=   Tokens$T/gte
             <    Tokens$T/lt
             <=   Tokens$T/lte
             not= Tokens$T/neq
             })


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
  (internal-memo [self name m])
  (internal-path [self fs])
  (where [self f])
  (internal-at-single [self n])
  (internal-at-range [self from to])
  (internal-has-kv [self k v])
  (internal-has-cmp [self k cmp v])
  (internal-has-not-kv [self k v])
  (internal-has-not-cmp [self k cmp v])
  (back [self v])
  (_ [self])
  (random [self n])
  (except [self c])
  (retain [self c])
  (interval [self k from to])
  (internal-dedup-simple [self])
  (internal-dedup-fn [self f])
  )

(defn- ensure-type [o]
  (cond
    (instance? Long o) (int o)
    (instance? Double o) (float o)
    :else o))

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
  (internal-memo [self name m]
    (if m
      (.memoize self name m)
      (.memoize self name)))
  (internal-path [self fs] (.path self (into-array PipeFunction (map clojure-pipe-function fs))))
  (where [self f] (.filter self (clojure-pipe-function f)))
  (internal-at-single [self n] (.range self n n))
  (internal-at-range [self from to] (.range self from to))
  (internal-has-kv [self k v] (.has self k v))
  (internal-has-cmp [self k cmp v] (.has self k cmp v))
  (internal-has-not-kv [self k v] (.hasNot self k v))
  (internal-has-not-cmp [self k cmp v] (.hasNot self k cmp v))
  (back [self v] (.back self v))
  (_ [self] (._ self))
  (random [self n] (.random self n))
  (except [self c] (.except self c))
  (retain [self c] (.retain self c))
  (interval [self k from to] (.interval self (name k) (ensure-type from) (ensure-type to)))
  (internal-dedup-simple [self] (.dedup self))
  (internal-dedup-fn [self f] (.dedup self (clojure-pipe-function f)))

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
  (internal-memo [self name m] (internal-memo (clojure-pipeline self) name m))
  (internal-path [self fs] (internal-path (clojure-pipeline self) fs))
  (where [self f] (where (clojure-pipeline self) f))
  (internal-at-single [self n] (internal-at-single (clojure-pipeline self) n))
  (internal-at-range [self from to] (internal-at-range (clojure-pipeline self) from to))
  (internal-has-kv [self k v] (internal-has-kv (clojure-pipeline self) k v))
  (internal-has-cmp [self k cmp v] (internal-has-cmp (clojure-pipeline self) k cmp v))
  (internal-has-not-kv [self k v] (internal-has-not-kv (clojure-pipeline self) k v))
  (internal-has-not-cmp [self k cmp v] (internal-has-not-cmp (clojure-pipeline self) k cmp v))
  (back [self v] (back (clojure-pipeline self) v))
  (_ [self] (_ (clojure-pipeline self)))
  (random [self n] (random (clojure-pipeline self) n))
  (except [self c] (except (clojure-pipeline self) c))
  (retain [self c] (retain (clojure-pipeline self) c))
  (interval [self k from to] (interval (clojure-pipeline self) k from to))
  (internal-dedup-simple [self] (internal-dedup-simple (clojure-pipeline self)))
  (internal-dedup-fn [self f] (internal-dedup-fn (clojure-pipeline self) f))
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

(defn memo
  ([o name-or-number] (memo o name-or-number nil))
  ([o name-or-number m] (internal-memo o name-or-number m)))

(defn path [o & fs]
  (internal-path o fs))

(defn at
  ([o n] (internal-at-single o n))
  ([o n to] (internal-at-range o n to)))

(defn has
  ([o k v] (internal-has-kv o k v))
  ([o k cmp v] (internal-has-cmp o k (get tokens cmp cmp) (ensure-type v))))

(defn has-not
  ([o k v] (internal-has-not-kv o k v))
  ([o k cmp v] (internal-has-not-cmp o k (get tokens cmp cmp) (ensure-type v))))

(defn dedup
  ([o] (internal-dedup-simple o))
  ([o f] (internal-dedup-fn o f)))
