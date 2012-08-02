(ns clj-gremlin.core
  (:import (clj-gremlin.pipeline GremlinClojurePipeline)
           (com.tinkerpop.pipes Pipe
                                PipeFunction)
           (com.tinkerpop.pipes.sideeffect SideEffectPipe)
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

(defn clojure-pipe-function-pair [f]
  (proxy [PipeFunction] []
      (compute [^Pair input] (f (.getA input) (.getB input)))))

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
  (internal-_ [self])
  (random [self n])
  (except [self c])
  (retain [self c])
  (interval [self k from to])
  (internal-dedup-simple [self])
  (internal-dedup-fn [self f])
  (side-effect [self f])
  (internal-group-2 [self f1 f2])
  (internal-group-3 [self m f1 f2])
  (internal-group-4 [self m f1 f2 f3])
  (internal-group-count-0 [self])
  (internal-group-count-1 [self m])
  (internal-group-count-1f [self f1])
  (internal-group-count-2 [self m f1])
  (internal-group-count-3 [self m f1 f2])
  (internal-aggregate-0 [self])
  (internal-aggregate-1 [self m])
  (internal-aggregate-1f [self f])
  (internal-aggregate-2 [self m f])
  (internal-table-0 [self])
  (internal-table-1 [self t])
  (internal-table-1f [self f])
  (internal-table-fns [self t fs])
  (internal-table-fns-only [self fs])
  (internal-tree-0 [self])
  (internal-tree-1 [self m])
  (internal-tree-1f [self f])
  (internal-tree-fns [self m fs])
  (internal-tree-fns-only [self fs])
  (optional [self at])
  (internal-store-0 [self])
  (internal-store-1 [self l])
  (internal-store-1f [self l])
  (internal-store-2 [self l f])
  (internal-loop-1 [self at f])
  (internal-loop-2 [self at f1 f2])
  (if-then-else [self i t e])
  (internal-copy-split [self pipes])
  (fair-merge [self])
  (exhaust-merge [self])
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
  (internal-_ [self] (._ self))
  (random [self n] (.random self n))
  (except [self c] (.except self c))
  (retain [self c] (.retain self c))
  (interval [self k from to] (.interval self (name k) (ensure-type from) (ensure-type to)))
  (internal-dedup-simple [self] (.dedup self))
  (internal-dedup-fn [self f] (.dedup self (clojure-pipe-function f)))
  (side-effect [self f] (.sideEffect self (clojure-pipe-function f)))
  (internal-group-2 [self f1 f2] (.groupBy self (clojure-pipe-function f1) (clojure-pipe-function f2)))
  (internal-group-3 [self m f1 f2] (.groupBy self m (clojure-pipe-function f1) (clojure-pipe-function f2)))
  (internal-group-4 [self m f1 f2 f3] (.groupBy self m (clojure-pipe-function f1) (clojure-pipe-function f2) (clojure-pipe-function f3)))
  (internal-group-count-0 [self] (.groupCount self))
  (internal-group-count-1 [self m] (.groupCount self m))
  (internal-group-count-1f [self f1] (.groupCount self (clojure-pipe-function f1)))
  (internal-group-count-2 [self m f1] (.groupCount self m (clojure-pipe-function f1)))
  (internal-group-count-3 [self m f1 f2] (.groupCount self m (clojure-pipe-function f1) (clojure-pipe-function-pair f2)))
  (internal-aggregate-0 [self] (.aggregate self))
  (internal-aggregate-1 [self m] (.aggregate self m))
  (internal-aggregate-1f [self f] (.aggregate self (clojure-pipe-function f)))
  (internal-aggregate-2 [self m f] (.aggregate self m (clojure-pipe-function f)))
  (internal-table-0 [self] (.table self))
  (internal-table-1 [self t] (.table self t))
  (internal-table-1f [self f] (.table self (into-array PipeFunction [(clojure-pipe-function f)])))
  (internal-table-fns [self t fs] (.table self t (into-array PipeFunction (map clojure-pipe-function fs))))
  (internal-table-fns-only [self fs] (.table self (into-array PipeFunction (map clojure-pipe-function fs))))
  (internal-tree-0 [self] (.tree self (into-array PipeFunction [])))
  (internal-tree-1 [self m] (.tree self m (into-array PipeFunction [])))
  (internal-tree-1f [self f] (.tree self (into-array PipeFunction [(clojure-pipe-function f)])))
  (internal-tree-fns [self m fs] (.tree self m (into-array PipeFunction (map clojure-pipe-function fs))))
  (internal-tree-fns-only [self fs] (.tree self (into-array PipeFunction (map clojure-pipe-function fs))))
  (optional [self at] (.optional self at))
  (internal-store-0 [self] (.store self))
  (internal-store-1 [self l] (.store self l))
  (internal-store-1f [self f] (.store self (clojure-pipe-function f)))
  (internal-store-2 [self l f] (.store self l (clojure-pipe-function f)))
  (internal-loop-1 [self at f] (.loop self at (clojure-pipe-function f)))
  (internal-loop-2 [self at f1 f2] (.loop self at (clojure-pipe-function f1) (clojure-pipe-function f2)))
  (if-then-else [self i t e] (.ifThenElse self (clojure-pipe-function i) (clojure-pipe-function t) (clojure-pipe-function e)))
  (internal-copy-split [self pipes] (.copySplit self (into-array Pipe pipes)))
  (fair-merge [self] (.fairMerge self))
  (exhaust-merge [self] (.exhaustMerge self))

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
  (internal-_ [self] (internal-_ (clojure-pipeline self)))
  (random [self n] (random (clojure-pipeline self) n))
  (except [self c] (except (clojure-pipeline self) c))
  (retain [self c] (retain (clojure-pipeline self) c))
  (interval [self k from to] (interval (clojure-pipeline self) k from to))
  (internal-dedup-simple [self] (internal-dedup-simple (clojure-pipeline self)))
  (internal-dedup-fn [self f] (internal-dedup-fn (clojure-pipeline self) f))
  (side-effect [self f] (side-effect (clojure-pipeline self) f))
  (internal-group-2 [self f1 f2] (internal-group-2 (clojure-pipeline self) f1 f2))
  (internal-group-3 [self m f1 f2] (internal-group-3 (clojure-pipeline self) m f1 f2))
  (internal-group-4 [self m f1 f2 f3] (internal-group-4 (clojure-pipeline self) m f1 f2 f3))
  (internal-group-count-0 [self] (internal-group-count-0 (clojure-pipeline self)))
  (internal-group-count-1 [self m] (internal-group-count-1 (clojure-pipeline self) m))
  (internal-group-count-1f [self f1] (internal-group-count-1f (clojure-pipeline self) f1))
  (internal-group-count-2 [self m f1] (internal-group-count-2 (clojure-pipeline self) m f1))
  (internal-group-count-3 [self m f1 f2] (internal-group-count-3 (clojure-pipeline self) m f1 f2))
  (internal-aggregate-0 [self] (internal-aggregate-0 (clojure-pipeline self)))
  (internal-aggregate-1 [self m] (internal-aggregate-1 (clojure-pipeline self) m))
  (internal-aggregate-1f [self f] (internal-aggregate-1f (clojure-pipeline self) f))
  (internal-aggregate-2 [self m f] (internal-aggregate-2 (clojure-pipeline self) m f))
  (internal-table-0 [self] (internal-table-0 (clojure-pipeline self)))
  (internal-table-1 [self t] (internal-table-1 (clojure-pipeline self) t))
  (internal-table-1f [self f] (internal-table-1f (clojure-pipeline self) f))
  (internal-table-fns [self t fs] (internal-table-fns (clojure-pipeline self) t fs))
  (internal-table-fns-only [self fs] (internal-table-fns-only (clojure-pipeline self) fs))
  (internal-tree-0 [self] (internal-tree-0 (clojure-pipeline self)))
  (internal-tree-1 [self m] (internal-tree-1 (clojure-pipeline self) m))
  (internal-tree-1f [self f] (internal-tree-1f (clojure-pipeline self) f))
  (internal-tree-fns [self m fs] (internal-tree-fns (clojure-pipeline self) m fs))
  (internal-tree-fns-only [self fs] (internal-tree-fns-only (clojure-pipeline self) fs))
  (optional [self at] (optional (clojure-pipeline self) at))
  (internal-store-0 [self] (internal-store-0 (clojure-pipeline self)))
  (internal-store-1 [self l] (internal-store-1 (clojure-pipeline self) l))
  (internal-store-1f [self f] (internal-store-1f (clojure-pipeline self) f))
  (internal-store-2 [self l f] (internal-store-2 (clojure-pipeline self) l f))
  (internal-loop-1 [self at f] (internal-loop-1 (clojure-pipeline self) at f))
  (internal-loop-2 [self at f1 f2] (internal-loop-2 (clojure-pipeline self) at f1 f2))
  (if-then-else [self i t e] (if-then-else (clojure-pipeline self) i t e))
  (internal-copy-split [self pipes] (internal-copy-split (clojure-pipeline self) pipes))
  (fair-merge [self] (fair-merge (clojure-pipeline self)))
  (exhaust-merge [self] (exhaust-merge (clojure-pipeline self)))
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

(defn group
  ([o f1 f2] (internal-group-2 o f1 f2))
  ([o m f1 f2] (internal-group-3 o m f1 f2))
  ([o m f1 f2 f3] (internal-group-4 o m f1 f2 f3)))

(defn group-count
  ([o] (internal-group-count-0 o))
  ([o f1]
     (if (fn? f1)
       (internal-group-count-1f o f1)
       (internal-group-count-1 o f1)))
  ([o m f1] (internal-group-count-2 o m f1))
  ([o m f1 f2] (internal-group-count-3 o m f1 f2)))

(defn aggregate
  ([o] (internal-aggregate-0 o))
  ([o m]
     (if (fn? m)
       (internal-aggregate-1f o m)
       (internal-aggregate-1 o m)))
  ([o m f] (internal-aggregate-2 o m f)))

(defn table
  ([o] (internal-table-0 o))
  ([o t]
     (if (fn? t)
       (internal-table-1f o t)
       (internal-table-1 o t)))
  ([o t & fs]
     (if (fn? t)
       (internal-table-fns-only o (cons t fs))
       (internal-table-fns o t fs))))

(defn tree
  ([o] (internal-tree-0 o))
  ([o m]
     (if (fn? m)
       (internal-tree-1f o m)
       (internal-tree-1 o m)))
  ([o m & fs]
     (if (fn? m)
       (internal-tree-fns-only o (cons m fs))
       (internal-tree-fns o m fs))))

(defn store
  ([o] (internal-store-0 o))
  ([o l]
     (if (fn? l)
       (internal-store-1f o l)
       (internal-store-1 o l)))
  ([o l f] (internal-store-2 o l f)))

(defn looping
  ([o at f] (internal-loop-1 o at f))
  ([o at f1 f2] (internal-loop-2 o at f1 f2)))

(defn _
  ([] (clojure-pipeline nil))
  ([o] (internal-_ o)))

(defn copy-split [o & pipes]
  (internal-copy-split o pipes))

(defn cap [^SideEffectPipe o]
  (.cap o))

(defn simple-path [o]
  (.simplePath o))

(defn enable-path [o]
  (.enablePath o))
