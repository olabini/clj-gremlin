(ns clj-gremlin.pipeline
  (:import (com.tinkerpop.blueprints Element)
           (com.tinkerpop.gremlin.pipes.transform PropertyMapPipe))
  (:gen-class
   :name clj-gremlin.pipeline.GremlinClojurePipeline
   :extends com.tinkerpop.gremlin.java.GremlinPipeline
   :implements [clojure.lang.ILookup]
   :constructors {[Object] [Object]}))

(defn property-map-pipe []
  (proxy [PropertyMapPipe] []
    (processNextStart []
      (let [result (proxy-super processNextStart)]
        (into {} (map (fn [[k v]] [(keyword k) v]) result))))))

(defn -valAt [p key]
  (.property p (name key)))

(defn -map [p]
  (.add p (property-map-pipe)))
