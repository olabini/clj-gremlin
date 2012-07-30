(ns clj-gremlin.core-test
  (:use clj-gremlin.core
        midje.sweet)
  (:import (com.tinkerpop.blueprints.impls.tg TinkerGraphFactory)))

(defn ids? [& all-ids]
  (fn [other]
    (= (into #{} (map #(.getId %) other)) (set all-ids))))

(let [g (TinkerGraphFactory/createTinkerGraph)]
  (facts "V"
    (V g) => (ids? "3" "2" "1" "6" "5" "4")
    )

  (facts "properties"
    (seq (:name (V g))) => (just #{"lop" "vadas" "marko" "peter" "ripple" "josh"})
    (seq (props (V g))) => (just #{
                                   {:name "lop" :lang "java"}
                                   {:name "vadas" :age 27}
                                   {:name "marko" :age 29}
                                   {:name "peter" :age 35}
                                   {:name "ripple" :lang "java"}
                                   {:name "josh" :age 32}
                                   })
    )
  )
