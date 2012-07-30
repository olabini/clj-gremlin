(ns clj-gremlin.core-test
  (:use clj-gremlin.core
        clojure.test)
  (:import (com.tinkerpop.blueprints.impls.tg TinkerGraphFactory)))

(defn ids? [left right]
  (= (into #{} (map #(.getId %) left)) right))

(let [g (TinkerGraphFactory/createTinkerGraph)]

  (deftest gremlin-test
    (testing "V"
      (is (ids? (V g) #{"3" "2" "1" "6" "5" "4"}))
      )

    (testing "properties"
      (is (= (into #{} (:name (V g))) #{"lop" "vadas" "marko" "peter" "ripple" "josh"}))
      (is (= (into #{} (props (V g))) #{
                                        {:name "lop" :lang "java"}
                                        {:name "vadas" :age 27}
                                        {:name "marko" :age 29}
                                        {:name "peter" :age 35}
                                        {:name "ripple" :lang "java"}
                                        {:name "josh" :age 32}
                                        }))
      )

    (testing "E"
      (is (ids? (E g) #{"10" "7" "9" "8" "11" "12"}))
      )

    (testing "v"
      (is (= (.getId (v g 1)) "1"))
      (is (ids? (v g 1 3) #{"1" "3"}))
      )

    (testing "property lookup"
      (let [vertice (v g 1)]
        (is (= (prop vertice :name) "marko"))
        (is (= (prop vertice :age) 29))
        )
      )

    (testing "out"
      (let [vertice (v g 1)]
        (is (ids? (out (V g)) #{"2" "4" "3" "5"}))
        (is (ids? (out vertice) #{"2" "3" "4"}))
        (is (ids? (out (V g) "knows") #{"2" "4"}))
        (is (ids? (out (V g) :knows) #{"2" "4"}))
        (is (ids? (out vertice "knows") #{"2" "4"}))
        (is (ids? (out vertice :knows) #{"2" "4"}))
        )
      )

    (testing "outE"
      (let [vertice (v g 1)]
        (is (ids? (outE vertice) #{"7" "9" "8"}))
        (is (ids? (outE vertice "knows") #{"7" "8"}))
        (is (ids? (outE vertice :knows) #{"7" "8"}))
        )
      )

    (testing "chains"
      (is (= (set (map str (-> g
                               (v 1)
                               outE
                               :weight
                               ))) #{"0.5" "0.4" "1.0"}))
      )
    )
  )




;; gremlin> // lets only traverse to the head vertices of those edges
;; gremlin> // that have a weight that is less than 1.0
;; gremlin> v.outE.has('weight', T.lt, 1.0f).inV
;; ==>v[2]
;; ==>v[3]
;; gremlin> // lets do the same, but with a more general "filter closure"
;; gremlin> v.outE.filter{it.weight < 1.0}.inV
;; ==>v[2]
;; ==>v[3]
;; gremlin> // lets get the property maps of these vertices.
;; gremlin> v.outE.filter{it.weight < 1.0}.inV.map
;; ==>{name=vadas, age=27}
;; ==>{name=lop, lang=java}
;; gremlin> // lets traverse to marko's 30+ year old friends' created projects
;; gremlin> v.out('knows').filter{it.age > 30}.out('created').name
;; ==>ripple
;; ==>lop
