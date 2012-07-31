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

    (testing "step"
      (is (= (into #{} (step (V g) #(prop (.next %) :name)))   #{"lop" "vadas" "marko" "peter" "ripple" "josh"}))
      (is (= (into #{} (step (v g 1) #(prop (.next %) :name))) #{"marko"}))
      )

    ;; transform{closure}
    ;; _      -- not sure if we need this
    ;; id
    ;; label
    ;; in(labels...?)
    ;; inE(labels...?)
    ;; both(labels...?)
    ;; bothE(labels...?)
    ;; outV
    ;; inV
    ;; bothV
    ;; memoize(string, map?)
    ;; memoize(integer, map?)
    ;; gather{closure?}
    ;; scatter
    ;; path(closures..?)
    ;; cap
    ;; select(list?,closures..?)
    ;; order(closure?)

    ;; filter{closure}  -- should be "where" instead
    ;; [i]
    ;; [i..j]
    ;; has('key',value)
    ;; has('key',T,value)
    ;; hasNot('key',value)
    ;; hasNot('key',T,value)
    ;; back(integer)
    ;; back(string)
    ;; and(pipes...)
    ;; or(pipes...)
    ;; random(double)
    ;; dedup(closure?)
    ;; simplePath
    ;; except(collection)
    ;; retain(collection)
    ;; interval(key,start,end)

    ;; sideEffect{closure}
    ;; groupBy(map?){closure}{closure}
    ;; groupCount(map?){closure?}{closure?}
    ;; aggregate(collection?,closure?)
    ;; table(table?,strings..?,closures..?)
    ;; tree(map?, closures..?)
    ;; as(string)
    ;; optional(integer)
    ;; optional(string)
    ;; store(collection?,closure?)

    ;; loop(integer){whileClosure}{emitClosure?}
    ;; loop(string){whileClosure}{emitClosure?}
    ;; ifThenElse{ifClosure}{thenClosure}{elseClosure}
    ;; copySplit(pipes...)
    ;; fairMerge
    ;; exhaustMerge





    (testing "chains"
      (is (= (set (map str (-> g
                               (v 1)
                               outE
                               :weight
                               )
                       )) #{"0.5" "0.4" "1.0"}))
      ;; (is (= (set (-> g
      ;;                 (v 1)
      ;;                 (out "knows")
      ;;                 (where #(> (prop % :age) 30))
      ;;                 (out "created")
      ;;                 :name))
      ;;        #{"ripple" "lop"}))

      ;; g.v(1).out('likes').in('likes').out('likes').groupCount(m)

      ;; m = [:]; c = 0;
      ;; g.V.out.groupCount(m).loop(2){c++ < 1000}
      ;; m.sort{a,b -> a.value <=> b.value}
      )
    )
  )
