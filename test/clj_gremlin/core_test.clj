(ns clj-gremlin.core-test
  (:use clj-gremlin.core
        clojure.test)
  (:import (com.tinkerpop.blueprints.impls.tg TinkerGraphFactory)
           (com.tinkerpop.pipes.util.structures Row)))

(defn ids? [left right]
  (= (into #{} (map #(.getId %) left)) right))

(let [g (TinkerGraphFactory/createTinkerGraph)
      v1 (.getVertex g "1")
      v2 (.getVertex g "2")
      v3 (.getVertex g "3")
      v4 (.getVertex g "4")
      v5 (.getVertex g "5")
      v6 (.getVertex g "6")
      e1 (.getEdge g "7")
      e2 (.getEdge g "8")
      e3 (.getEdge g "9")
      e4 (.getEdge g "10")
      e5 (.getEdge g "11")
      e6 (.getEdge g "12")
      ]

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

    (testing "e"
      (is (= (.getId (e g 8)) "8"))
      (is (ids? (e g 11 12) #{"11" "12"}))
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

    (testing "transform"
      (is (= (into #{} (transform (V g) #(prop % :name))) #{"lop" "vadas" "marko" "peter" "ripple" "josh"}))
      (is (= (into #{} (transform
                        (transform (V g) #(prop % :name))
                        count)) #{3 5 6 4}))

      (is (= (into #{} (transform (v g 1) #(prop % :name))) #{"marko"}))
      (is (= (into #{} (transform
                        (transform (v g 1) #(prop % :name))
                        count)) #{5}))
      )

    (testing "id"
      (is (= (into #{} (id (V g))) #{"1" "2" "3" "4" "5" "6"}))

      (is (= (into #{} (id (v g 1))) #{"1"}))
      )

    (testing "label"
      (is (= (into #{} (label (E g))) #{"created" "knows"}))
      (is (= (into #{} (label (e g 10))) #{"created"}))
      )

    (testing "outV"
      (is (ids? (outV (E g)) #{"4" "1" "6"}))
      (is (ids? (outV (e g 10)) #{"4"}))
      )

    (testing "inV"
      (is (ids? (inV (E g)) #{"2" "3" "4" "5"}))
      (is (ids? (inV (e g 10)) #{"5"}))
      )

    (testing "bothV"
      (is (ids? (bothV (E g)) #{"1" "2" "3" "4" "5" "6"}))
      (is (ids? (bothV (e g 10)) #{"4" "5"}))
      )

    (testing "in"
      (is (ids? (in (V g)) #{"1" "4" "6"}))
      (is (ids? (in (v g 5)) #{"4"}))

      (is (ids? (in (V g) "knows") #{"1"}))
      (is (ids? (in (V g) :knows) #{"1"}))
      (is (ids? (in (V g) "knows" "foo") #{"1"}))
      (is (ids? (in (V g) "knows" "created") #{"1" "4" "6"}))

      (is (ids? (in (v g 5) "created") #{"4"}))
      )

    (testing "inE"
      (is (ids? (inE (V g)) #{"9" "11" "12" "7" "10" "8"}))
      (is (ids? (inE (v g 5)) #{"10"}))

      (is (ids? (inE (V g) "knows") #{"7" "8"}))
      (is (ids? (inE (V g) :knows) #{"7" "8"}))
      (is (ids? (inE (V g) "knows" "foo") #{"7" "8"}))
      (is (ids? (inE (V g) "knows" "created") #{"9" "11" "12" "7" "10" "8"}))

      (is (ids? (inE (v g 5) "created") #{"10"}))
      )

    (testing "both"
      (is (ids? (both (V g)) #{"1" "2" "3" "4" "5" "6"}))
      (is (ids? (both (v g 5)) #{"4"}))

      (is (ids? (both (V g) "knows") #{"1" "2" "4"}))
      (is (ids? (both (V g) "knows" "foo") #{"1" "2" "4"}))
      (is (ids? (both (V g) "knows" "created") #{"1" "2" "3" "4" "5" "6"}))
      (is (ids? (both (V g) :knows :created) #{"1" "2" "3" "4" "5" "6"}))

      (is (ids? (both (v g 5) "created") #{"4"}))
      )

    (testing "bothE"
      (is (ids? (bothE (V g)) #{"7" "8" "9" "10" "11" "12"}))
      (is (ids? (bothE (v g 5)) #{"10"}))

      (is (ids? (bothE (V g) "knows") #{"7" "8"}))
      (is (ids? (bothE (V g) "knows" "foo") #{"7" "8"}))
      (is (ids? (bothE (V g) "knows" "created") #{"7" "8" "9" "10" "11" "12"}))
      (is (ids? (bothE (V g) :knows :created) #{"7" "8" "9" "10" "11" "12"}))

      (is (ids? (bothE (v g 5) "created") #{"10"}))
      )

    (testing "gather"
      (is (= (first (gather (V g))) [v3 v2 v1 v6 v5 v4]))
      (is (= (first (gather (V g) #(map (fn [e] (prop e :name)) %))) ["lop" "vadas" "marko" "peter" "ripple" "josh"]))
      (is (= (first (gather (v g 4))) [v4]))
      (is (= (first (gather (v g 4) #(map (fn [e] (prop e :name)) %))) ["josh"]))
      )

    (testing "scatter"
      (is (ids? (scatter (V g)) #{"1" "2" "3" "4" "5" "6"}))
      (is (ids? (scatter (v g 1)) #{"1"}))
      (is (ids? (scatter (gather (V g))) #{"1" "2" "3" "4" "5" "6"}))
      (is (= (seq (scatter (gather (V g) #(take 3 %)))) [v3 v2 v1]))
      )

    (testing "order"
      (is (= (seq (order (:name (V g)))) ["josh" "lop" "marko" "peter" "ripple" "vadas"]))
      (is (= (seq (order (:name (V g)) (fn [l r] (- (count l) (count r))))) ["lop" "josh" "vadas" "marko" "peter" "ripple"]))
      )

    (testing "select"
      (is (= (seq (select (V g))) [(Row.) (Row.) (Row.) (Row.) (Row.) (Row.)]))
      (is (= (-> g
                 V
                 (as "foo")
                 (out "knows")
                 select
                 seq) [(Row. [v1] ["foo"]) (Row. [v1] ["foo"])]))

      (is (= (seq (select (v g 1))) [(Row.)]))
      (is (= (-> g
                 (v 1)
                 (as "foo")
                 (out "knows")
                 select
                 seq) [(Row. [v1] ["foo"]) (Row. [v1] ["foo"])]))

      (is (= (-> g (v 1) (as "a") (out "knows") (as "b") select seq)
             [(Row. [v1 v2] ["a" "b"]) (Row. [v1 v4] ["a" "b"])]))

      (is (= (-> g (v 1) (as "a") (out "knows") (as "b") (select #(prop % :name)) seq)
             [(Row. ["marko" "vadas"] ["a" "b"]) (Row. ["marko" "josh"] ["a" "b"])]))

      (is (= (-> g (v 1) (as "a") (out "knows") (as "b") (select ["a"]) seq)
             [(Row. [v1] ["a"]) (Row. [v1] ["a"])]))

      (is (= (-> g (v 1) (as "a") (out "knows") (as "b") (select ["a"] #(prop % :name)) seq)
             [(Row. ["marko"] ["a"]) (Row. ["marko"] ["a"])]))
      )

    (testing "as"
      (is (ids? (as (V g) "first") #{"1" "2" "3" "4" "5" "6"}))
      (is (ids? (as (v g 1) "something") #{"1"}))
      )

    (testing "memo"
      (is (ids? (-> g (v 1) (as "a") (out "knows") (as "b") (memo "a")) #{"2" "4"}))
      (is (ids? (-> g (v 1) (as "a") (out "knows") (as "b") (memo 1)) #{"2" "4"}))
      (let [m (java.util.HashMap.)
            m2 (java.util.HashMap.)]
        (is (ids? (-> g (v 1) (as "a") (out "knows") (as "b") (memo "a" m)) #{"2" "4"}))
        (is (ids? (-> g (v 1) (as "a") (out "knows") (as "b") (memo 1 m2)) #{"2" "4"}))
        (is (= m  {v1 [v2 v4]}))
        (is (= m2 {v1 [v2 v4]}))
        )
      )

    (testing "path"
      (is (= (-> g V path seq) [[v3] [v2] [v1] [v6] [v5] [v4]]))
      (is (= (-> g (v 1) path seq) [[v1]]))
      (is (= (-> g (v 1) out path seq) [[v1 v2] [v1 v4] [v1 v3]]))

      (is (= (-> g (v 1) out (path #(prop % :age) #(prop % :name)) seq) [[29 "vadas"] [29 "josh"] [29 "lop"]]))
      )


    (testing "where"
      (is (= (-> g V     (where (fn [_] false)) seq) nil))
      (is (= (-> g (v 1) (where (fn [_] false)) seq) nil))

      (is (= (-> g V     (where (fn [_] true)) seq) [v3 v2 v1 v6 v5 v4]))
      (is (= (-> g (v 1) (where (fn [_] true)) seq) [v1]))

      (is (= (-> g V     (where #(= (prop % :lang) "java")) seq) [v3 v5]))
      )

    (testing "at"
      (is (= (-> g V     (at 2) seq) [v1]))
      (is (= (-> g (v 1) (at 0) seq) [v1]))

      (is (= (-> g V     (at 2 4) seq) [v1 v6 v5]))
      (is (= (-> g (v 1) (at 0 2) seq) [v1]))
      )

    (testing "has"
      (is (= (-> g V     (has "name" "marko") seq) [v1]))
      (is (= (-> g V     (has "name" "ola") seq) nil))

      (is (= (-> g (v 1) (has "name" "marko") seq) [v1]))
      (is (= (-> g (v 1) (has "name" "ola") seq) nil))

      (is (= (-> g V     (has "blah" nil) seq) [v3 v2 v1 v6 v5 v4]))

      (is (= (-> g V     (has "age" > 30) seq) [v6 v4]))
      (is (= (-> g V     (has "age" > 32) seq) [v6]))
      (is (= (-> g V     (has "age" >= 32) seq) [v6 v4]))
      (is (= (-> g V     (has "age" = 32) seq) [v4]))
      (is (= (-> g V     (has "age" not= 32) seq) [v3 v2 v1 v6 v5]))
      (is (= (-> g V     (has "age" < 32) seq) [v2 v1]))
      (is (= (-> g V     (has "age" <= 32) seq) [v2 v1 v4]))

      (is (= (-> g (v 4) (has "age" > 30) seq) [v4]))

      )

    (testing "has-not"
      (is (= (-> g V     (has-not "name" "marko") seq) [v3 v2 v6 v5 v4]))
      (is (= (-> g V     (has-not "name" "ola") seq) [v3 v2 v1 v6 v5 v4]))

      (is (= (-> g (v 1) (has-not "name" "marko") seq) nil))
      (is (= (-> g (v 1) (has-not "name" "ola") seq) [v1]))

      (is (= (-> g V     (has-not "blah" nil) seq) nil))

      (is (= (-> g V     (has-not "age" > 30) seq) [v2 v1]))
      (is (= (-> g V     (has-not "age" > 32) seq) [v2 v1 v4]))
      (is (= (-> g V     (has-not "age" >= 32) seq) [v2 v1]))
      (is (= (-> g V     (has-not "age" = 32) seq) [v3 v2 v1 v6 v5]))
      (is (= (-> g V     (has-not "age" not= 32) seq) [v4]))
      (is (= (-> g V     (has-not "age" < 32) seq) [v6 v4]))
      (is (= (-> g V     (has-not "age" <= 32) seq) [v6]))

      (is (= (-> g (v 4) (has-not "age" > 30) seq) nil))
      )

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
    ;; optional(integer)
    ;; optional(string)
    ;; store(collection?,closure?)

    ;; cap  -- can't be done until we have side effects

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
