(ns clj-gremlin.core-test
  (:use clj-gremlin.core
        clojure.test)
  (:import (com.tinkerpop.blueprints.impls.tg TinkerGraphFactory)
           (com.tinkerpop.pipes.util.structures Row
                                                Table
                                                Tree)
           (java.util ArrayList
                      HashMap)))

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

    (testing "back"
      (is (= (-> g (v 1) (as "here") out (back "here") seq) [v1]))
      (is (= (-> g V (as "here") out (back "here") seq) [v1 v6 v4]))

      (is (= (-> g V out (where #(= (prop % :lang) "java")) (back 1) seq) [v3 v3 v5 v3]))
      (is (= (-> g (v 4) out (where #(= (prop % :lang) "java")) (back 1) seq) [v5 v3]))
      )

    (testing "_"
      (is (ids? (_ (V g)) #{"3" "2" "1" "6" "5" "4"}))
      (is (ids? (_ (v g 1)) #{"1"}))
      )

    (testing "random"
      (is (#{v1 v2 v3 v4 v5 v6} (first (-> g V (random 1.0)))))
      (is (= v1 (first (-> g (v 1) (random 1.0)))))
      )

    (testing "except"
      (is (= (-> g V (except [v2 v3]) seq) [v1 v6 v5 v4]))
      (is (= (-> g (v 1) (except [v2 v3]) seq) [v1]))
      (is (= (-> g (v 1) (except [v1]) seq) nil))
      )

    (testing "retain"
      (is (= (-> g V (retain [v2 v3]) seq) [v3 v2]))
      (is (= (-> g (v 1) (retain [v1 v2]) seq) [v1]))
      (is (= (-> g (v 1) (retain [v3 v2]) seq) nil))
      )

    (testing "interval"
      (is (= (-> g (v 1) outE (interval "weight" 0.0 0.6) inV seq) [v2 v3]))
      (is (= (-> g (v 1) (interval :age 20 30) seq) [v1]))
      )

    (testing "dedup"
      (is (= (-> g V dedup seq) [v3 v2 v1 v6 v5 v4]))
      (is (= (-> g (v 1) dedup seq) [v1]))

      (is (= (-> g V out dedup seq) [v2 v4 v3 v5]))
      (is (= (-> g V both dedup seq) [v1 v4 v6 v2 v3 v5]))

      (is (= (-> g V out (dedup #(prop % :lang)) seq) [v2 v3]))
      )

    (testing "side-effect"
      (let [a (atom nil)]
        (doall (seq (-> g (v 1) (side-effect (fn [it] (swap! a (fn [_] it)))) :name)))
        (is (= @a v1))
        (doall (seq (-> g V (side-effect (fn [it] (swap! a (fn [_] it)))) :name)))
        (is (= @a v4))
        )
      )

    (testing "group"
      (let [m (HashMap.)]
        (doall (-> g V (group m #(prop % :lang) #(prop % :name)) seq))
        (is (= m {"java" ["lop" "ripple"] nil ["vadas" "marko" "peter" "josh"]}))
        )
      (let [m (HashMap.)]
        (doall (-> g (v 1) (group m #(prop % :lang) #(prop % :name)) seq))
        (is (= m {nil ["marko"]}))
        )
      )

    (testing "group-count"
      (let [m (HashMap.)]
        (doall (-> g V (out "created") (group-count m #(prop % :name)) seq))
        (is (= m {"lop" 3 "ripple" 1}))
        )

      (let [m (HashMap.)]
        (doall (-> g (v 1) (out "created") (group-count m #(prop % :name)) seq))
        (is (= m {"lop" 1}))
        )

      (let [m (HashMap.)]
        (doall (-> g V (out "created") (group-count m #(prop % :name) (fn [a b] (+ b 2))) seq))
        (is (= m {"lop" 6 "ripple" 2}))
        )

      (let [m (HashMap.)]
        (doall (-> g (v 1) (out "created") (group-count m #(prop % :name) (fn [a b] (+ b 2))) seq))
        (is (= m {"lop" 2}))
        )
      )


    (testing "aggregate"
      (let [x (ArrayList.)]
        (doall (-> g (v 1) (aggregate x) (out "created") (in "created") (except x) seq))
        (is (= x [v1]))
        )

      (let [x (ArrayList.)]
        (doall (-> g V (aggregate x) seq))
        (is (= x [v3 v2 v1 v6 v5 v4]))
        )

      (let [x (ArrayList.)]
        (doall (-> g V (aggregate x #(prop % :name)) seq))
        (is (= x ["lop" "vadas" "marko" "peter" "ripple" "josh"]))
        )

      (let [x (ArrayList.)]
        (doall (-> g (v 1) (aggregate x #(prop % :name)) seq))
        (is (= x ["marko"]))
        )
      )

    (testing "table"
      (let [t (Table.)]
        (doall (-> g (v 1) (table t) seq))
        (is (= t [[]]))
        )

      (let [t (Table.)]
        (doall (-> g (v 1) (as "a") out :name (as "b") (table t) seq))
        (is (= t [(Row. [v1 "vadas"] ["a" "b"]) (Row. [v1 "josh"] ["a" "b"]) (Row. [v1 "lop"] ["a" "b"])]))
        )

      (let [t (Table.)]
        (doall (-> g (v 1) (as "a") out (as "b") (table t #(prop % :name)) seq))
        (is (= t [(Row. ["marko" "vadas"] ["a" "b"]) (Row. ["marko" "josh"] ["a" "b"]) (Row. ["marko" "lop"] ["a" "b"])]))
        )

      (let [t (Table.)]
        (doall (-> g (v 1) (as "a") out :name (as "b") (table t #(prop % :name) #(.length %)) seq))
        (is (= t [(Row. ["marko" (int 5)] ["a" "b"]) (Row. ["marko" (int 4)] ["a" "b"]) (Row. ["marko" (int 3)] ["a" "b"])]))
        )
      )

    (testing "tree"
      (let [m (Tree.)]
        (doall (-> g (v 1) (tree m) seq))
        (is (= m {v1 {}}))
        )

      (let [m (Tree.)]
        (doall (-> g (v 1) out out (tree m) seq))
        (is (= m {v1 {v4 {v3 {} v5 {}}}}))
        )

      (let [m (Tree.)]
        (doall (-> g (v 1) (tree m #(prop % :name)) seq))
        (is (= m {"marko" {}}))
        )

      (let [m (Tree.)]
        (doall (-> g (v 1) out out (tree m #(prop % :name)) seq))
        (is (= m {"marko" {"josh" {"lop" {} "ripple" {}}}}))
        )

      (let [m (Tree.)]
        (doall (-> g (v 1) out out (tree m #(prop % :name) #(prop % :age)) seq))
        (is (= m {"marko" {(int 32) {"ripple" {}}} (int 29) {"josh" {nil {}}}}))
        )
      )

    (testing "optional"
      (is (= (-> g V (as "a") out (optional "a") seq) [v3 v2 v1 v6 v5 v4]))
      (is (= (-> g (v 1) (as "a") out (optional "a") seq) [v1]))

      (is (= (-> g V (as "a") out (optional 1) seq) [v3 v2 v1 v6 v5 v4]))
      (is (= (-> g (v 1) (as "a") out (optional 1) seq) [v1]))
      )

    (testing "store"
      (let [a (ArrayList.)]
        (doall (-> g V (store a) seq))
        (is (= a [v3 v2 v1 v6 v5 v4]))
        )

      (let [a (ArrayList.)]
        (doall (-> g (v 1) (store a) seq))
        (is (= a [v1]))
        )

      (let [a (ArrayList.)]
        (doall (-> g V (store a #(prop % :name)) seq))
        (is (= a ["lop" "vadas" "marko" "peter" "ripple" "josh"]))
        )

      (let [a (ArrayList.)]
        (doall (-> g (v 1) (store a #(prop % :name)) seq))
        (is (= a ["marko"]))
        )
      )

    (testing "looping"
      (is (= (-> g (v 1) out (looping 1 #(< (.getLoops %) 3)) :name seq) ["ripple" "lop"]))
      (is (= (-> g (v 1) (as "here") out (looping "here" #(< (.getLoops %) 3)) :name)))

      (is (= (-> g V out (looping 1 #(< (.getLoops %) 3) (fn [_] true)) :name seq) ["vadas" "josh" "lop" "ripple" "lop" "lop" "ripple" "lop"]))
      (is (= (-> g V out (looping 1 #(< (.getLoops %) 3) (fn [_] false)) :name seq) nil))
      )

    (testing "if-then-else"
      (is (= (-> g (v 1) out (if-then-else #(= (prop % :lang) "java") identity out) :name seq) ["ripple" "lop" "lop"]))
      (is (= (-> g (v 1) (if-then-else #(= (prop % :lang) "java") identity out) :name seq) ["vadas" "josh" "lop"]))
      )


    (testing "copy-split and fair-merge and exhaust-merge"
      (is (= (-> g V (copy-split (-> (_) outE) (-> (_) inE)) fair-merge seq) [e1 e3 e2 e5 e3 e6 e6 e1 e4 e4 e5 e2]))
      (is (= (-> g (v 1) (copy-split (-> (_) outE) (-> (_) inE)) fair-merge seq) [e1 e2 e3]))

      (is (= (-> g (v 1) out (copy-split (-> (_) :name) (-> (_) :age)) fair-merge seq) ["vadas" 27 "josh" 32 "lop" nil]))
      (is (= (-> g (v 1) out (copy-split (-> (_) :name) (-> (_) :age)) exhaust-merge seq) ["vadas" "josh" "lop" 27 32 nil]))
      )

    (testing "cap"
      (is (= (-> g V (group #(prop % :lang) #(prop % :name)) cap seq) [{nil ["vadas" "marko" "peter" "josh"] "java" ["lop" "ripple"]}]))

      (is (= (-> g V (out "created") group-count cap seq) [{v3 3 v5 1}]))
      (is (= (-> g V (out "created") (group-count #(prop % :name)) cap seq) [{"lop" 3 "ripple" 1}]))

      (is (= (-> g V aggregate cap seq) [[v3 v2 v1 v6 v5 v4]]))
      (is (= (-> g V (aggregate #(prop % :name)) cap seq) [["lop" "vadas" "marko" "peter" "ripple" "josh"]]))

      (is (= (-> g (v 1) table cap seq) [[[]]]))
      (is (= (-> g (v 1) (as "a") out :name (as "b") table cap seq) [[(Row. [v1 "vadas"] ["a" "b"]) (Row. [v1 "josh"] ["a" "b"]) (Row. [v1 "lop"] ["a" "b"])]]))
      (is (= (-> g (v 1) (as "a") out (as "b") (table #(prop % :name)) cap seq) [[(Row. ["marko" "vadas"] ["a" "b"]) (Row. ["marko" "josh"] ["a" "b"]) (Row. ["marko" "lop"] ["a" "b"])]]))
      (is (= (-> g (v 1) (as "a") out :name (as "b") (table #(prop % :name) #(.length %)) cap seq) [[(Row. ["marko" (int 5)] ["a" "b"]) (Row. ["marko" (int 4)] ["a" "b"]) (Row. ["marko" (int 3)] ["a" "b"])]]))

      (is (= (-> g (v 1) tree cap seq) [{v1 {}}]))
      (is (= (-> g (v 1) out out tree cap seq) [{v1 {v4 {v3 {} v5 {}}}}]))
      (is (= (-> g (v 1) (tree #(prop % :name)) cap seq) [{"marko" {}}]))
      (is (= (-> g (v 1) out out (tree #(prop % :name)) cap seq) [{"marko" {"josh" {"lop" {} "ripple" {}}}}]))
      (is (= (-> g (v 1) out out (tree #(prop % :name) #(prop % :age)) cap seq) [{"marko" {(int 32) {"ripple" {}}} (int 29) {"josh" {nil {}}}}]))

      (is (= (-> g (v 1) (store #(prop % :name)) cap seq) [["marko"]]))
      )

    (testing "simple-path"
      (is (= (-> g V out simple-path (looping 2 #(< (.getLoops %) 5)) seq) nil))
      )

    (testing "enable-path"
      (is (= (-> g (v 1) out (looping 1 #(< (.getLoops %) 3) #(.contains (.getPath %) (v g 4))) enable-path seq) [v5 v3]))
      )

    (testing "chains"
      (is (= (set (map str (-> g
                               (v 1)
                               outE
                               :weight
                               )
                       )) #{"0.5" "0.4" "1.0"}))
      (is (= (-> g
                 (v 1)
                 (out "knows")
                 (where #(> (prop % :age) 30))
                 (out "created")
                 :name
                 set)
             #{"ripple" "lop"}))


      (is (= (-> g
                 (v 1)
                 (out "knows")
                 (in "knows")
                 (out "knows")
                 group-count
                 cap
                 seq)
             [{v2 (int 2) v4 (int 2)}]))

      (let [m (HashMap.)
            c (atom 0)]
        (-> g
            V
            out
            (group-count m)
            (looping 2 (fn [_] (< (swap! c inc) 1000)))
            seq
            doall)
        (is (= (sort-by (fn [[k v]] v) (into {} m)) [[v2 (int 1)]
                                                     [v4 (int 1)]
                                                     [v5 (int 2)]
                                                     [v3 (int 4)]]))
      ))
    )
  )
