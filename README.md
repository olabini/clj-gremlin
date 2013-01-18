# clj-gremlin

Gremlin is a language/framework for traversing and querying graph databases. The original implementation is written in Groovy, and you can read all about it here: http://gremlin.tinkerpop.com. clj-gremlin is an implementation of Gremlin for Clojure. We try to keep as close as possible to the Groovy implementation, so that you can read the original documentation and apply those lessons to the Clojure implementation. That said, there are a few things that will be different, and this page will document them.

clj-gremlin is available through Clojars, so in order to use it you can simply add this to your lein project.xml file:

```clojure
:dependencies [[clj-gremlin "0.0.3"]]
```

In order to get access to all the steps and helper functions, use the clj-gremlin.core namespace:

```clojure
(:use clj-gremlin.core)
```

## Simple usage

When showing examples, we will be using the variable g to stand for the current Graph. This should be an instance of the Graph interface from http://blueprints.tinkerpop.com. Most of the examples use the threading operator -> to make them read nicely. The return value of most query steps will be pipelines, which are all Iterable. This means they can be treated as seqable as well, from a clojure standpoint.

Getting all the vertices from the graph is as simple as

```clojure
(V g)
```

or

```clojure
(-> g V)
```

Same thing to get all the edges:

```clojure
(-> g E)
```

To get a specific vertice or edge with an id:

```clojure
(-> g (v 42))
(-> g (e 13))
```

In order to add more operations, just put them as function calls in the threading operator:

```clojure
(-> g (v 42) (out "knows") inV)
```

## Things that are different

### Getting properties
There are two ways to get properties in clj-gremlin. Neither matches exactly to the way the Groovy version does it. In order to extract a property as part of a pipeline, you just use a symbol as part of the pipeline chain:

```clojure
(-> g V :name)
```

However, if you are dealing with a single element, you need to instead use the prop function:

```clojure
(-> g V (step #(prop % :name))
```

### map -> props
In order to extract all the properties as part of a chain, the original implementation of Gremlin uses the method map. However, in Clojure, we can't really take that word, since it's used for other things in Clojure. So instead of map, we use props:

```clojure
(-> g V props)
```

### memoize -> memo
Since memoize is already a clojure.core function, memoize becomes memo in Gremlin.

### filter -> where
Filter is also one of those very common operations in Clojure, so the name for "filter" is "where" in the Clojure implementation.

### [] -> at
Square brackets is pretty clunky for use as a function in Clojure. Instead, this implementation just uses at.

```clojure
(-> g V (at 2))
```

It takes two arguments for the range version:

```clojure
(-> g V (at 2 4))
```

### loop -> looping
Loop is another already used word in Clojure. looping became the translation, although it feels pretty clunky. Better alternatives would be great!

### groupBy -> group
In this case, the natural thing would have been to create a group-by function. However, that one is taken and well used, so grouping is done with "group" instead.

### Argument to order and second group-count closure
The order step takes a required closure. In the Groovy implementation it yields a Pair object to the closure. In the Clojure version I instead decided to yield the two objects as separate arguments. The same thing happens with the potential second closure given to group-count.

### Name differences for consistency with Clojure style:
Because Clojure usually doesn't use camel cased names, but instead separates names with dashes, several functions got a change in name for the sake of consistency:

* hasNot -> has-not
* sideEffect -> side-effect
* groupCount -> group-count
* ifThenElse -> if-then-else
* copySplit -> copy-split
* fairMerge -> fair-merge
* exhaustMerge -> exhaust-merge
* simplePath -> simple-path
* enablePath -> enable-path
