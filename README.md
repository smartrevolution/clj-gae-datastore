About clj-gae-datastore
=======================

This library provides an easy bridge between Clojure datastructures (structs)
and the Google App Engine datastore by generating all the boilerplate code needed
to access the datastore. For example, to create a simple book database just define your
data as follows:

    (defentity book
      [:key]
      [:title]
      [:author]
      [:publisher]
      [:isbn]
      [:pages]
      [:outofprint
       :pre-save #(if (= % "yes")
                    true
                    false)
       :post-load #(if %
                     "yes"
                     "no")])

The data can be queried via a very simple query language that is also provided in the
library. To find all books by a certain author just use the following code:

    (select (query/where book ([= :author "Peter Norvig"])))

I need to try this!
===================

A quick way to play with the library is from an Emacs Clojure
REPL. Clone the repository and put the following libraries into a
directory `"lib"`.

- clojure.jar
- clojure-contrib.jar
- joda-time-1.6.jar
- some libraries from the Google App Engine SDK. You can use the small script "etc/copy-gae-libs.sh" to 
  do this.

Open the example.clj file in Emacs and use swank-clojure-project to setup a Clojure session. Compile the
file and execute (init-app-engine) in the REPL. Now you should be able to play with the examples.

For more information about setting up Clojure, Emacs and Google App Engine have a look at our
[blog post](http://www.hackers-with-attitude.com/2009/08/intertactive-programming-with-clojure.html).

Creating a library
==================

Just type "ant jar" to create a jar file of the datastore library.

If you want to create a jar of the compiled files type "ant binary-jar". This should be done after copying
the dependent jar files into `"lib"` (see above.)

 
