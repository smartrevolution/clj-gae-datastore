About clj-gae-datastore
=======================

About this version:
-------------------

- forked smartrevolution/clj-gae-datastore

- added leiningen support: lein jar

- uploaded to clojars

- [org.clojars.choas/clj-gae-datastore "0.1"]


Building
--------

Get all dependencies and build the jar:

    lein deps true
    lein jar


----

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
       :pre-save #(= % "yes")
       :post-load #(if %
                     "yes"
                     "no")])

Storing Data
------------

Storing a book in the datastore can be done via `store-entities!`.

    com.freiheit.gae.datastore.example> (store-entities! [(make-book :title "Paradigms of Artificial Intelligence Programming: Case Studies in Common Lisp"    		   
                                                                     :author "Peter Norvig"
                                                                     :publisher "Morgan Kaufmann"
                                                                     :isbn "978-1558601918"
                                                                     :pages 946
                                                                     :outofprint "no")])
result:

    ({:outofprint "no", :pages 946, :isbn "978-1558601918", :publisher "Morgan Kaufmann", :author "Peter Norvig", :title "Paradigms of Artificial Intelligence Programming: Case Studies in Common Lisp", :key #<Key book(1)>, :parent-key nil, :kind "book"})

Querying the Datastore
----------------------

The data can be queried via a very simple query language that is also provided in the
library. To find all books by a certain author just use the following code:

    com.freiheit.gae.datastore.example> (select (where book ([= :author "Peter Norvig"])))

result:

    ({:outofprint "no", :pages 946, :isbn "978-1558601918", :publisher "Morgan Kaufmann", :author "Peter Norvig", :title "Paradigms of Artificial Intelligence Programming: Case Studies in Common Lisp", :key #<Key book(1)>, :parent-key nil, :kind "book"})

Changing Data in the Datastore
------------------------------

Of course you need to able to change data that is already in the datastore. The API contains a function called `assoc-entity` that can be used to change the values of an entity. It also does some bookkeeping that marks an entity as changed. Please note that `assoc-entity` doesn't change the entity in the datastore. This is done with `update-entities!`.

     com.freiheit.gae.datastore.example> (let [book (first (select (where book ([= :author "Peter Norvig"]))))
                                               unavailable-book (assoc-entity book :outofprint "yes")]
                                              (update-entities! [unavailable-book]))

result:
     
    ({:outofprint "yes", :pages 946, :isbn "978-1558601918", :publisher "Morgan Kaufmann", :author "Peter Norvig", :title "Paradigms of Artificial Intelligence Programming: Case Studies in Common Lisp", :key #<Key book(1)>, :parent-key nil, :kind "book"})

Deleting Data in the Datastore
------------------------------

Data that needs to be deleted in the datastore is identified by its key. So, in order to delete all books in the datastore just get the keys of all books in the datastore and call `delete-all!`.

    com.freiheit.gae.datastore.example> (let [keys (map :key (select (where book [])))]
                                             (delete-all! keys))

result:

    ()

A query for all books verifies that indeed all entities are removed from the datastore:

    com.freiheit.gae.datastore.example> (select (where book []))

result:

    ()

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

Open the `src/example/clojure/com/freiheit/gae/datastore/example.clj` file in Emacs and use 
swank-clojure-project to setup a Clojure session. Compile the
file and execute (init-app-engine) in the REPL. Now you should be able to play with the examples.

For more information about setting up Clojure, Emacs and Google App Engine have a look at our
[blog post](http://www.hackers-with-attitude.com/2009/08/intertactive-programming-with-clojure.html).

Creating a library
==================

Just type `ant jar` to create a jar file of the datastore library.

If you want to create a jar of the compiled files type "ant binary-jar". This should be done after copying
the dependent jar files into `"lib"` (see above.)

Generate API documentation
==========================

You can generate API documentation by typing `ant doc`. It will be available in `target/docs/api.html`.