(defproject clj-gae-datastore "0.3-SNAPSHOT"
  :description "clojure dsl for accessing the appengine datastore"
  :dependencies [[clojure "1.2.0"]
		 [clojure-contrib "1.2.0"]
		 ;FIXME: clj-gae-datastore and clj-fdc-commons depend on each other!, SR
                 [clj-fdc-commons "0.2-SNAPSHOT"]]
  :dev-dependencies [[swank-clojure "1.2.1"]])
