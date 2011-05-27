(defproject clj-gae-datastore "0.3.2-SNAPSHOT"
  :description "clojure dsl for accessing the appengine datastore"
  :dependencies [[clojure "1.2.0"]
		 [clojure-contrib "1.2.0"]
                 [org.clojars.thnetos/clj-time "0.3.0-SNAPSHOT"]
                 [compojure "0.6.2"]
                 [ring/ring-core "0.3.7"]
                 [com.google.appengine/appengine-api "1.4.2"]
                 [com.google.appengine/appengine-api-labs "1.4.2"]
                 [com.google.appengine/appengine-api-1.0-sdk "1.4.2"]
                 [com.google.appengine/appengine-api-stubs "1.4.2"]]
  :dev-dependencies [[swank-clojure "1.2.1"]
                     [com.google.appengine/appengine-local-runtime "1.4.2"]
                     [ring/ring-jetty-adapter "0.3.7"]])
