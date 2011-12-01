(defproject clj-gae-datastore "0.4.1"
  :description      "clojure dsl for accessing the appengine datastore"
  :dependencies     [[clojure "1.2.0"]
                     [clojure-contrib "1.2.0"]
                     [clj-time "0.3.0"]
                     [compojure "0.6.2"]
                     [ring/ring-core "0.3.7"]
                     [com.google.appengine/appengine-api-labs "1.5.4"]
                     [com.google.appengine/appengine-api-1.0-sdk "1.5.4"]
                     [com.google.appengine/appengine-api-stubs "1.5.4"]]
  :dev-dependencies [[com.google.appengine/appengine-local-runtime "1.5.4"]
                     [ring/ring-jetty-adapter "0.3.7"]])
