(defproject org.clojars.choas/clj-gae-datastore "0.1"
  :description "A Clojure mini-language for the Google App Engine Datastore."
  :url "http://github.com/choas/clj-gae-datastore"
  :dependencies [[org.clojure/clojure "1.2.0-master-SNAPSHOT"]
                 [org.clojure/clojure-contrib "1.2.0-SNAPSHOT"]
		 [joda-time "1.6"]
                 [com.google.appengine/appengine-api-1.0-sdk "1.3.2"]
                 [com.google.appengine/appengine-tools-sdk "1.3.0"]
		 [leiningen/lein-swank "1.1.0"]]
  :dev-dependencies [[lein-clojars "0.5.0"]
		     [com.google.appengine/appengine-api-1.0-sdk "1.3.2"]
		     [com.google.appengine/appengine-api-labs "1.3.2"]
		     [com.google.appengine/appengine-api-stubs "1.3.2"]
		     [com.google.appengine/appengine-local-runtime "1.3.2"]
		     [com.google.appengine/appengine-testing "1.3.2"]]
  :source-path  "src/main/clojure"
  )
