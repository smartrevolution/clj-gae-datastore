(ns com.freiheit.gae.datastore.example  
  (:use
   [clojure.contrib def])
  (:require 
   [com.freiheit.gae.datastore.datastore-access-dsl :as datastore]
   [com.freiheit.gae.datastore.datastore-query-dsl :as query]
   [com.freiheit.gae.datastore.datastore-types :as types])
  (:import
   [com.google.apphosting.api ApiProxy ApiProxy$Environment]
   [java.io File]
   [java.util HashMap]
   [com.google.appengine.tools.development ApiProxyLocalFactory ApiProxyLocalImpl LocalServiceContext LocalServerEnvironment]
   [com.google.appengine.api.labs.taskqueue.dev LocalTaskQueue]))

(datastore/defentity book
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


(def *books*
  (list (make-book :title "On Lisp"
		   :author "Paul Graham"
		   :publisher "Prentice Hall"
		   :isbn "978-0130305527"
		   :pages 413
		   :outofprint "yes")
	(make-book :title "Paradigms of Artificial Intelligence Programming: Case Studies in Common Lisp"
		   :author "Peter Norvig"
		   :publisher "Morgan Kaufmann"
		   :isbn "978-1558601918"
		   :pages 946
		   :outofprint "no")
	(make-book :title "Programming Clojure"
		   :author "Stuart Halloway"
		   :publisher "Pragmatic Programmers"
		   :isbn "978-1934356333"
		   :pages 304
		   :outofprint "no")))

(defn save-books-to-datastore
  []
  (datastore/store-entities! *books*))

(defn load-books-from-datastore
  []
  (query/select (query/where book ([= :author "Peter Norvig"]))))

(defn init-app-engine
  "Initialize the app engine services. Call it once from the REPL"
  ([]
     (init-app-engine "/tmp"))
  ([dir]
     (let [default-port 9090
	   env-proxy (proxy [ApiProxy$Environment] []
		       (isLoggedIn [] false)
		       (getRequestNamespace [] "")
		       (getDefaultNamespace [] "")
		       (getAttributes [] (let [attributes (HashMap.)
					       local-server-url (str "http://localhost:" default-port)]
					   (.put attributes "com.google.appengine.server_url_key" local-server-url)
					   attributes))
		       (getAppId [] "local"))
	   local-env (proxy [LocalServerEnvironment] []
		       (getAppDir [] (File. dir))
		       (getAddress [] "localhost")
		       (getPort [] default-port)
		       (waitForServerToStart [] nil))
	   api-proxy (.create (ApiProxyLocalFactory.) local-env)]
	  (do
	    (com.google.apphosting.api.ApiProxy/setEnvironmentForCurrentThread env-proxy)
	    (com.google.apphosting.api.ApiProxy/setDelegate api-proxy)))))
