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
  [:title :unindexed true] ;TODO: :unindexed is not evaluated at the moment, but can be specified
  [:author]
  [:publisher]
  [:isbn]
  [:pages]
  [:outofprint
   :pre-save #(= % "yes")
   :post-load #(if %
		 "yes"
		 "no")])

(datastore/defentity person
  [:key]
  [:name] 
  [:books])

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
  "Stores all books from the var *books* to the datastore."
  []
  (datastore/store-entities! *books*))

(defn load-all-books-from-datastore
  "Loads all books from the datastore."
  []
  (query/select (query/where book [])))

(defn load-norvig-books-from-datastore
  "Load all books from the datastore, where the author is 'Peter Norvig'"
  []
  (query/select (query/where book ([= :author "Peter Norvig"]))))

(defn save-person-to-datastore
  "Stores some users into the datastore"
  []
  (datastore/store-entities! [(make-person :books (map :key (load-norvig-books-from-datastore)))]))

(defn load-all-persons-from-datastore
  "Load all persons from the datastore"
  []
  (query/select (query/where person [])))

(defn load-all-persons-with-books
  "Load all persons and resolve the book keys"
  []
  (query/resolve-entities (load-all-persons-from-datastore) :books))

(defn change-book-in-datastore
  "Renames the author of the first book in the datastore to 'P. Graham'"
  []
  (let [book (first (load-all-books-from-datastore))]
    (datastore/update-entities! [(datastore/assoc-and-track-changes book :author "P. Graham")])))

(defn delete-books-from-datastore
  "Cleans the datastore by selecting the keys of all books and then using these keys 
to delete the data."
  []
  (let [keys (query/select-only-keys (query/where book []))]
    (datastore/delete-all! keys)))

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
