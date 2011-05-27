(ns com.freiheit.gae.datastore.dsl
  #^{:doc "A mini-language to access the Google AppEngine datastore."}
  (:use [clojure.contrib def ns-utils])
  (:require [clojure.contrib.seq-utils :as seq-utils])
  (:import [com.google.appengine.api.datastore
            DatastoreServiceFactory DatastoreService Entity Key KeyFactory
	    Query Query$FilterOperator
	    EntityNotFoundException FetchOptions FetchOptions$Builder Cursor]
	   [java.util Date]))


;;;; -------------------------------------------------------------------------
;;;; Implementation of new Datastore DSL based on Clojure Protocols
;;;; -------------------------------------------------------------------------

(defn- validate-field-option
  "Returns the option, if it is valid or throws and IllegalArgumentException"
  [option]
  (if (or (= option :pre-save)
	  (= option :post-load)
	  (= option :unindexed))
    option
    (throw (IllegalArgumentException.
	    (str "This option is not valid (Try :pre-save, :post-load or :unindexed instead): " option)))))


(defn- destructure-field-clause
  "[:field :pre-save #pre-save-func :post-load #post-load-func :unindexed bool]
--> {:attr-name :field, :pre-save #pre-save-func, :post-load #post-load-func :unindexed bool}"
  [clause]
  (if (and (> (count clause) 0)
	   (odd? (count clause)))
    (let [mapped-clause {:attr-name (keyword (first clause))}
	  options (partition 2 (rest clause))]
      (reduce (fn [left right]
		(assoc left (validate-field-option (keyword (first right))) (last right)))
	      mapped-clause
	      options))
    (throw (IllegalArgumentException.
	    (str "Malformed clause: " clause)))))


(defn- transform-clauses
  "list of {:attr-name :field, :pre-save #pre-save-func, :post-load #post-load-func, :unindexed bool}
--> list of {:field {:pre-save #pre-save-func, :post-load #post-load-func, :unindexed bool}}"
  [clauses]
  (map #(destructure-field-clause %) clauses))


(defn- build-lookup-table
  "list of {:field {:pre-save #pre-save-func, :post-load #post-load-func, :unindexed bool}}
--> one single hashmap of {:field1 {:pre-save #pre-save-func :post-load #post-load-func :unindexed bool}, ... :field2 {...}}"
  [clauses]
  (reduce (fn [left right]
            (assoc left (:attr-name right) (dissoc right :attr-name)))
          {}
          (transform-clauses clauses)))


(defn lookup
  "Looks up the options from the defentity clauses"
  [lookup-table attr option val-if-not-exists]
  (if-let [result (-> lookup-table attr option)]
    result
    val-if-not-exists))


(defn make-empty-entity
  ([^java.lang.String kind]
     (Entity. kind))
  ([^java.lang.String kind ^java.lang.String name]
     (Entity. (KeyFactory/createKey kind name)))
  ([^com.google.appengine.api.datastore.Entity parent ^java.lang.String kind ^java.lang.String name]
     (Entity. (KeyFactory/createKey parent kind name))))

(defprotocol Datastore
  (to-entity [this] "Converts this to a GAE Datastore Entity")
  (set-parent [this parent] "Set the parent Entity")
  (get-parent [this] "Get the parent Entity")
  (get-kind [this] "Returns the :kind of this Entity"))


(defmulti from-entity
  "Creates the corresponding Clojure record (defined with defentity) from a GAE Entity"
  (fn [^com.google.appengine.api.datastore.Entity entity]
    (.getKind entity)))


(defmacro defentity
  "Define the schema for Google App Engine datastore entity.
Syntax: (defentity <entity-name> 
           [:attr-name1 :pre-save #fn :post-load #fn :unindexed bool]
           [:attr-name2 ...]...
           :pre-save #fn
           :post-load #fn)"
  [entity-name & body]
  (let [options (apply hash-map (remove vector? body)) ;a list of everything else but the attribute clauses 
	attributes (take-while vector? body) ;a list of the attribute clauses
	attr-list (map first attributes) ;a list of the attribute names
        lookup-table (build-lookup-table attributes) ; hash-map with attr-name as key and attr-options as val
        kind (str entity-name)]
    `(do
       (defrecord ~entity-name [~@(map #(symbol (name %)) attr-list)]
	 Datastore
	 (to-entity
	  [this#]
	  (let [entity#
		(if (empty? (:key this#))
		  (Entity. ~kind)
		  (try
		   (Entity. (KeyFactory/stringToKey (:key this#)))
		   (catch java.lang.IllegalArgumentException _
		     (Entity. (keyFactory/createKey ~kind (:key this#))))))
		;;foreach complete clojure record: call 'global' pre-save fn (and store result in 'that')
		that# (if-let [presave-fn# (:pre-save ~options)]
			(presave-fn# this#)
			this#)]
	    (doseq [[attr-name# value#] (dissoc that# :key)]
	      ;;foreach item in record: 1.) call pre-save fn 2.) store attribute unindexed or indexed
	      (let [unindexed-flag# (lookup ~lookup-table attr-name# :unindexed false) 
		    pre-save-fn# (lookup ~lookup-table attr-name# :pre-save identity)]
		(if unindexed-flag#
		  (.setUnindexedProperty entity# (name attr-name#) (pre-save-fn# value#))
		  (.setProperty entity# (name attr-name#) (pre-save-fn# value#)))))
	    entity#))
	 (set-parent
	  [this# parent#]
	  (with-meta this# {:parent_ parent#}))
	  ;(with-meta this# {~(keyword (gensym "parent")) parent#})) 
	 (get-parent
	  [this#]
	  (:parent_ (meta this#)))
	 (get-kind
	  [this#]
	  ~kind))
       (defmethod from-entity ~kind
	 [entity#]
	 (let [entity-as-map#
	       (reduce #(assoc %1 (keyword (key %2))
			       ((lookup ~lookup-table (keyword (key %2)) :post-load identity) (val %2)))
		       {:kind ~kind :key (KeyFactory/keyToString (.getKey entity#))}
		       #_{:kind (.getKind entity#) :key (.getKey entity#)}
		       (.entrySet (.getProperties entity#)))
	       {:keys [~@(map #(symbol (name %)) attr-list)]} entity-as-map#]
	   (let [record# (new ~entity-name ~@(map #(symbol (name %)) attr-list))]
	     (if-let [postload-fn# (:post-load ~options)]
	       (postload-fn# record#)
	       record#))))
       (class ~entity-name))))


(defn- dss
  "Get the DatastoreService"
  []
  (com.google.appengine.api.datastore.DatastoreServiceFactory/getDatastoreService))


(defn save!
  "Saves the data objects into the datastore and returns a copy of the original input with the newly generated keys from the datastore"
  [data]
  (let [data-coll (if (coll? data)
		    data
		    (list data))
	one-or-more-entities (map to-entity data-coll)]
    (map #(assoc %1 :key %2)
	 data-coll
	 (.put (dss) one-or-more-entities))))


(defn- fetch-options
  "Create fetch options for a query with the given limit and an optional websafe cursor.
  The cursor is ignored if it is nil."
  ([#^Number limit]
     (fetch-options limit nil))
  ([#^Number limit #^String cursor]
     (let [limit-options (FetchOptions$Builder/withLimit limit)]
       (if (nil? cursor)
         limit-options
         (->> (Cursor/fromWebSafeString cursor)
              (.cursor limit-options))))))

(defvar- *default-fetch-options* (fetch-options 1000))


(defn- execute-query
  "Execute the datastore query. Uses the default fetch options if none are given."
  ([#^Query query]
     (execute-query query *default-fetch-options*))
  ([#^Query query #^FetchOptions fetch-options]
     (. (.prepare ^com.google.appengine.api.datastore.DatastoreService (dss) query)
	asQueryResultList fetch-options)))


(defn select
  [#^Query query]
  (map from-entity (-> query
		       execute-query)))

(defn select-only-keys
  [#^Query query]
  (map #(.getKey %) (-> query
			(.setKeysOnly)
			execute-query)))


(defmacro where
  "Create a query for a given kind.
  TODO: run the query-parameter-values through translate-to-datastore.
  When the kind is nil, then a kindless-query will be generated.
  Ex: (where person [[= :person-password-hash \"123\"]])"
  ([kind queries]
     `(where nil ~kind ~queries))
  ([parent-key kind queries]
     (let [kindless? (nil? kind)
	   op-smap {'= 'com.google.appengine.api.datastore.Query$FilterOperator/EQUAL
                    'not= 'com.google.appengine.api.datastore.Query$FilterOperator/NOT_EQUAL
                    '< 'com.google.appengine.api.datastore.Query$FilterOperator/LESS_THAN
                    '<= 'com.google.appengine.api.datastore.Query$FilterOperator/LESS_THAN_OR_EQUAL
                    '> 'com.google.appengine.api.datastore.Query$FilterOperator/GREATER_THAN
                    '>= 'com.google.appengine.api.datastore.Query$FilterOperator/GREATER_THAN_OR_EQUAL
                    'in 'com.google.appengine.api.datastore.Query$FilterOperator/IN}
	   op-queries (map (fn [query] (replace op-smap query)) queries)]
       `(let [q# ~(if kindless?
                    `(com.google.appengine.api.datastore.Query.)
                    `(com.google.appengine.api.datastore.Query. ~(name kind)))]
	  (when ~parent-key
            (.setAncestor q# ~parent-key))
	  (doseq [[operator# prop# value#] ~(vec op-queries)]
	    (. q# addFilter (name prop#) (eval operator#) value#))
	  q#))))

(defn delete-all!
  "Delete all entities specified by the given keys from the datastore."
  [#^java.util.Collection keys]
  (let [service (DatastoreServiceFactory/getDatastoreService)
        max-batch-size 500]
    (dorun
     (for [#^Key key-batch (partition-all max-batch-size keys)]
       (.delete service key-batch)))))


(defn make-key
  "Create a key for an entity. Can either be a websafe keystring, an appengine key (which
   will be returned unchanged) or a kind and a numeric id."
  ([key]
     (cond
      (= Key (class key)) key
      true (KeyFactory/stringToKey key)))
  ([kind key]
     (let [typed-key (if (= (class key) java.lang.Integer) 
		       (long key) 
		       key )]
       (KeyFactory/createKey kind typed-key)))
  ([parent kind #^String name]
     (KeyFactory/createKey parent kind name)))


(defn make-named-key
  "Create a named key for an entity."
  ([#^String kind #^String keyname]
     (KeyFactory/createKey kind keyname)))

(defn make-web-key
  "Create a websafe string representation of the key"
  [key]
  (KeyFactory/keyToString key))

(defn to-text
  [#^String s]
  (com.google.appengine.api.datastore.Text. s))

(defn from-text
  [#^com.google.appengine.api.datastore.Text t]
  (.getValue t))

(defn to-e-mail
  [#^String e-mail-str]
  (com.google.appengine.api.datastore.Email. e-mail-str))

(defn from-e-mail
  [#^com.google.appengine.api.datastore.Email e]
  (.getEmail e))

(defn to-key
  [key]
  (make-key key))

(defn to-webkey
  [#^com.google.appengine.api.datastore.Key key]
  (make-web-key key))

(def keyword-to-str
     (memoize name))

(def str-to-keyword
     (memoize keyword))

(defn to-sexpr-text
  [obj]
  (binding [*print-dup* true]
    (com.google.appengine.api.datastore.Text. (print-str obj))))

(defn from-sexpr-text
  [#^com.google.appengine.api.datastore.Text t]
  (read-string (.getValue t)))

(defn to-vector
  [obj]
  (into [] obj))

(defn with-default
  "Executes the conversions with the given nil-val if a nil value is provided for the function."
  [f nil-val]
  (fn [val]
    (if-not (nil? val)
      (f val)
      (f nil-val))))

(defn with-default-value
  "Returns the given nil-val if a nil value is provided for the function."
  [f nil-val]
  (fn [val]
    (if-not (nil? val)
      (f val)
      nil-val)))

