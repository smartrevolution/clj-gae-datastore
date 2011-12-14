;; Copyright (c) 2010 freiheit.com technologies gmbh
;;
;; This file is part of clj-gae-datastore.
;; clj-gae-datastore is free software: you can redistribute it and/or modify
;; it under the terms of the GNU Lesser General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; clj-gae-datastore is distributed in the hope that it will be useful
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU Lesser General Public License for more details.

;; You should have received a copy of the GNU Lesser General Public License
;; along with clj-gae-datastore.  If not, see <http://www.gnu.org/licenses/>.

(ns com.freiheit.gae.datastore.datastore-query-dsl
  #^{:doc "A DSL for querying entities from the google datastore."}
  (:use
   [clojure.contrib.def :only [defvar-]])
  (:import
   [com.google.appengine.api.datastore
    DatastoreServiceFactory Entity Key KeyFactory Query Query$FilterOperator
    EntityNotFoundException FetchOptions FetchOptions$Builder Cursor]))

;;;; Querying entities from the google datastore.
;;;; Provides a query-language which can be used to
;;;; search for entities by search predicates.
;;;;
;;;; Examples:
;;;; 1 (select (where feature ([= :feature-title "Register user"])))
;;;;     this will select all features with a feature-title of "Register user".
;;;; 2 (select-only-keys (where feature ([= :feature-title "Register user"])))
;;;;     same as above but will only return the keys of the entities. This is
;;;;     much faster than querying for whole entities.
;;;;
;;;; It's also possible to use '<', '<=', '>' or '>=' in the where clause.
;;;;
;;;; The returned entities are already translated by their corresponding translation function.
;;;;
;;;; 3 (resolve-entities (select (where feature ())) :feature-author-id)
;;;;     returns a list of features (as in the example above) but replaces the feature-author-id
;;;;     with the actual entities referenced in the entities' :feature-author-id attributes.
;;;;
;;;;     this is done by first collecting all author ids in the list of features returned by
;;;;     the select clause, and afterwards doing a "batch get" operation. this is more efficient
;;;;     than doing single queries.

;; ------------------------------------------------------------------------------
;; constants and data structures
;; ------------------------------------------------------------------------------
(defn- fetch-options
  "Create fetch options for a query with the given limit and an optional websafe cursor.
  The cursor is ignored if it is nil."
  ([#^Number limit]
     (fetch-options limit nil))
  ([#^Number limit #^String cursor]
     (let [clamped-limit (if (> 100 limit) 100 limit)
           limit-options (doto (FetchOptions$Builder/withLimit limit)
                           (.prefetchSize clamped-limit)
                           (.chunkSize clamped-limit))]
       (if (nil? cursor)
         limit-options
         (->> (Cursor/fromWebSafeString cursor)
              (.cursor limit-options))))))

;; ------------------------------------------------------------------------------
;; private functions
;; ------------------------------------------------------------------------------

(defn- mapmap
  "Create a new map by applying key-f to every key and val-f to every value of a map"
  [key-f val-f m]
  (apply merge (map (fn [[k v]] {(key-f k) (val-f v)}) m)))

(defn- execute-query
  "Execute the datastore query. Uses the default fetch options if none are given."
  ([#^Query query]
     (execute-query query (FetchOptions$Builder/withDefaults)))
  ([#^Query query #^FetchOptions fetch-options]
     (let [data-service (com.google.appengine.api.datastore.DatastoreServiceFactory/getDatastoreService)
           results (. (.prepare data-service query) asQueryResultList fetch-options)]
       results)))

(defn- distinct-entity-keys
  "resolving an id property for a list of elements."
  [entities attr]
  (filter identity (distinct (mapcat #(let [ret (attr %)]
                                        (if (instance? java.util.Collection ret)
                                          ret
                                          [ret]))
                                        entities))))

(defn- translate-map-vals
  "Apply the given function to all values in the map."
  [m function]
  (mapmap identity function m))

(defn- replace-map-vals
  "."
  [m entities attr target-attr]
  (map (fn [entity]
	 (assoc entity target-attr
                (let [entity-vals (attr entity)]
                  (if (instance? java.util.Collection entity-vals)
                    (vals (select-keys m (attr entity)))
                    (get m entity-vals))))) entities))

(def mem-keyword (memoize keyword))


;; ------------------------------------------------------------------------------
;; public functions
;; ------------------------------------------------------------------------------

(defn entity-to-map
  "Converts an instance of com.google.appengine.api.datastore.Entity
  to a PersistentHashMap with properties stored under keyword keys
  plus the entity's kind stored under :kind and key stored under :key."
  [#^com.google.appengine.api.datastore.Entity entity]
  (reduce #(assoc %1 (mem-keyword (key %2)) (val %2))
	  {:kind (.getKind entity) :key (.getKey entity)}
	  (.entrySet (.getProperties entity))))

(defn get-entity-by-key
  "Return a map of keys to entities for the given keys."
  [#^com.google.appengine.api.datastore.Key key]
  (.get (DatastoreServiceFactory/getDatastoreService) key))

(defn get-entities-by-keys
  "Return a map of keys to entities for the given keys."
  [#^java.util.Collection keys]
  (.get (DatastoreServiceFactory/getDatastoreService) keys))

;; translation multi methods
(defmulti translate-to-datastore
  "Multimethod for translating elements to the datastore. Dispatches on the kind."
  :kind)

(defmulti translate-from-datastore
  "Multimethod for translating elements from the datastore. Dispatches on the kind."
  :kind)

(defmulti to-entity
  "Multimethod for creating an entity. Dispatches on the kind."
  :kind)

(defn translate-entities
  "Translate the attributes of entities with the translation functions from datastore to
  the actual domain model."
  [entities]
  (doall (map (comp translate-from-datastore entity-to-map) entities)))

(defn translate-keys-only
  "Translate only the keys of the entities."
  [entities]
  (map (memfn getKey) entities))

(defn assert-cursor
  "Get the cursor if one was returned."
  [result]
  (if-let [cursor(.getCursor result)]
    (.toWebSafeString cursor)
    (throw (IllegalArgumentException. "Batch-Query didn't return a cursor (ie not-equal filter), use offset/limit instead."))))

(defn select
  "Executes the given com.google.appengine.api.datastore.Query
  and returns the results as a sequence of items converted with entity-to-map."
  [#^Query query]
  (doall (-> query
             execute-query
             translate-entities)))

(defn select-only-keys
  "Executes the given com.google.appengine.api.datastore.Query
  and returns the result-keys."
  [#^Query query]
  (doall (-> query
             (.setKeysOnly)
             execute-query
             translate-keys-only)))

(defn select-batch
  "Executes the given com.google.appengine.api.datastore.Query with a fetch options
  limit and an optional cursor information to resume a previous batch select.

  The return value is a map containing the query result list and a cursor to resume
  the query at the last postion."
  ([#^Query query #^Number limit]
     (select-batch query limit nil))
  ([#^Query query #^Number limit #^String cursor]
     (let [query-result (->> (fetch-options limit cursor)
                             (execute-query query))]
       {:result (translate-entities query-result)
        :cursor (assert-cursor query-result)})))

(defn select-only-keys-batch
  "Works similar to select-batch but returns the entities' keys as result."
  ([#^Query query #^Number limit]
     (select-only-keys-batch query limit nil))
  ([#^Query query #^Number limit #^String cursor]
     (let [query-result (->> (fetch-options limit cursor)
                             (execute-query (.setKeysOnly query)))]
       {:result (translate-keys-only query-result)
        :cursor (assert-cursor query-result)})))

(defmacro where
  "Create a query for a given kind.

  TODO: run the query-parameter-values through translate-to-datastore.

  When the kind is nil, then a kindless-query will be generated.

  Ex: (where person [[= :person-password-hash \"123\"]])"
  ([kind queries]
     `(where :none ~kind ~queries))
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

          (cond
           (nil? ~parent-key) (throw (IllegalArgumentException. "Parent key cannot be nil, use :none instead."))
           (not= :none ~parent-key) (.setAncestor q# ~parent-key))

	  (doseq [[operator# prop# value#] ~(vec op-queries)]
	    (. q# addFilter (name prop#) (eval operator#) value#))
	  q#))))

(defmacro where-dyn-queries
  "Create a query for a given kind with dynamic query parameters.

  TODO: run the query-parameter-values through translate-to-datastore.

  Ex: (where-dyn-queries person [[:= :person-password-hash \"123\"]])"
  ([kind queries]
     `(where-dyn-queries nil ~kind ~queries))
  ([parent-key kind queries]
     (let [kind-name (name kind)]
       `(let [q# (com.google.appengine.api.datastore.Query. ~kind-name)
              op-smap# {:= com.google.appengine.api.datastore.Query$FilterOperator/EQUAL
                        :not= com.google.appengine.api.datastore.Query$FilterOperator/NOT_EQUAL
                        :< com.google.appengine.api.datastore.Query$FilterOperator/LESS_THAN
                        :<= com.google.appengine.api.datastore.Query$FilterOperator/LESS_THAN_OR_EQUAL
                        :> com.google.appengine.api.datastore.Query$FilterOperator/GREATER_THAN
                        :>= com.google.appengine.api.datastore.Query$FilterOperator/GREATER_THAN_OR_EQUAL
                        :in com.google.appengine.api.datastore.Query$FilterOperator/IN}
              op-queries# (map (fn [query#] (replace op-smap# query#)) ~queries)]
	  (when ~parent-key
            (.setAncestor q# ~parent-key))
	  (doseq [[operator# prop# value#] op-queries#]
	    (. q# addFilter (name prop#) (eval operator#) value#))
	  q#))))

(defn get-by-keys
  "For a list of keys fetch the entities and return a map of key to entry"
  [#^java.util.Collection keys]
  (-> keys
      get-entities-by-keys
      (translate-map-vals (comp translate-from-datastore entity-to-map))))

(defn get-by-key
  "For a single key return the datastore entity"
  ([key]
     (-> key
         get-entity-by-key
         entity-to-map
         translate-from-datastore))
  ([key not-found-val]
     (try
      (get-by-key key)
      (catch EntityNotFoundException e
        not-found-val))))

(defn resolve-entities
  "For a sequence of entities 'resolve' other entities that are referenced by
   keys.

   The function first selects all distinct keys from the given attributes.
   Then a batch get for all keys is done and afterwards all entities for these keys
   are assigned to the original entities."
  ([entities attr]
     (resolve-entities entities attr attr))
  ([entities attr target-attr]
     (-> (distinct-entity-keys entities attr)
         get-by-keys
         (replace-map-vals entities attr target-attr))))
