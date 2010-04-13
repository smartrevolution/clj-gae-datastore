;; Copyright (c) 2010 freiheit.com technologies gmbh
;; 
;; This file is part of clj-gae-datastore.
;; clj-gae-datastore is free software: you can redistribute it and/or modify
;; it under the terms of the GNU Lesser General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; clj-gae-datastore is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU Lesser General Public License for more details.

;; You should have received a copy of the GNU Lesser General Public License
;; along with clj-gae-datastore.  If not, see <http://www.gnu.org/licenses/>.

(ns com.freiheit.gae.datastore.datastore-access-dsl
  #^{:doc "Functions and macros for defining entities to be stored in
           the google appengine datastore."}
  (:use [clojure.contrib def ns-utils]
        [com.freiheit.gae.datastore.datastore-query-dsl 
         :only [to-entity translate-to-datastore translate-from-datastore entity-to-map]])
  (:require [com.freiheit.gae.datastore.transactions :as transactions]            
            [clojure.contrib.seq-utils :as seq-utils])
  (:import [com.google.appengine.api.datastore
            DatastoreServiceFactory Entity Key KeyFactory]))

;;;; Clojure and the Google App Engine Datastore are a perfect fit. With the low-level API
;;;; you can almost directly store Clojure datastructures natively into the datastore.
;;;; To use the low-level API, we put some simple abstractions on top of it. This way the
;;;; access to the datastore feels more lispy.

;;;; These definitions are needed to understand the code:

;;;; * Entity: An Entity is a record or a struct of data that is stored in the GAE datastore.

;;;; * Translate: In Clojure you can use normal structs. Before you can store a struct into the
;;;;   GAE datastore, it has to be translated, because the datastore can only handle
;;;;   a fixed set of datastructures. If you want to store an instance of a type which is not supported
;;;;   by the datastore, you have to translate it to a supported datatype. Example: If
;;;;   you want to store java.util.Date, you have to translate it into Millis (long) before saving it.
;;;;   When you retrieve it from the datastore, you have to translate it back from Millis to
;;;;   java.util.Date. So in this file, the word "translation" has nothing to do with I18N etc.

;;;; We made it really easy to define translations. You just use the Macro defentity.
;;;; It takes a list of clauses. Each clause contains a key and optional one or two
;;;; translation functions. The first one is the translate-to-datastore and the second one is the
;;;; translate-from-datastore function. The translation-function can also contain validations or other
;;;; transformations of the data, like changing a string to lower-case.
;;;; An entity is processed in the following steps:
;;;;
;;;; Loading from the datastore:
;;;; datastore -> load -> translate-attributes -> translate entity (post-load) -> entity
;;;;
;;;; Storing in the datastore:
;;;; entity -> translate-entity (pre-save) -> translate attributes -> store -> datastore

;;;; If you don't supply a translation-function, then the identity function is used.

;;;; We supplied some standard translations for the currently supported GAE datatypes, too.


;;;; Using defentity
;;;; ---------------

;;;; Entities that should be stored in the datastore are defined as follows:

(comment
  "This defines an Entity named project, with two properties, :key and :title"
  (defentity project
    [:key]
    [:title :pre-save to-text :post-load from-text]
    :options []))


;;;; The defentity-Macro automatically generates some functions:

;;;; This creates an in-memory struct-map of the type project with the attribute :title set.
;;;; Your entity is not saved to the datastore.

(comment
  (make-project :title "Project1"))

;;;; create a new datastore-entry of the kind "project". The value of the attribute :title
;;;; is translated from String to a GAE Text type type (and is therefore not indexed).
;;;; This saves your Entity to the datastore (persistence = side-effect!).

(comment
  (create-project (make-project :title "Project1")))

;;;; In addition to that, you will find two generated Multi-Methods in your namespace. They are
;;;; used under the hood for the translation. You don't need to call them directly. It is
;;;; all done automatically. They have no side-effects.

(comment
  (translate-from-datastore <your-entity-goes-here>)
  (translate-to-datastore <your-entity-goes-here>))

;;;; RELATIONSHIPS BETWEEN ENTITIES
;;;;
;;;; At the moment it's only possible to define parent-key relationships between entities.
;;;; Ex.
;;;; (def-parent-key-relationship project phase)
;;;; creates a function called (phases-for-project-id [project-key queries]) which can be used
;;;; to query the datastore for phases that are children of the given parent-key.
;;;; creates a function called (project-id-of-phase [phase]) which returns the id for a phase
;;;;
;;;;

;;;; CHANGING DATA.
;;;;
;;;; Changing data in entities should not be done directly via (assoc ...) but by using the
;;;; (change ...) function. This keeps a log of changes in the returned entity and can therefore
;;;; be used to check if an entity needs to be updated in the datastore.

;; ------------------------------------------------------------------------------
;; private functions
;; ------------------------------------------------------------------------------

(defn- create-new-entity
  "Create a new entity in memory to be stored in the datastore. No attributes are set
   yet."
  ([#^Key key]
     (Entity. key))
  ([#^String kind #^Key parent-key #^String name]
     (if parent-key
       (if name
         (Entity. kind name parent-key)
         (Entity. kind parent-key))
       (if name
         (Entity. kind name)
         (Entity. kind)))))

(defn- set-properties
  "Set the properties of the entity with the attributes of the item."
  [entity item]
  (doseq [[prop-name value] (dissoc item :key)] (.setProperty entity (name prop-name) value))
  entity)

(defn- entity-for-item
  "Create an entity for a map that represents an existing entity in the datastore. Uses the stored key."
  [item]
  (->
   (create-new-entity (:key item))
   (set-properties item)))

(defn- entity-for-item-when-existing
  [item]
  "Create an entity for a map that represents an existing entity in the datastore if it already exists.
   Otherwise create a new entity."
  (if (:key item)
    (entity-for-item item)
    (to-entity item)))

(defn- save-entities!
  "Save a collection of entities to the datastore."
  [#^java.util.Collection entities]
  (if-not (empty? entities)
    (.put (com.google.appengine.api.datastore.DatastoreServiceFactory/getDatastoreService) entities))
  entities)

;; ------------------------------------------------------------------------------
;; parsing the access dsl
;; ------------------------------------------------------------------------------

(defn- is-allowed-callback-specifier?
  [callback-specifier]
  (or (= callback-specifier :pre-save) (= callback-specifier :post-load)))

(defn- field-clauses
  [clauses]
  (take-while vector? clauses))

(defn- option-clauses
  [clauses]
  (drop-while vector? clauses))

(defn- destructure-clause
  "[:field :pre-save #func1 :post-load #func2] --> {:attr-name :field :pre-save #func1 :post-load func2}"
  ([attr-name]
     (if (keyword? attr-name)
       {:attr-name attr-name}
       (throw (IllegalArgumentException.
               (str "The attr-name is not a keyword: " attr-name)))))
  ([callback-specifier callback-fn]
     (if (and (keyword? callback-specifier) (is-allowed-callback-specifier? callback-specifier))
       {callback-specifier callback-fn}
       (throw (IllegalArgumentException.
             (str "The callback1-specifier is not a keyword or a wrong keyword: " callback-specifier)))))
  ([attr-name callback-specifier callback-fn]
     (merge (destructure-clause attr-name)
            (destructure-clause callback-specifier callback-fn)))
  ([attr-name callback1-specifier callback1-fn callback2-specifier callback2-fn]
     (merge (destructure-clause attr-name)
            (destructure-clause callback1-specifier callback1-fn)
            (destructure-clause callback2-specifier callback2-fn))))

(defn- transform-clauses
  "list of {:attr-name :field :pre-save #func1 :post-load func2} --> list of {:field {:pre-save #func1 :post-load #func2}}"
  [clauses]
  (map #(apply destructure-clause %) clauses))

(defn- build-fn-lookup-table
  "list of {:field {:pre-save #func1 :post-load #func2}} --> one single hashmap of {:field1 {:pre-save #func1 :post-load #func2}, ... :field2 {...}}"
  [clauses]
  (reduce (fn [left right]
            (assoc left (:attr-name right) (dissoc right :attr-name)))
          {}
          (transform-clauses (field-clauses clauses))))

(defn- build-allowed-keys
  "Erzeugt eine Liste der keys, die mit defentity deklariert wurden."
  [clauses]
  (map first (field-clauses clauses)))

(defn- build-options-table
  "build the options table. at the moment only contains :pre-save and :post-load as for the field declarations.

   Returns a map of :pre-save and :post-load."
  [clauses]
  ;; at the moment just pretend we have a [:options :pre-save ... :post-load ...] clause to use the
  ;; field-fn-lookup parser
  (let [option-clause (rest (option-clauses clauses))]
    (:options (build-fn-lookup-table 
               [(into [] (cons :options (first option-clause)))]))))
  
  
(defn- get-conversion-fn
  "Get pre-save or post-load conversion function for attr-name"
  [func-name attr-name lookup-table]
  (get (get lookup-table attr-name) func-name identity))

(defmacro merge-with-translation
  [m msource & kfns]
  (let [kfn-pairs (partition 2 kfns)
        with-applied-to-msource (mapcat
                                 (fn [[k v]]
                                   `(~k (~v (~k ~msource)))) kfn-pairs)]
  `(assoc ~m ~@with-applied-to-msource))) ;~@(with-applied-to-msource))))

(defn build-conversion-fn
  "Build the s-expr for a conversion function from a lookup table"
  [entity-sym kind keys func-name fn-lookup-table]
  (let [key-with-translate-fn     
        (mapcat #(list 
                  %
                  (get-conversion-fn func-name % fn-lookup-table)) keys)]
    `(merge-with-translation {:kind ~kind} ~entity-sym
       ~@key-with-translate-fn)))

;; ------------------------------------------------------------------------------
;; public functions
;; ------------------------------------------------------------------------------

(defn create-entity
  "Create a new entity for storing a new element in the datastore."
  ([#^String kind item] (create-entity kind item nil nil))
  ([#^String kind item #^Key parent-key]
     (create-entity kind item parent-key nil))
  ([#^String kind item #^Key parent-key #^String keyName]
     (let [entity (create-new-entity kind parent-key keyName)]
       (set-properties entity item)
       entity)))

;; changing the contents of an entity in-memory
(defn assoc-entity
     "Change an Entity. This does some bookkeeping of the changes
be storing meta-data about the changes. This way we can check, if an entity
changed and if we have to update it in the datastore, too. You can lookup
the changes via using (meta <your-entity>)."
     ([entity & kvs]
        (with-meta (apply assoc entity kvs) (apply assoc {} kvs) )))


;;Checking if an entity was changed in-memory
(defn dirty-entity?
  "Return true when an entity has been changed by using the function update-entity
and needs to be saved in the datastore. This won't detect changes, if you did not use
update-entity and used assoc or something else instead."
  [entity]
  (not (empty? (meta entity))))

;; for serializing keys
(defmethod print-dup com.google.appengine.api.datastore.Key
  [val writer]
  (.write writer (str "#=(com.google.appengine.api.datastore.KeyFactory/stringToKey \"" (KeyFactory/keyToString val) "\")")))

(defn set-parent-for-map
  "set the parent for a struct map representing an in-memory-entity before storing to datastore. only public because
   used in a macro"
  [parent-key m]
  (assoc m :parent-key parent-key))

(defn get-parent-for-map
  "get the parent for a struct map representing an in-memory-entity before storing to datastore. only public because
   it is used in macro."
  [m]
  (:parent-key m))

(defmacro defentity
  "Define an appengine datastore entity."
  [entity-name & body]
  (let [allowed-keys (build-allowed-keys body)
        fn-lookup-table (build-fn-lookup-table body)
        options-table (build-options-table body)
        selection-keys (vec (conj allowed-keys :parent-key))
        kind (str entity-name)]
    `(do
       (defstruct ~entity-name ~@allowed-keys)
       (defn ~(symbol (str "create-" (name entity-name)))
         [~entity-name]
         (create-entity ~kind (select-keys ~entity-name (vector ~@allowed-keys)) (get-parent-for-map ~entity-name)))

       (defn ~(symbol (str "make-" (name entity-name)))
         [& inits#]
         (apply struct-map ~entity-name :kind ~kind inits#))

       (defmethod translate-to-datastore ~kind
         [~entity-name]         
         ~(if-let [entity-pre-save (:pre-save options-table)]
            `(let [~entity-name (~entity-pre-save ~entity-name)]
               ~(build-conversion-fn entity-name kind selection-keys :pre-save fn-lookup-table))
            (build-conversion-fn entity-name kind selection-keys :pre-save fn-lookup-table)))
    
       (defmethod translate-from-datastore ~kind
         [~entity-name]
         ~(if-let [entity-post-load (:post-load options-table)]
            `(~entity-post-load
              ~(build-conversion-fn entity-name kind selection-keys :post-load fn-lookup-table))
            (build-conversion-fn entity-name kind selection-keys :post-load fn-lookup-table)))

       (defmethod to-entity ~kind
         [~entity-name]
         (~(symbol (str "create-" (name entity-name))) ~entity-name)))))

;; defining relationships
(defmacro def-parent-key-relationship [parent child]
  (let [parent-id-sym (symbol (str parent "-id"))
        query-sym (symbol "queries")
        child-sym (symbol child)]
    `(do
       ;; ex: phases-for-release-id
       (defmacro ~(symbol (str child "s-for-" parent "-id"))
         [~parent-id-sym ~query-sym]
         `(when ~~parent-id-sym
            (com.freiheit.clojure.datastore.datastore-query-dsl/select
             (com.freiheit.clojure.datastore.datastore-query-dsl/where ~~parent-id-sym ~~child ~~query-sym))))
       ;; ex: release-id-of-phase
       (defn ~(symbol (str parent "-id-of-" child))
         [~child]
         (if-let [child-key# (:key ~child)]
           (.getParent child-key#)
           (get-parent-for-map ~child)))
       ;; ex: make-phase-for-release-id
       (defn ~(symbol (str "make-" child "-for-" parent "-id"))
         [~parent-id-sym & inits#]
         (set-parent-for-map ~parent-id-sym (apply ~(symbol (str "make-" child)) inits#))))))

;; storing a list of elements in the datastore
(def #^{:doc "Store a list of elements in the datastore."
        :arglist '[entities]}
     store-entities!
     (comp
      (partial map translate-from-datastore)
      (partial map entity-to-map)
      save-entities!
      (partial map to-entity)
      (partial map translate-to-datastore)))

(def update-entities!
     #^{:doc "Update a list of elements in the datastore."
        :arglist '[entities]}
     (comp
      (partial map translate-from-datastore)
      (partial map entity-to-map)
      save-entities!
      (partial map entity-for-item)
      (partial map translate-to-datastore)))

(def store-or-update-entities!
     #^{:doc "Update or save a list of elements in the datastore."
        :arglist '[entities]}
     (comp
      (partial map translate-from-datastore)
      (partial map entity-to-map)
      save-entities!
      (partial map entity-for-item-when-existing)
      (partial map translate-to-datastore)))

(defn delete-all!
  "Delete all elements specified by the given keys from the datastore."
  [#^java.util.Collection keys]
  (let [service (DatastoreServiceFactory/getDatastoreService)
        max-batch-size 500]
    (dorun
     (for [#^Key key-batch (seq-utils/partition-all max-batch-size keys)]
       (.delete service key-batch)))))
