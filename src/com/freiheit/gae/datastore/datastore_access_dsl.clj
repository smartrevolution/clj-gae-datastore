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
  #^{:doc "A mini-language to access the Google AppEngine datastore."}
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

;;;; * Variable and Parameter naming:
;;;;   When we use "entity", we always mean objects of the GAE class Entity
;;;;   When we use "entity-map" we always mean a Clojure map which can be converted
;;;;   to an "entity", because it was defined with defentity and is maintained by the
;;;;   functions and macros of this package.


;;;; * Translate: In Clojure you can use normal structs. Before you can store a struct into the
;;;;   GAE datastore, it has to be translated, because the datastore can only handle
;;;;   a fixed set of datastructures. If you want to store an instance of a type which is not supported
;;;;   by the datastore, you have to translate it to a supported datatype. Example: If
;;;;   you want to store java.util.Date (which is a supported type, but just to give you an easy example),
;;;;   you might want to translate it into Millis (long) before saving it (pre-save).
;;;;   When you retrieve it from the datastore (post-load), you have to translate it back from Millis to
;;;;   java.util.Date. So in this file, the word "translation" has nothing to do with I18N etc.

;;;; We made it really easy to define such translations. You just use the Macro defentity.
;;;; It takes a list of clauses. Each clause contains an attribute-name and (optional) options.

;;;; Valid options are:
;;;; Name         Value         Description
;;;; :pre-save    a function    Called for this attribute before an entity is saved to the datastore
;;;;                            Default is the identity function
;;;; :post-load   a function    Called for this attribute after an entity is loaded from the datastore
;;;;                            Default is the identity function
;;;; :unindexed   boolean       If true, this attribute is not indexed. If false, it will be indexed.
;;;;                            Default is false. This means the attribute will be indexed if the GAE type
;;;;                            supports indexing.

;;;; Under the hood we generate two multimethods (translate-to-datastore, translate-from-datastore) for
;;;; each defentity definition, which specialize on the kind of an entity/entity-map. These multimethods
;;;; are used to by this system to call your pre-save and post-load functions.
;;;; Check out the macro defentity for how they are generated.

;;;; You should not use these multimethods in you programs, but you can use them for debugging purposes:

(comment
  (translate-from-datastore <your-entity-goes-here>) ; DON'T RELY ON THIS IN YOU CODE. IT MIGHT CHANGE!
  (translate-to-datastore <your-entity-goes-here>)) ; SHOULD ONLY BE USED BY THIS PACKAGE!


;;;; An entity is processed in the following steps:
;;;;
;;;; Loading from the datastore:
;;;; datastore -> load -> translate-attributes -> translate entity (post-load) -> entity
;;;;
;;;; Storing in the datastore:
;;;; entity -> translate-entity (pre-save) -> translate attributes -> store -> datastore

;;;; We supplied some standard translations for the currently supported GAE datatypes, too.


;;;; Using defentity
;;;; ---------------

;;;; To define the schema of an entity, you use defentity.

(comment
  "This defines an Entity named project, with two properties, :key and :title"
  (defentity project
    [:key]
    [:title :pre-save to-text :post-load from-text]
    :options []))

;;;; CAUTION: Currently we don't evaluate the options-statement (see last line in the above example)

;;;; This creates the code to generate and convert objects of the GAE class Entity. This example
;;;; generates a function "make-project", which can be used to create entity-maps of the kind "project".

;;;; The following code creates an in-memory entity-map of the kind "project" with the attribute :title set
;;;; to "Project1".
;;;; Your entity-map is not saved to the datastore.

(comment
  (make-project :title "Project1"))

;;;; If you want to convert this to an entity object you can call to-entity. This is a
;;;; multimethod that is generated by defentity and which "knows" how to generate
;;;; a GAE Entity of this kind. But you should never use it in your code.
;;;; You can use it for debugging.

(comment
  (to-entity (make-project :title "Project1"))) ;DON'T RELY ON to-entity IN YOUR CODE. IT MIGHT CHANGE!

;;;; You NEVER work with entities (objects of class Entity) in your Clojure programs.
;;;; You should only work with entity-maps, which are standard Clojure struct-maps.

;;;; CHANGING DATA.
;;;;
;;;; Changing data in entity-maps should not be done directly via (assoc ...) but by using the
;;;; (assoc-and-track-changes ...) function. This keeps a log of changes in the meta-data
;;;; of the returned entity-map and can therefore be used to check if an entity-map needs
;;;; to be updated in the datastore.

;;;; If you want to save soemthing to the datastore, you are using the store!-function
;;;; You just hand-over an entity-map. The system converts this automatically to an entity
;;;; of the correct kind and stores that into the datastore.

(comment
  (store! (make-project :title "Project1")))

;;;; RELATIONSHIPS BETWEEN ENTITIES
;;;;
;;;; At the moment it's only possible to define parent-key relationships between entities.
;;;; Ex.
;;;; (def-parent-key-relationship project phase)
;;;; creates a function called (phases-for-project-id [project-key queries]) which can be used
;;;; to query the datastore for phases that are children of the given parent-key.
;;;; creates a function called (project-id-of-phase [phase]) which returns the id for a phase

;; ------------------------------------------------------------------------------
;; private functions
;; ------------------------------------------------------------------------------

(defn- create-empty-entity
  "Create an empty entity in memory to be stored in the datastore. No attributes are set yet."
  ([#^Key key]
     (Entity. key))
  ([#^String kind #^Key parent-key #^String key-name]
     (if parent-key
       (if key-name
         (Entity. kind key-name parent-key)
         (Entity. kind parent-key))
       (if key-name
         (Entity. kind key-name)
         (Entity. kind)))))

(defn- set-properties
  "Set the properties of the entity with the attributes of the item."
  [entity entity-map]
  (doseq [[prop-name value] (dissoc entity-map :key)] (.setProperty entity (name prop-name) value))
  entity)

(defn- entitymap-to-new-entity
  "Converts an entity-map to an entity. Uses the key contained in the entity-map to
construct a fresh new Entity to be stored for the first time in the datastore."
  [entity-map]
  (->
   (create-empty-entity (:key entity-map))
   (set-properties entity-map)))

(defn- entitymap-to-entity
  [entity-map]
  "Converts an entity-map to an entity. When a key exists, don't create a new one!
This means we create a fresh new entity, if the key is not already set. If we have
a key, we keep it, so that the system will update the already existing data in the datastore."
  (if (:key entity-map)
    (entitymap-to-new-entity entity-map)
    (to-entity entity-map)))

(defn- save-entity!
  "Save an entity to the datastore."
  [entity]
  (when-not (nil? entity)
    (.put (com.google.appengine.api.datastore.DatastoreServiceFactory/getDatastoreService) entity))
  entity)

(defn- save-entities!
  "Save a collection of entities to the datastore."
  [#^java.util.Collection entities]
  (when-not (empty? entities)
    (.put (com.google.appengine.api.datastore.DatastoreServiceFactory/getDatastoreService) entities))
  entities)

;; ------------------------------------------------------------------------------
;; parsing the access dsl (private, too!)
;; ------------------------------------------------------------------------------

(defn- field-clauses
  [clauses]
  (take-while vector? clauses))

(defn- option-clauses
  [clauses]
  (drop-while vector? clauses))


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

(defn- build-fn-lookup-table
  "list of {:field {:pre-save #pre-save-func, :post-load #post-load-func, :unindexed bool}}
--> one single hashmap of {:field1 {:pre-save #pre-save-func :post-load #post-load-func :unindexed bool}, ... :field2 {...}}"
  [clauses]
  (reduce (fn [left right]
            (assoc left (:attr-name right) (dissoc right :attr-name)))
          {}
          (transform-clauses (field-clauses clauses))))


(defn- build-allowed-keys
  "Creates a list of the keys that were defined with a call to defentity.
   We use it to check which keys from the map should be stored in the datastore
   to prevent the programmer from adding new properties accidentally."
  [clauses]
  (map first (field-clauses clauses)))


(defn- get-conversion-fn
  "Get pre-save or post-load conversion function for attr-name"
  [func-name attr-name lookup-table]
  (get (get lookup-table attr-name) func-name identity))

(defmacro merge-with-translation
  [m msource & kfns]
  (let [kfn-pairs               (partition 2 kfns)
        with-applied-to-msource (mapcat (fn [[k v]] `(~k (~v (~k ~msource)))) kfn-pairs)]
  `(assoc ~m ~@with-applied-to-msource))) ;~@(with-applied-to-msource)

(defn- build-conversion-fn
  "Build the s-expr for a conversion function from a lookup table"
  [entity-sym kind keys func-name fn-lookup-table options-table]
  (let [key-with-translate-fn (mapcat #(list % (get-conversion-fn func-name % fn-lookup-table)) keys)]
    (if (:allow-dynamic options-table)
      `(let [base-object# (assoc (into {} ~entity-sym) :kind ~kind)]
         (merge-with-translation base-object# ~entity-sym ~@key-with-translate-fn))
      `(merge-with-translation {:kind ~kind} ~entity-sym ~@key-with-translate-fn))))

;;; ------------------------------------------------------------------------------
;;; public functions
;;; ------------------------------------------------------------------------------

(defn create-entity
  "Creates a new GAE Entity object from a clojure map. The Entity is created in memory and is not persisted.
It is only used by the multimethod to-entity (which is generated by defentity) AND SHOULD NOT BE USED ELSEWHERE IN YOUR CODE.
We leave it as a public function to make it easier for the programmer to create Entity objects for debugging purposes.
But please do not rely on it!"
  ([#^String kind entity-map] (create-entity kind entity-map nil nil))
  ([#^String kind entity-map #^Key parent-key]
     (create-entity kind entity-map parent-key nil))
  ([#^String kind entity-map #^Key parent-key #^String keyName]
     (let [entity (create-empty-entity kind parent-key keyName)]
       (set-properties entity entity-map)
       entity)))


(defn assoc-and-track-changes
  "Works exactly like assoc on a map, but does some book-keeping of the changes
be storing the changes in the meta-data. This way we can check, if an entity
changed and if we have to update it in the datastore, too. This even enables us to
only send the changed properties to the datastore."
  ([entity-map & kvs]
     (with-meta (apply assoc entity-map kvs) (apply assoc {} kvs) )))


(defn has-changes?
  "Returns true when the entity-map has been changed by using the function assoc-and-track-changes
and needs to be saved to the datastore. This won't detect changes, if you did not use
assoc-and-track-changes and used assoc or something else instead to manipulate the entity-map."
  [entity-map]
  (not (empty? (meta entity-map))))

(defn changes
  "Returns only the changed key/values when the entity-map has been changed by using the function assoc-and-track-changes
and needs to be saved to the datastore. This won't return changes, if you did not use
assoc-and-track-changes and used assoc or something else instead to manipulate the entity-map."
  [entity-map]
  (meta entity-map))


;; for serializing keys
(defmethod print-dup com.google.appengine.api.datastore.Key
  [val writer]
  (.write writer (str "#=(com.google.appengine.api.datastore.KeyFactory/stringToKey \"" (KeyFactory/keyToString val) "\")")))

(defn set-parent-for-map
  "Get the parent for a struct map representing an in-memory-entity before storing to datastore."
  [parent-key m]
  (assoc m :parent-key parent-key))

(defn get-parent-for-map
  "Get the parent for a struct map representing an in-memory-entity before storing to datastore."
  [m]
  (:parent-key m))

(defmacro defentity
  "Define the schema for Google App Engine datastore entity.
Syntax: (defentity <entity-name> [:attr-name1 :pre-save #fn :post-load fn :unindexed bool] [:attr-name2 ...]...)"
  [entity-name & body]
  (let [allowed-keys    (build-allowed-keys body)
        fn-lookup-table (build-fn-lookup-table (take-while vector? body))
        options-table   (apply hash-map (remove vector? body))
        selection-keys  (vec (conj allowed-keys :parent-key))
        ;FIXME: Better to store parent-key in meta-data!
        kind (str entity-name)]
    `(do
       (defstruct ~entity-name ~@allowed-keys)

       (defn ~(symbol (str "make-" (name entity-name)))
         [& inits#]
         (apply struct-map ~entity-name :kind ~kind inits#))

       (defmethod translate-to-datastore ~kind
         [~entity-name]
         ~(if-let [entity-pre-save (:pre-save options-table)]
            `(let [~entity-name (~entity-pre-save ~entity-name)]
               ~(build-conversion-fn entity-name kind selection-keys :pre-save fn-lookup-table options-table))
            (build-conversion-fn entity-name kind selection-keys :pre-save fn-lookup-table options-table)))

       (defmethod translate-from-datastore ~kind
         [~entity-name]
         ~(if-let [entity-post-load (:post-load options-table)]
            `(~entity-post-load
              ~(build-conversion-fn entity-name kind selection-keys :post-load fn-lookup-table options-table))
            (build-conversion-fn entity-name kind selection-keys :post-load fn-lookup-table options-table)))

       (defmethod to-entity ~kind
         [~entity-name]
         ~(if (:allow-dynamic options-table)
            `(create-entity ~kind (dissoc ~entity-name :parent-key :kind) (get-parent-for-map ~entity-name))
            `(create-entity ~kind (select-keys ~entity-name (vector ~@allowed-keys)) (get-parent-for-map ~entity-name)))))))


;; defining relationships
(defmacro def-parent-key-relationship [parent child]
  (let [parent-id-sym (symbol (str parent "-id"))
        query-sym (symbol "queries")]
    `(do
       ;; ex: phases-for-release-id
       (defmacro ~(symbol (str child "s-for-" parent "-id"))
         [~parent-id-sym ~query-sym]
         `(when ~~parent-id-sym
            (com.freiheit.gae.datastore.datastore-query-dsl/select
             (com.freiheit.gae.datastore.datastore-query-dsl/where ~~parent-id-sym
                                                                   ~(quote ~child)
                                                                   ~~query-sym))))
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


;;; TODO: the store-functions should be consolidated into one single function

(defn store-entities!
  "Store a list of entity-maps in the datastore."
  [entities]
  (->> entities
       (map translate-to-datastore)
       (map to-entity)
       (save-entities!)
       (map entity-to-map)
       (map translate-from-datastore)))

(defn store-entity!
  "Store an entity-map in the datastore."
  [entity]
  (->> entity
       (translate-to-datastore)
       (to-entity)
       (save-entity!)
       (entity-to-map)
       (translate-from-datastore)))

(defn update-entities!
  "Update a list of entity-maps in the datastore."
  [entities]
  (->> entities
      (map translate-to-datastore)
      (map entitymap-to-new-entity) ;entity-for-item
      (save-entities!)
      (map entity-to-map)
      (map translate-from-datastore)))

(defn update-entity!
  "Update an entity-map in the datastore."
  [entity]
  (->> entity
      (translate-to-datastore)
      (entitymap-to-new-entity) ;entity-for-item
      (save-entity!)
      (entity-to-map)
      (translate-from-datastore)))

(defn store-or-update-entities!
  "Update or save a list of entity-maps in the datastore."
  [entities]
  (->> entities
      (map translate-to-datastore)
      (map entitymap-to-entity) ;entity-for-item-when-existing
      (save-entities!)
      (map entity-to-map)
      (map translate-from-datastore)))

(defn store-or-update-entity!
  "Update or save an entity-map in the datastore."
  [entity]
  (->> entity
      (translate-to-datastore)
      (entitymap-to-entity) ;entity-for-item-when-existing
      (save-entity!)
      (entity-to-map)
      (translate-from-datastore)))

(defn delete-all!
  "Delete all entities specified by the given keys from the datastore."
  [#^java.util.Collection keys]
  (let [service (DatastoreServiceFactory/getDatastoreService)
        max-batch-size 500]
    (dorun
     (for [#^Key key-batch (seq-utils/partition-all max-batch-size keys)]
       (.delete service key-batch)))))

(defn delete!
  "Delete all entities specified by the given keys from the datastore."
  [key]
  (let [service (DatastoreServiceFactory/getDatastoreService)]
    (.delete service key)))
