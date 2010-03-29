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

(ns com.freiheit.gae.datastore.keys
  (:use 
   clojure.test)
  (:import
   [com.google.appengine.api.datastore KeyFactory Key]))

;; ------------------------------------------------------------------------------
;; public functions
;; ------------------------------------------------------------------------------

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
