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

(ns com.freiheit.gae.datastore.datastore-types
  #^{:doc "Translation functions for different datastore types."}
  (:require
   [clj-time.coerce :as date]
   [clj-time.core :as date-core]
   [com.freiheit.gae.datastore.keys :as keys])
  (:import
   [com.google.appengine.api.datastore Key Text Email]))

;;;; Some translation functions for the datastore api

;; ------------------------------------------------------------------------------
;; public functions
;; ------------------------------------------------------------------------------

(defn to-text
  [#^String s]
  (com.google.appengine.api.datastore.Text. s))

(defn from-text
  [#^com.google.appengine.api.datastore.Text t]
  (.getValue t))

(defn to-ms
  [#^org.joda.time.Datetime date-time]
  (date/to-long date-time))

(defn from-ms
  [#^Long ms]
  (date/from-long ms))

(defn from-ms-with-tz
  [tz]
  (fn [#^Long ms]
    (-> (date/from-long ms)
        (date-core/to-time-zone tz))))

(def from-ms-with-default-tz
     (from-ms-with-tz (date-core/default-time-zone)))

(defn to-e-mail
  [#^String e-mail-str]
  (com.google.appengine.api.datastore.Email. e-mail-str))

(defn from-e-mail
  [#^com.google.appengine.api.datastore.Email e]
  (.getEmail e))

(defn to-key
  [#^String key-string]
  (keys/make-key key-string))

(defn to-webkey
  [#^com.google.appengine.api.datastore.Key key]
  (keys/make-web-key key))

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
  (if t
    (read-string (.getValue t))
    t))

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

