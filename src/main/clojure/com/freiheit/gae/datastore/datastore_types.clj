(ns com.freiheit.gae.datastore.datastore-types
  #^{:doc "Translation functions for different datastore types."}
  (:require 
   [com.freiheit.clojure.util.date :as date]
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
  (date/date-to-ms date-time))

(defn from-ms
  [#^Long ms]
  (date/date-from-ms ms))

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
  (read-string (.getValue t)))

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

