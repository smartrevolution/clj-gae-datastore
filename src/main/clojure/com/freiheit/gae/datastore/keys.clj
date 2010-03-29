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
