(ns com.freiheit.gae.datastore.dsl-test
  (:use [com.freiheit.gae.datastore.dsl])
  (:use [clojure.test])
  (:require [com.freiheit.gae.local-dev :as local-dev])
  (:import [com.google.appengine.api.datastore Entity Key KeyFactory]))

(defentity _test
  [:key]
  [:foo])

(defn load-testdata
  []
  (select (where _test [])))

(defn delete-testdata
  []
  (delete-all! (select-only-keys (where _test []))))

(defn appengine-fixture [f]
  (do
    (local-dev/init-app-engine)
    (delete-testdata)
    (f)))

(use-fixtures :once appengine-fixture)


(deftest test-defentity
  (is (instance? com.freiheit.gae.datastore.dsl-test._test (_test. nil "foo")))
  (is (nil? (:key (_test. nil "foo"))))
  (is (= "foo" (:foo (_test. nil "foo"))))
  (is (not (nil? (_test. "keyname" "foo")))))


(deftest test-insert-update-delete
  (is (= 0 (count (load-testdata))))
  (is (= 1 (count (save! (list (_test. nil "foo"))))))
  (is (= 1 (count (load-testdata))))
  (is (= 2 (count (keys (first (load-testdata))))))
  (is (= "foo" (:foo (first (load-testdata)))))
  (is (not (nil? (:key (first (load-testdata))))))
  (is (string? (:key (first (load-testdata)))))
  (is (instance? com.google.appengine.api.datastore.Key (KeyFactory/stringToKey (:key (first (load-testdata))))))
  (is (< 0 (.getId (KeyFactory/stringToKey (:key (first (load-testdata)))))))
  (is (= "bar"  (:foo (first (save! (list (assoc (first (load-testdata)) :foo "bar")))))))
  (is (= 1 (count (load-testdata))))
  (is (= "keyname" (-> (_test. "keyname" "foo")
                       list
                       save!
                       first
                       :key
                       KeyFactory/stringToKey
                       .getName))))

(deftest test-serialize
  (is (= "{\"key\":null,\"foo\":\"foo\"}" (serialize (_test. nil "foo")))))

(deftest test-protocol-datastore
  (is (instance? com.google.appengine.api.datastore.Entity (to-entity (_test. nil "foo"))))
  (is (instance? com.google.appengine.api.datastore.Key (.getKey (to-entity (_test. nil "foo")))))
  (is (= 0  (.getId (.getKey (to-entity (_test. nil "foo"))))))
  (is (= "_test" (get-kind (_test. nil "foo"))))
  (is (= "baz" (:foo  (-> (set-parent (_test. nil "foo")
                                      (_test. nil "baz"))
                          get-parent)))))