(ns com.freiheit.gae.datastore.dsl-test
  (:use [clojure.test])
  (:require [com.freiheit.gae.local-dev :as local-dev]
            [com.freiheit.gae.datastore.dsl :as dsl])
  (:import [com.google.appengine.api.datastore Entity Key KeyFactory]))

(dsl/defentity _test
  [:key]
  [:foo])

(defn load-testdata
  []
  (dsl/select (dsl/where _test [])))

(defn print-testdata
  []
  (doseq [t (load-testdata)]
    (println t)))

(defn delete-testdata
  []
  (dsl/delete-all! (dsl/select-only-keys (dsl/where _test []))))

(defn appengine-fixture [f]
  (do
    (local-dev/init-app-engine)
    (delete-testdata)
    (f)))


;(use-fixtures :once appengine-fixture)
(use-fixtures :each appengine-fixture)


(deftest test-defentity
  (is (instance? com.freiheit.gae.datastore.dsl-test._test (_test. nil "foo")))
  (is (nil? (:key (_test. nil "foo"))))
  (is (= "foo" (:foo (_test. nil "foo"))))
  (is (not (nil? (_test. "keyname" "foo"))))
  (is (instance? com.google.appengine.api.datastore.Entity (dsl/to-entity (_test. nil "foo"))))
  (is (instance? com.google.appengine.api.datastore.Key (.getKey (dsl/to-entity (_test. nil "foo")))))
  (is (= 0  (.getId (.getKey (dsl/to-entity (_test. nil "foo"))))))
  (is (= "_test" (dsl/get-kind (_test. nil "foo")))))

(deftest parent-child-relationships
  #_(is (nil? (dsl/get-parent (_test. nil "child"))))
  (is (= "parent" (:foo (dsl/get-parent (dsl/set-parent (_test. nil "child") (_test. nil "parent"))))))
  (is (= 1 (count (dsl/save! (_test. nil "parent")))))
  ;;Create a new entity with a parent that is already stored in the datastore
  (is (= 1 (count (dsl/select (dsl/where _test ([= :foo "parent"]))))))
  (is (= 1 (count (dsl/save! (dsl/set-parent (_test. nil "child")
                                             (first (dsl/select (dsl/where _test ([= :foo "parent"])))))))))
  (is (= 1 (count (dsl/select (dsl/where _test ([= :foo "child"]))))))
  (is (= 1 (count (dsl/select (dsl/where _test ([= :foo "parent"]))))))
  (is (= "parent" (:foo (dsl/get-parent (first (dsl/select (dsl/where _test ([= :foo "child"]))))))))
  ;;create a new entity with a parent that is not stored (it will be stored automatically first, before its child)
  (is (= "anotherparent" (:foo (dsl/get-parent
                                (first (dsl/save! (dsl/set-parent (_test. nil "anotherchild")
                                                                  (_test. nil "anotherparent"))))))))
  #_(print-testdata))


(deftest test-insert-update-delete
  (is (= 0 (count (load-testdata))))
  (is (= 1 (count (dsl/save! (_test. nil "foo")))))
  (is (= 1 (count (load-testdata))))
  (is (= 2 (count (keys (first (load-testdata))))))
  (is (= "foo" (:foo (first (load-testdata)))))
  (is (not (nil? (:key (first (load-testdata))))))
  (is (string? (:key (first (load-testdata)))))
  (is (instance? com.google.appengine.api.datastore.Key (KeyFactory/stringToKey (:key (first (load-testdata))))))
  (is (< 0 (.getId (KeyFactory/stringToKey (:key (first (load-testdata)))))))
  (is (= "bar"  (:foo (first (dsl/save! (assoc (first (load-testdata)) :foo "bar"))))))
  (is (= 1 (count (load-testdata))))
  (is (= "keyname" (-> (_test. "keyname" "foo")
                       dsl/save!
                       first
                       :key
                       KeyFactory/stringToKey
                       .getName)))
  #_(print-testdata))

(deftest test-serialize
  (is (= "{\"key\":null,\"foo\":\"foo\"}" (dsl/serialize (_test. nil "foo")))))

