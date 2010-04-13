(ns com.freiheit.clojure.util.genapidoc
  (:use clojure.contrib.gen-html-docs))

(defn gen-all-docs
  ([path]
     (generate-documentation-to-file path
                                     ['com.freiheit.gae.datastore.datastore-query-dsl
                                      'com.freiheit.gae.datastore.datastore-access-dsl
                                      'com.freiheit.gae.datastore.datastore-types])))

(when (not-empty *command-line-args*)
  (gen-all-docs (first *command-line-args*)))
  