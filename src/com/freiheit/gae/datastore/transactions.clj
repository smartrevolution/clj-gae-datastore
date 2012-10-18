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

(ns com.freiheit.gae.datastore.transactions
  (:require
   [clojure.tools.logging :as log])
  (:import
   [com.google.appengine.api.datastore DatastoreServiceFactory TransactionOptions$Builder]))

;;;; Transaction support for the appengine.

;; ------------------------------------------------------------------------------
;; public functions
;; ------------------------------------------------------------------------------
(defn begin-transaction
  "Begin a datastore transaction. Returns a new transaction."
  ([]
     (.beginTransaction (DatastoreServiceFactory/getDatastoreService)))
  ([options]
     (.beginTransaction (DatastoreServiceFactory/getDatastoreService) options)))

(defn commit-transaction
  "Commit a transaction."
  [transaction]
  (.commit transaction))

(defn rollback-transaction
  "Rollback a transaction."
  [transaction]
  (.rollback transaction))

(defn current-transaction
  "Return the current transaction."
  []
  (.getCurrentTransaction (DatastoreServiceFactory/getDatastoreService)))

(defmacro with-transaction
  "Encapsulate code with a datastore transaction. The transaction is commited at the end."
  [& body]
  `(let [transaction# (begin-transaction)]
     (let [body# (do ~@body)]
       (commit-transaction transaction#)
       body#)))

(defmacro with-xg-transaction
  "Encapsulate code with a cross group datastore transaction. The transaction is commited at the end."
  [& body]
  `(let [transaction# (begin-transaction (TransactionOptions$Builder/withXG true))]
     (let [body# (do ~@body)]
       (commit-transaction transaction#)
       body#)))

(defn get-stack-trace
  "Get the string representation of the strack trace for the given Throwable."
  [#^java.lang.Throwable t]
  (with-open [#^java.io.StringWriter string-writer (java.io.StringWriter.)
              #^java.io.PrintWriter print-writer (java.io.PrintWriter. string-writer true)]
    (do
      (.printStackTrace t print-writer)
      (str string-writer))))

(defmacro with-transaction-rethrow
  "Encapsulate code with a datastore transaction. The transaction is commited at the end. If an exception
   has occured then the transaction is rolled back. The exception is rethrown."
  [& body]
  `(let [transaction# (begin-transaction)]
     (try
	(let [body# (do ~@body)]
          (commit-transaction transaction#)
          body#)
	(catch Exception e#
          (log/log :error (str "Exception during transaction.\n\n" (get-stack-trace e#)))
          (when (.isActive transaction#)
            (rollback-transaction transaction#))
          (throw e#)))))

(defmacro with-xg-transaction-rethrow
  "Encapsulate code with a datastore transaction. The transaction is commited at the end. If an exception
   has occured then the transaction is rolled back. The exception is rethrown."
  [& body]
  `(let [transaction# (begin-transaction (TransactionOptions$Builder/withXG true))]
     (try
	(let [body# (do ~@body)]
          (commit-transaction transaction#)
          body#)
	(catch Exception e#
          (log/log :error (str "Exception during transaction.\n\n" (get-stack-trace e#)))
          (when (.isActive transaction#)
            (rollback-transaction transaction#))
          (throw e#)))))
