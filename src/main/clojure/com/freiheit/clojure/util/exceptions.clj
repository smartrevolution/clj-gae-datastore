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

(ns
    #^{:doc "Utility functions for handling java exceptions."}
  com.freiheit.clojure.util.exceptions)

;; ------------------------------------------------------------------------------
;; public functions
;; ------------------------------------------------------------------------------

(defn get-stack-trace
  "Get the string representation of the strack trace for the given Throwable."
  [#^java.lang.Throwable t]
  (with-open [#^java.io.StringWriter string-writer (java.io.StringWriter.)
              #^java.io.PrintWriter print-writer (java.io.PrintWriter. string-writer true)]
    (do
      (.printStackTrace t print-writer)
      (str string-writer))))

(defmacro with-retry
  "Execute the body for the given amount of times when exception 'exception-class' occured."
  [amount exception-class & body]
  (let [dec-amount (dec amount)]
    (if (= 0 amount)
      `(do ~@body)
      `(try
	(do ~@body)
	(catch ~exception-class e#
	  (do
	    (with-retry ~dec-amount ~exception-class ~@body)))))))
