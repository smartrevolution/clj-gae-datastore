(ns
    #^{:doc "Utility functions for handling java exceptions."}
  com.freiheit.clojure.util.exceptions)

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
