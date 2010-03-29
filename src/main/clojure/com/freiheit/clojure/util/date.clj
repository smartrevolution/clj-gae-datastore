(ns com.freiheit.clojure.util.date
  #^{:doc "Functions for working with dates. Internally use joda time DateTimes."}
  (:use
   [clojure.contrib test-is def])
  (:import
   [org.joda.time Minutes Hours Days Months Years DateTime Period]
   [org.joda.time DateTimeConstants DateTimeZone DateTimeComparator DateMidnight]
   [org.joda.time.format DateTimeFormat DateTimeFormatter]
   [java.util Locale]))

(defvar- *now-fn* (fn [] (DateTime.)))

(defn make-date
  "Create a new date in the default timezone."
  ([year month day]
     (make-date year month day 0 0 0 0))
  ([year month day hour minute second ms]
     (DateTime. year month day hour minute second ms)))

(defn now
  "Return the current date and time."
  []
  (*now-fn*))

(defn date-from-ms 
  "Create a date from milliseconds from epoch."
  ([ms]
     (DateTime. (long ms)))
  ([ms timezone]
     (.. (DateTime. (long ms)) (withZoneRetainFields (DateTimeZone/forID timezone)))))

(defn date-to-ms
  "Get the milliseconds from epoch for the given date."
  [date]
  (.getMillis date))

(def *date-time-comparator* (DateTimeComparator/getInstance))
(def *date-time-comparator-desc* (fn [d1 d2]
				   (. *date-time-comparator* compare d2 d1)))
				

(defn compare-dates
  "Compares two dates."
  [d1 d2]
  (.compare *date-time-comparator* d1 d2))

(defn equal?
  "True if the two dates are equal in ms (independent from their timezone)."
  [d1 d2]
  (.isEqual d1 d2))

(defn before?
  "true if the first date is before the second"
  [d1 d2]
  (neg? (compare-dates d1 d2)))

(defn after?
  "true if the first date is after the second"
  [d1 d2]
  (not (before? d1 d2)))

(defn future-date?
  "true if the given date is in the future."
  [date]
  (pos? (compare-dates date (now))))

(defn past-date?
  "true if the given date is in the past."
  [date]
  (not (future-date? date)))

(defn month-of
  "return the month of the year"
  [date]
  (.. date monthOfYear get))

(defn day-of
  "return the day of the month"
  [date]
  (.. date dayOfMonth get))

(defn year-of
  "return the year"
  [date]
  (.. date year get))

(defn minus-days
  "subtract the number of days from the date"
  [date days]
  (. date minusDays days))

(defn plus-days
  "add the number of days to the date"
  [date days]
  (. date plusDays days))

(defn same-day?
  "Are the two DateTimes on the same day?"
  [date1 date2]
  (equal? (DateMidnight. date1) (DateMidnight. date2)))

(deftest test-same-day
  (is (same-day? (make-date 2009 11 19 23 23 23 23) (make-date 2009 11 19 13 37 13 37)))
  (is (not (same-day? (make-date 2009 11 19 23 23 23 23) (make-date 2009 11 20 3 37 13 37)))))

(defn period-between
  "Return a map containing information about the period between two dates. Possible
   arguments to the function are :minutes, :hours, :days, :months or :years"
  [d1 d2]
  (if (or (nil? d1)
	  (nil? d2))
    nil
    {:minutes (.getMinutes (Minutes/minutesBetween d1 d2))
     :hours (.getHours (Hours/hoursBetween d1 d2))
     :days (.getDays (Days/daysBetween d1 d2))
     :months (.getMonths (Months/monthsBetween d1 d2))
     :years (.getYears (Years/yearsBetween d1 d2))}))

(defn nights-between
  "Return how many nights are between the two dates"
  [date1 date2]
  (.getDays (Days/daysBetween (DateMidnight. date1) (DateMidnight. date2))))

(defmacro with-now
  "Set the value that the now function should return in the given body"
  [now-date & body]
  `(binding [*now-fn* (fn [] ~now-date)]
     ~@body))

(deftest test-nights-between
  (is (= 0 (nights-between (make-date 2009 11 19 23 23 23 23) (make-date 2009 11 19 13 37 13 37))))
  (is (= 1 (nights-between (make-date 2009 11 19 23 23 23 23) (make-date 2009 11 20 3 37 13 37)))))

(deftest test-period-between
  (let [d1 (make-date 2009 1 1)
	d2 (make-date 2009 1 3)]
    (is (= 2 (:days (period-between d1 d2))))))

(deftest test-period-between-2
  (let [d1 (make-date 2009 1 1)
	d2 (make-date 2009 1 8)]
    (is (= 7 (:days (period-between d1 d2))))))

(deftest test-period-between-3
  (is (= nil (:days (period-between (now) nil))))
  (is (= nil (:days (period-between nil (now))))))

(deftest test-comparison
  (let [now (DateTime.)
	future (.plusMinutes now 30)
	past (.minusMinutes now 30)]
    (is (before? now future))
    (is (after? now past))
    (is (future-date? future))
    (is (past-date? past))))

(deftest test-date-accessors
  (let [d (make-date 2009 3 4)]
    (is (= 2009 (year-of d)))
    (is (= 3 (month-of d)))
    (is (= 4 (day-of d)))))

(defn formatter-for-pattern
  [pattern]
  (DateTimeFormat/forPattern pattern))

(defvar *german-date-formatter*
  (DateTimeFormat/forPattern "d. MMMM yyyy 'um' k:mm 'Uhr'")
  "A formatter for formatting a date from and to \"d. MMMM yyyy 'um' k:mm 'Uhr'\".")

(defvar *german-date-input-formatter*
  (DateTimeFormat/forPattern "d.MM.yyyy k:m")
  "A formatter for formatting a date from and to \"d.MM.yyyy k:m\"")

(defvar *english-date-formatter*
  (DateTimeFormat/forPattern "k:m MMMM d")
  "A formatter for formatting a date from and to \"k:m MMMM d\"")

(defvar *rfc-1123-date-formatter*
  (..(DateTimeFormat/forPattern "EEE, dd MMM yyyy HH:mm:ss 'GMT'")
     (withLocale Locale/US)
     (withZone DateTimeZone/UTC))
  "A formatter for formatting a date from and to RFC1123 that is e.g. used in HTTP headers.")

(defn withTimeZone
  [formatter timezone]
  (.withZone formatter (DateTimeZone/forID timezone)))

(defn getAllTimeZones
  []
  (DateTimeZone/getAvailableIDs))

(defmulti format-date "Format a date with the given formatter or string format." (fn [date formatter & _] (class formatter)))

(defmethod format-date DateTimeFormatter
  ([date formatter locale]
     (.print (.withLocale formatter locale) date))
  ([date formatter]
     (.print formatter date)))

(defmethod format-date String
  ([date format-string locale]
     (.print (.withLocale (formatter-for-pattern format-string) locale) date))
  ([date format-string]
     (.print (formatter-for-pattern format-string) date)))

(defvar *default-english-formatter*
  {:less-than-minute (fn [period date] "less than one minute ago")
   :less-than-hour (fn [period date] (str (.getMinutes period) " minute(s) ago"))
   :less-than-day (fn [period date] (str (.getHours period) " hour(s) ago"))
   :less-than-week (fn [period date] (str (.getDays period) " day(s) ago"))
   :more-than-week (fn [period date] (str (.getDays period) " day(s) ago"))}
  "A period formatter for readable english periods.")

(defvar *default-german-formatter*
  {:less-than-minute (fn [period date] "vor weniger als einer Minute")
   :less-than-hour (fn [period date] (str "vor " (.getMinutes period) " Minute(n)"))
   :less-than-day (fn [period date] (str "vor " (.getHours period) " Stunde(n)"))
   :less-than-week (fn [period date] (str "vor " (.getDays period) " Tage(n)"))
   :more-than-week (fn [period date] "")}
  "A formatter for readable german periods.")

(defn- full-seconds-for-period
  [period]
  (+ (.getSeconds period) 
     (* (.getMinutes period) DateTimeConstants/SECONDS_PER_MINUTE)
     (* (.getHours period) DateTimeConstants/SECONDS_PER_HOUR)
     (* (.getDays period) DateTimeConstants/SECONDS_PER_DAY)
     (* (.getWeeks period) DateTimeConstants/SECONDS_PER_WEEK)))
     
(defn human-readable-period
  "Format a period in a human readable format. Implicitly takes the current time as a base
   for calculating the period. The output can be formatted with a given formatter."
  ([date]
     (human-readable-period date (DateTime.) *default-english-formatter*))
  ([date now formatter]
     (let [period (Period. date now)
	   period-secs (full-seconds-for-period period)
	   {format-less-than-minute :less-than-minute
	    format-less-than-hour :less-than-hour
	    format-less-than-day :less-than-day
	    format-less-than-week :less-than-week
	    format-more-than-week :more-than-week} formatter]
	 (cond (< period-secs DateTimeConstants/SECONDS_PER_MINUTE)
	       (format-less-than-minute period date)
	       (< period-secs DateTimeConstants/SECONDS_PER_HOUR)
	       (format-less-than-hour period date)
	       (< period-secs (* 24 DateTimeConstants/SECONDS_PER_HOUR))
	       (format-less-than-day period date)
	       (< period-secs (* 7 DateTimeConstants/SECONDS_PER_DAY))
	       (format-less-than-week period date)
	       true
	       (format-more-than-week period date)))))	     

(deftest test-human-readable-period
  (let [now (DateTime.)
	now+30s (.plusSeconds now 30)
	now+90s (.plusSeconds now 90)
	now+1d (.plusDays now 1)]
  (is (= (human-readable-period now now+30s
				*default-english-formatter*) "less than one minute ago"))
  (is (= (human-readable-period now now+90s
				*default-english-formatter*) "1 minute(s) ago"))
  (is (= (human-readable-period now now+1d
				*default-english-formatter*) "1 day(s) ago"))))

(defn parse-date
  "Parse a date from a string with the given formatter. Uses the
  *german-date-input-formatter* as a default."
  ([string]
     (parse-date string *german-date-input-formatter*))
  ([string formatter]
     (.parseDateTime formatter string))
  ([string formatter timezone]
     (.. (.parseDateTime formatter string)
	 (withZoneRetainFields (DateTimeZone/forID timezone)))))

(defn get-midnight
  "Return a datetime that is the same date as the one given as parameter but the time is set to 0:00 (midnight)"
  [date]
  (when date
    (.toDateMidnight date)))
