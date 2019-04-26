(ns elastica.impl.valid
  #?(:clj (:refer-clojure :exclude [uri?]))
  #?(:clj (:import [org.apache.commons.validator.routines UrlValidator])))

(defn url?
  [url-str]
  #?(:clj
     (.isValid (UrlValidator. UrlValidator/ALLOW_LOCAL_URLS) (str url-str))
     :cljs
     (re-seq
      #"^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]"
      (str url-str))))

(defn uri?
  [uri-str]
  (url? (str "http://localhost/" uri-str)))
