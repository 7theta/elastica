(ns elastica.impl.url
  (:require [utilis.map :refer [compact]]
            [utilis.types.string :refer [->string]]
            [clojure.string :as st])
  (:require [clojure.string :as st]))

(defn query-string
  [m]
  {:pre [(or (nil? m)
             (empty? m)
             (map? m))]}
  (when (seq (compact m))
    (->> m
         (map (fn [[k v]] (str (name k) "=" v)))
         (st/join "&")
         (str "?"))))

(defn base-url
  [method hostname port]
  {:pre [(and method hostname)]}
  (str (name method) "://" hostname (when port (str ":" port))))

(defn url
  [& [{:keys [method hostname port query segments]
       :or {method :http
            hostname "localhost"}}]]

  (str
   (base-url method hostname port) "/"
   (->> segments
        (mapcat #(st/split (str %) #"/"))
        (st/join "/"))
   (query-string query)))
