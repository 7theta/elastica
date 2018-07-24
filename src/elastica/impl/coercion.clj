;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://www.eclipse.org/legal/epl-v10.html)
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns elastica.impl.coercion
  "Functions for converting between native clojure types and those used by
  Elasticsearch.

  These functions are an implementation detail and should not be called directly
  by consumers of the library."
  (:require [utilis.fn :refer [fsafe]]
            [inflections.core :refer [underscore dasherize]]))

(defn ->es-key
  [k]
  (when k
    (->> k name
         underscore
         (str (when-let [ns (when (keyword? k) (namespace k))]
                (str ns "/"))))))

(defn es-key->
  [k]
  (let [k (if (keyword? k) (name k) k)]
    (when (and k (string? k))
      (if-not (re-find #"^_" k)
        (->> k dasherize keyword)
        (keyword k)))))

(defn ->es
  "Coerces v to a value that can be passed to elasticsearch without error"
  [v]
  (cond
    (map? v) (reduce-kv (fn [m k v] (assoc m (->es-key (->es k)) (->es v))) {} v)
    (coll? v) (mapv ->es v)
    (or (keyword? v) (symbol? v)) (name v)
    :default v))

(defn es->
  "Coerces v from an es value back to an idiomatic clojure value"
  [v]
  (cond
    (map? v) (reduce-kv (fn [m k v] (assoc m (es-key-> (es-> k)) (es-> v))) {} v)
    (coll? v) (mapv es-> v)
    :default v))
