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
  by consumers of the library.")

(defn ->es-value
  "Coerces v to a value that can be passed to elasticsearch without error"
  [v]
  (cond
    (map? v) (reduce-kv (fn [m k v] (assoc m (->es-value k) (->es-value v))) {} v)
    (coll? v) (mapv ->es-value v)
    (or (keyword? v) (symbol? v)) (name v)
    :default v))
