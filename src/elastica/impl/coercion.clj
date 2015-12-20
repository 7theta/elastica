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
  (:require [utilis.coll :refer [seqable?]]
            [utilis.map :refer [map-vals]])
  (:import  [org.elasticsearch.common.collect ImmutableOpenMap]
            [org.elasticsearch.cluster.metadata MappingMetaData]
            [java.util Map HashMap LinkedHashMap]))

(defn ->es-value
  "Coerces v to a value that can be passed to elasticsearch without error"
  [v]
  (cond
    (map? v) (reduce-kv (fn [m k v] (assoc m (->es-value k) (->es-value v))) {} v)
    (coll? v) (mapv ->es-value v)
    (or (keyword? v) (symbol? v)) (name v)
    :default v))

(defn ->clj-value
  "Coerce the supplied value to a clojure value as appropriate.
  If the value cannot be coerced, it is returned unchanged.

  For associate structures keys->keyword can be used to indicate
  whether the keys should be coerced to keywords"
  [v & {:keys [keys->keyword] :or {keys->keyword true}}]
  (cond
    (instance? ImmutableOpenMap v)
    (let [^ImmutableOpenMap v v]
      (->> v .keysIt iterator-seq
           (reduce (fn [hm k] (assoc! hm (cond-> k keys->keyword keyword)
                                     (->clj-value (.get v k)
                                                  :keys->keyword keys->keyword)))
                   (transient {}))
           persistent!))

    (instance? MappingMetaData v)
    (let [^MappingMetaData v v]
      (->clj-value (.sourceAsMap v) :keys->keyword keys->keyword))

    (or (instance? HashMap v) (instance? LinkedHashMap v))
    (into {} (map (fn [e] [(cond-> (key e) keys->keyword keyword)
                          (->clj-value (val e) :keys->keyword keys->keyword)]) v))

    (and (seqable? v) (not (map? v)) (not (string? v)))
    (mapv #(->clj-value % :keys->keyword keys->keyword) v)

    :default v))
