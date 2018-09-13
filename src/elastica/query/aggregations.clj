(ns elastica.query.aggregations
  (:require [elastica.impl.coercion :refer [->es-key]]
            [utilis.map :refer [compact map-vals]]))

;;; Declarations

(declare xform-field)

;;; Public

(defn significant-terms
  [field & {:keys [name] :or {name :significant_terms_agg}}]
  {:pre [field name]}
  (xform-field {name {:significant_terms {:field field}}}))

(defn terms
  [field & {:keys [name] :or {name :terms_agg}}]
  (xform-field {name {:terms {:field field}}}))

(defn date-histogram
  [field & {:keys [name interval]
            :or {name :date_histogram_agg
                 interval :month}}]
  (xform-field
   {name
    {:date_histogram
     {:field field
      :interval interval}}}))

(defn significant-text
  [field & {:keys [name filter-duplicate-text]
            :or {name :significant_text_agg}}]
  {:pre [field name]}
  (xform-field
   (compact
    {name
     {:significant_text
      {:field field
       :filter_duplicate_text filter-duplicate-text}}})))

(defn sampler
  [& {:keys [shard-size name aggregations]
      :or {shard-size 100
           name :sampler_agg}}]
  (xform-field
   (compact
    {name
     {:sampler {:shard_size shard-size}
      :aggregations aggregations}})))

(defn geo-centroid
  [& {:keys [name field]
      :or {name :centroid}}]
  (xform-field
   {name {:geo_centroid {:field field}}}))

(defn geohash-grid
  [& {:keys [name field precision size]
      :or {name :grid}}]
  (xform-field
   (compact
    {name
     {:geohash_grid
      {:field field
       :precision precision
       :size size}}})))

(defn bounded-geohash-grid
  [& {:keys [name geohash-grid bounding-box]
      :or {name :bounded-grid}}]
  (xform-field
   {name
    {:filter bounding-box
     :aggregations geohash-grid}}))

;;; Private

(defn- xform-field
  [m]
  (cond

    (map? m)
    (->> m
         (map (fn [[k v]] [k (if (= :field k) (->es-key v) v)]))
         (into {})
         (map-vals xform-field))

    (coll? m)
    (->> m (map xform-field)
         (into (empty m)))

    :else m))
