(ns elastica.query.aggregations
  (:require [utilis.map :refer [compact]]))

(defn significant-terms
  [field & {:keys [name] :or {name :significant_terms_agg}}]
  {:pre [field name]}
  {name {:significant_terms {:field field}}})

(defn terms
  [field & {:keys [name] :or {name :terms_agg}}]
  {name {:terms {:field field}}})

(defn date-histogram
  [field & {:keys [name interval]
            :or {name :date_histogram_agg
                 interval :month}}]
  {name
   {:date_histogram
    {:field field
     :interval interval}}})

(defn significant-text
  [field & {:keys [name filter-duplicate-text]
            :or {name :significant_text_agg}}]
  {:pre [field name]}
  (compact
   {name
    {:significant_text
     {:field field
      :filter_duplicate_text filter-duplicate-text}}}))

(defn sampler
  [& {:keys [shard-size name aggregations]
      :or {shard-size 100
           name :sampler_agg}}]
  (compact
   {name
    {:sampler {:shard_size shard-size}
     :aggregations aggregations}}))

(defn geo-centroid
  [& {:keys [name field]
      :or {name :centroid}}]
  {name {:geo_centroid {:field field}}})

(defn geohash-grid
  [& {:keys [name field precision size]
      :or {name :grid}}]
  (compact
   {name
    {:geohash_grid
     {:field field
      :precision precision
      :size size}}}))

(defn bounded-geohash-grid
  [& {:keys [name geohash-grid bounding-box]
      :or {name :bounded-grid}}]
  {name
   {:filter bounding-box
    :aggregations geohash-grid}})
