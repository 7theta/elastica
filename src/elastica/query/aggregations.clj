(ns elastica.query.aggregations
  (:require [utilis.map :refer [compact]]))

(defn significant-terms
  [field & {:keys [name] :or {name :significant_terms_agg}}]
  {:pre [field name]}
  {name {:significant_terms {:field field}}})

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
