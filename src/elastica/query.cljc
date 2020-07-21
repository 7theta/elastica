;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://www.eclipse.org/legal/epl-v10.html)
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns elastica.query
  "Functions for the generation of queries that can be used with
  elastica.core/search"
  (:refer-clojure :exclude [range type])
  (:require [elastica.impl.coercion :refer [->es ->es-key es-> es-key->]]
            [utilis.types.keyword :refer [->keyword]]
            [utilis.map :refer [assoc-if compact map-keys]]
            [utilis.fn :refer [fsafe]]
            [clojure.string :as st]))

(defn bool
  "A query that can be used to combine other queries. The :must, :should
  and :must-not parameters can be used to control the combination and can
  be specified as single queries or collections of queries.

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html"
  [& {:keys [must should must-not filter
             boost minimum-should-match]}]
  (let [must (when must (if (coll? must) must [must]))
        should (when should (if (coll? should) should [should]))
        must-not (when must-not (if (coll? must-not) must-not [must-not]))
        filter (when filter (if (coll? filter) filter [filter]))]
    (compact
     {:bool {:must must
             :should should
             :must_not must-not
             :filter filter
             :boost boost
             :minimum_should_match minimum-should-match}})))

(defn boosting
  "Can be used to effectively demote results that match 'query'. Unlike the
   :must-not clause in bool query, this still selects documents that contain
  undesirable terms, but reduces their overall score

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-boosting-query.html"
  [& {:keys [positive-query negative-query
             positive-boost negative-boost]}]
  {:pre [(or positive-query negative-query)]}
  (compact
   {:boosting {:positive positive-query
               :positive_boost positive-boost
               :negative negative-query
               :negative_boost negative-boost}}))

(defn constant-score
  "Wraps another query and returns a constant score equal to the boost for
  every document.

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-constant-score-query.html"
  [query boost]
  {:constant_score {:filter query
                    :boost boost}})

(defn decay-exponential
  [field origin scale & {:keys [offset decay multi-value-mode]}]
  {:pre [(#{:min :max :avg :sum} multi-value-mode)]}
  (compact
   {:exp {field {:origin origin
                 :scale scale
                 :offset offset
                 :decay decay}
          :multi_value_mode (name multi-value-mode)}}))

(defn decay-gauss
  [field origin scale & {:keys [offset decay multi-value-mode]}]
  {:pre [(#{:min :max :avg :sum} multi-value-mode)]}
  (compact
   {:gauss {field {:origin origin
                   :scale scale
                   :offset offset
                   :decay decay}
            :multi_value_mode (name multi-value-mode)}}))

(defn decay-linear
  [field origin scale & {:keys [offset decay multi-value-mode]}]
  {:pre [(#{:min :max :avg :sum} multi-value-mode)]}
  (compact
   {:linear {field {:origin origin
                    :scale scale
                    :offset offset
                    :decay decay}
             :multi_value_mode (name multi-value-mode)}}))

(defn dis-max
  "Generates the union of documents produced by its subqueries, and scores each
  document with the maximum score for that document as produced by any sub query,
  plus a tie breaking increment for any additional matching sub queries

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-dis-max-query.html"
  [queries & {:keys [boost tie-breaker]}]
  (compact
   {:dis_max {:tie_breaker tie-breaker
              :boost boost
              :queries queries}}))

(defn exists
  "Returns documents that have at least one non-null value in 'field'

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-exists-query.html"
  [field]
  {:exists {:field field}})

(defn field-value-factor
  "Uses the value of a field to influence the overall score of the document.

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html#function-field-value-factor"
  [field & {:keys [factor missing modifier]
            :or {modifier :none}}]
  {:pre [(#{:ln :ln1p :ln2p :log :log1p :log2p :none :reciprocal :sqrt :square} modifier)]}
  (compact
   {:field_value_factor {:field field
                         :factor factor
                         :modifier (name modifier)
                         :missing missing}}))

(defn function-score
  "Modifies the score of documents that are retrieved by 'query'. This can be
  useful if, for example, a score function is computationally expensive and
  it is sufficient to compute the score on a filtered set of documents.

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html"
  [query functions & {:keys [score-mode min-score
                             boost-mode max-boost
                             boost]}]
  {:pre [(or (nil? score-mode) (#{:multiply :sum :avg :first :max :min} score-mode))
         (or (nil? boost-mode) (#{:multiply :replace :sum :avg :max :min} boost-mode))]}
  (compact
   {:function_score
    {:query query
     :boost boost
     :functions functions
     :score-mode score-mode
     :min-score min-score
     :boost-mode boost-mode
     :max-boost max-boost}}))

(defn fuzzy
  "The fuzzy query generates all possible matching terms that are within the
  maximum edit distance specified in fuzziness and then checks the term
  dictionary to find out which of those generated terms actually exist in
  the index

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-fuzzy-query.html"
  [field value & {:keys [boost
                         fuzziness
                         prefix-length max-expansions]
                  :or {prefix-length 0
                       max-expansions 50}}]
  (compact
   {:fuzzy {field (if (or boost fuzziness prefix-length max-expansions)
                    {:value value
                     :boost boost
                     :fuzziness fuzziness
                     :prefix-length prefix-length
                     :max-expansions max-expansions}
                    value)}}))

(defn has-child
  "Finds parents documents whose children match the provided 'query'

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-has-child-query.html"
  [type query & {:keys [min-children max-children score-mode boost
                        ignore-unmapped]
                 :or {score-mode :none}}]
  {:pre [(#{:none :avg :min :max :sum} score-mode)]}
  {:has_child {:type type
               :query query
               :min_children min-children
               :max_children max-children
               :score_mode (name score-mode)
               :ignore_unmapped ignore-unmapped
               :boost boost}})

(defn has-parent
  "Finds child documents whose parents march the provided 'query'

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-has-parent-query.html"
  [parent-type query & {:keys [score ignore-unmapped boost]}]
  {:has_parent {:parent_type parent-type
                :query query
                :score score
                :ignore_unmapped ignore-unmapped
                :boost boost}})

(defn ids
  "Finds documents matching the provided sequence of IDs

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-ids-query.html"
  [id-list]
  {:ids {:values (map name id-list)}})

(defn match
  "A family of match queries that accepts text/numerics/dates, analyzes them,
  and constructs a query. There are 3 types of match queries :boolean, :phrase
  and :phrase-prefix. The default match query type is :boolean.

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html"
  [field text & {:keys [phrase
                        phrase-prefix
                        operator analyzer lenient fuzziness
                        zero-terms-query cutoff-frequency
                        max-expansions prefix-length
                        type
                        minimum-should-match boost]
                 :or {lenient false
                      zero-terms-query :none}}]
  (compact
   {(cond
      phrase :match_phrase
      phrase-prefix :match_phrase_prefix
      :else :match)
    {field (if (or operator
                   zero-terms-query
                   cutoff-frequency
                   analyzer
                   fuzziness
                   phrase)
             (merge
              {:query text
               :zero_terms_query ((fsafe clojure.core/name) zero-terms-query)
               :max_expansions max-expansions
               :prefix_length prefix-length
               :operator ((fsafe clojure.core/name) operator)
               :cutoff_frequency cutoff-frequency
               :analyzer analyzer
               :boost boost
               :fuzziness (if (keyword? fuzziness)
                            (st/upper-case (name fuzziness))
                            fuzziness)})
             text)}}))

(defn match-all
  "The simplest query that matches all documents giving them each
  a _score of 1.0. The _score can be adjusted by providing a 'boost'
  value.

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-all-query.html"
  [& {:keys [boost]}]
  {:match_all (assoc-if {} :boost boost)})

(defn match-none
  "This is the inverse of the match-all query, which matches no documents.

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-all-query.html"
  [& {:keys [boost]}]
  {:match_none {}})

(defn nested
  "Finds root documents based on executing queries against the nested objects.

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-nested-query.html"
  [path query & {:keys [score-mode boost]
                 :or {score-mode :avg}}]
  {:pre [(#{:none :avg :min :max :sum} score-mode)]}
  {:nested {:path path
            :score_mode score-mode
            :query query
            :boost boost}})

(defn parent-id
  "Query used to find child documents which belong to a particular parent

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-parent-id-query.html"
  [child-type id & {:keys [ignore-unmapped]}]
  {:parent_id {:type child-type
               :id id
               :ignore_unmapped ignore-unmapped}})

(defn prefix
  "Matches documents that have fields containing terms with a specified prefix

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-prefix-query.html"
  [field prefix & {:keys [boost]}]
  {:prefix {field (if boost {:value prefix :boost boost} prefix)}})

(defn random-score
  "The random_score generates scores that are uniformly distributed
  in [0, 1[. By default, it uses the internal Lucene doc ids as a
  source of randomness, which is very efficient but unfortunately not
  reproducible since documents might be renumbered by merges.

  In case you want scores to be reproducible, it is possible to
  provide a seed and field. If 'seed' is not provided, the current
  time is used."
  [& {:keys [seed field]}]
  (compact
   {:random_score {:seed seed
                   :field field}}))

(defn range
  "Matches documents with fields that have terms within a certain range.

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html"
  [field & {:keys [lt lte gt gte
                   time-zone date-format
                   boost]
            :or {include-lower true
                 include-upper true}}]
  (compact
   {:range {field {:lt lt :lte lte :gt gt :gte gte
                   :time_zone time-zone
                   :format date-format
                   :boost boost}}}))

(defn regexp
  "The regexp query allows you to use regular expression term queries

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-regexp-query.html"
  [field regex  & {:keys [boost flags]}]
  [{:pre [(string? regex)
          (#{:any-string :complement :empty :intersection :interval :none} flags)]}]
  (compact
   {:regexp {field (if (or boost flags)
                     {:value regex
                      :boost boost
                      :flags (st/join "|" (map {:any-string "ANYSTRING"
                                                :complement "COMPLEMENT"
                                                :empty "EMPTY"
                                                :intersection "INTERSECTION"
                                                :interval "INTERVAL"
                                                :none "NONE"} flags))}
                     regex)}}))

(defn term
  "The term query finds documents that contain the exact term specified
  in the inverted index.

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-term-query.html"
  [field value & {:keys [boost]}]
  {:term {field (if boost {:value value :boost boost} value)}})

(defn terms
  "Finds documents that have fields that match any of the provided terms

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html"
  [field values & {:keys [index type id path]}]
  (compact
   {:terms {field (if (or index type id path)
                    {:index index
                     :type type
                     :id id
                     :path path}
                    values)}}))

(defn type
  "Finds documents matching the provided mapping type

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-type-query.html"
  [mapping-type]
  {:type (:value mapping-type)})

(defn wildcard
  "Matches documents that have fields matching a wildcard expression. Supported
  wildcards are *, which matches any character sequence (including the empty one),
  and ?, which matches any single character.

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html"
  [field wildcard-expression & {:keys [boost]}]
  {:wildcard {field (if boost {:value wildcard-expression :boost boost} wildcard-expression)}})

(defn weight
  "Multiplies the score with 'weight'"
  [weight]
  {:weight weight})

(defn more-like-this
  "Find documents similar to those provided in the 'like' parameter, different
  from those provided in the 'unlike' parameter, on the 'fields' provided.

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-mlt-query.html"
  [like & {:keys [unlike
                  fields
                  min-term-freq
                  max-query-terms]
           :or {min-term-freq 1
                max-query-terms 12}}]
  {:pre [(or (string? like)
             (and (coll? like)
                  (every? #(or (map? %)
                               (string? %))
                          like)))]}
  (compact
   {:more_like_this
    (merge
     (when (seq fields)
       {:fields (map ->es-key fields)})
     {:like (if (string? like)
              like
              (map (fn [like]
                     (cond
                       (string? like) like
                       (map? like)
                       (-> (map-keys ->keyword like)
                           (update :doc (fsafe ->es))
                           (update :_type (fsafe ->es-key))
                           (update :_index (fsafe ->es-key))
                           compact)
                       :else like)) like))
      :unlike unlike
      :min_term_freq min-term-freq
      :max_query_terms max-query-terms})}))

(defn suggest
  "The completion suggester provides auto-complete/search-as-you-type
  functionality. This is a navigational feature to guide users to relevant
  results as they are typing, improving search precision. It is not meant for
  spell correction or did-you-mean functionality like the term or phrase
  suggesters.

  https://www.elastic.co/guide/en/elasticsearch/reference/current/search-suggesters-completion.html"
  [field prefix & {:keys [name fuzziness] :or {name :suggest}}]
  (compact
   {:suggest
    {name
     {:prefix prefix
      :completion {:field field
                   :fuzzy {:fuzziness fuzziness}}}}}))

(defn percolate
  "The percolate query can be used to match queries stored in an index. The
  percolate query itself contains the document that will be used as query to
  match with the stored queries.

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-percolate-query.html"
  [field & {:keys [name document documents index id routing preference version]}]
  (compact
   {:percolate
    (merge {:field field
            :name name}
           (cond
             document {:document document}
             documents {:documents documents}
             (and id index) {:id id
                             :index index
                             :routing routing
                             :preference preference
                             :version version}))}))
