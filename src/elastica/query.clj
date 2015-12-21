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
  (:require [elastica.impl.coercion :refer [->es-value]])
  (:import  [org.elasticsearch.index.query QueryBuilders
             BoolQueryBuilder DisMaxQueryBuilder
             MatchAllQueryBuilder MatchQueryBuilder
             MatchQueryBuilder$Operator MatchQueryBuilder$ZeroTermsQuery MatchQueryBuilder$Type
             TermQueryBuilder TermsQueryBuilder
             RangeQueryBuilder ExistsQueryBuilder MissingQueryBuilder
             PrefixQueryBuilder WildcardQueryBuilder RegexpQueryBuilder RegexpFlag
             FuzzyQueryBuilder TypeQueryBuilder IdsQueryBuilder
             NestedQueryBuilder HasChildQueryBuilder HasParentQueryBuilder]
            [org.elasticsearch.common.unit Fuzziness]))

(defn bool
  "A query that can be used to combine other queries. The :must, :should
  and :must-not parameters can be used to control the combination and can
  be specified as single queries or collections of queries.

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-bool-query.html"
  [& {:keys [query-name
             must should must-not
             filter
             boost minumum-should-match]}]
  (let [must (when must (if (coll? must) must [must]))
        should (when should (if (coll? should) should [should]))
        must-not (when must-not (if (coll? must-not) must-not [must-not]))
        filter (when filter (if (coll? filter) filter [filter]))]
    (cond-> (BoolQueryBuilder.)
      query-name (.queryName query-name)
      must ((fn [^BoolQueryBuilder q] (doseq [m must] (.must q m)) q))
      should ((fn [^BoolQueryBuilder q] (doseq [s should] (.should q s)) q))
      must-not ((fn [^BoolQueryBuilder q] (doseq [mn must-not] (.mustNot q mn)) q))
      filter ((fn [^BoolQueryBuilder q] (doseq [f filter] (.mustNot q f)) q)))))

(defn dis-max
  "Generates the union of documents produced by its subqueries, and scores each
  document with the maximum score for that document as produced by any sub query,
  plus a tie breaking increment for any additional matching sub queries

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-dis-max-query.html"
  [queries & {:keys [boost tie-breaker]}]
  (let [^DisMaxQueryBuilder query (reduce (fn [^DisMaxQueryBuilder qb q] (.add qb q))
                                          (DisMaxQueryBuilder.) queries)]
    (cond-> ^DisMaxQueryBuilder query
      boost (.boost ^double boost)
      tie-breaker (.tieBreaker ^double tie-breaker))))

(defn match-all
  "The simplest query that matches all documents giving them each
  a _score of 1.0. The _score can be adjusted by providing a 'boost'
  value.

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-match-all-query.html"
  [& {:keys [boost]}]
  (cond-> (MatchAllQueryBuilder.)
    boost (.boost ^double boost)))

(defn match
  "A family of match queries that accepts text/numerics/dates, analyzes them,
  and constructs a query. There are 3 types of match queries :boolean, :phrase
  and :phrase-prefix. The default match query type is :boolean.

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-match-query.html"
  [field text & {:keys [query-name
                        operator analyzer lenient fuzziness
                        zero-terms-query cutoff-frequency
                        query-type
                        minimum-should-match boost]
                 :or {operator :or
                      lenient false
                      zero-terms-query :none}}]
  {:pre [(#{:or :and} operator)
         (#{:all :none} zero-terms-query)]}
  (cond-> (MatchQueryBuilder. (->es-value field) (->es-value text))
    query-name (.queryName query-name)
    (= :and operator) (.operator MatchQueryBuilder$Operator/AND)
    analyzer (.analyzer analyzer)
    lenient (.setLenient true)
    (= :all zero-terms-query) (.zeroTermsQuery MatchQueryBuilder$ZeroTermsQuery/ALL)
    cutoff-frequency (.cutoffFrequency cutoff-frequency)
    query-type (.type (case query-type
                        :boolean (MatchQueryBuilder$Type/BOOLEAN)
                        :phrase (MatchQueryBuilder$Type/PHRASE)
                        :phrase-prefix (MatchQueryBuilder$Type/PHRASE_PREFIX)))
    minimum-should-match (.minimumShouldMatch minimum-should-match)
    boost (.boost ^double boost)))

(defn term
  "The term query finds documents that contain the exact term specified
  in the inverted index.

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-term-query.html"
  [field value & {:keys [query-name boost]}]
  (cond-> (TermQueryBuilder. ^String (->es-value field) (->es-value value))
    query-name (.queryName query-name)
    boost (.boost ^double boost)))

(defn terms
  "Finds documents that have fields that match any of the provided terms

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-terms-query.html"
  [field values & {:keys [query-name boost]}]
  (cond-> (TermsQueryBuilder. ^String (->es-value field)
                              (into-array (map ->es-value values)))
    query-name (.queryName query-name)
    boost (.boost ^double boost)))

(defn range
  "Matches documents with fields that have terms within a certain range.

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-range-query.html"
  [field & {:keys [query-name
                   from to
                   lt lte gt gte
                   include-lower include-upper
                   time-zone date-format
                   boost]
            :or {include-lower true
                 include-upper true}}]
  (cond-> (RangeQueryBuilder. (->es-value field))
    query-name (.queryName query-name)
    from (.from from)
    to (.to to)
    lt (.lt lt)
    lte (.lte lte)
    gt (.gt gt)
    gte (.gte gte)
    include-lower (.includeLower include-lower)
    include-upper (.includeUpper include-upper)
    time-zone (.timeZone time-zone)
    date-format (.format date-format)
    boost (.boost ^double boost)))

(defn exists
  "Returns documents that have at least one non-null value in 'field'

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-exists-query.html"
  [field & {:keys [query-name]}]
  (cond-> (ExistsQueryBuilder. (->es-value field)) query-name (.queryName query-name)))

(defn missing
  "Returns documents that have only null values or no value in 'field'

  http://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-missing-query.html"
  [field & {:keys [query-name existence null-value]
            :or {existence true
                 null-value true}}]
  (cond-> (MissingQueryBuilder. (->es-value field))
    query-name (.queryName query-name)
    existence (.existence existence)
    null-value (.nullValue null-value)))

(defn prefix
  "Matches documents that have fields containing terms with a specified prefix

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-prefix-query.html"
  [field prefix & {:keys [query-name boost]}]
  (cond-> (PrefixQueryBuilder. (->es-value field) prefix)
    query-name (.queryName query-name)
    boost (.boost ^double boost)))

(defn wildcard
  "Matches documents that have fields matching a wildcard expression. Supported
  wildcards are *, which matches any character sequence (including the empty one),
  and ?, which matches any single character.

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-wildcard-query.html"
  [field wildcard-expression & {:keys [query-name boost]}]
  (cond-> (WildcardQueryBuilder. (->es-value field) wildcard-expression)
    query-name (.queryName query-name)
    boost (.boost ^double boost)))

(defn regexp
  "The regexp query allows you to use regular expression term queries

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-regexp-query.html"
  [field regex  & {:keys [query-name boost flags]}]
  (cond-> (RegexpQueryBuilder. (->es-value field) regex)
    flags (.flags (->> flags
                       (map #(case %
                               :all RegexpFlag/ALL
                               :anystring RegexpFlag/ANYSTRING
                               :complement RegexpFlag/COMPLEMENT
                               :empty RegexpFlag/EMPTY
                               :intersection RegexpFlag/INTERSECTION
                               :interval RegexpFlag/INTERVAL
                               :none RegexpFlag/NONE))
                       into-array))
    query-name (.queryName query-name)
    boost (.boost ^double boost)))

(defn fuzzy
  "The fuzzy query generates all possible matching terms that are within the
  maximum edit distance specified in fuzziness and then checks the term
  dictionary to find out which of those generated terms actually exist in
  the index

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-fuzzy-query.html"
  [field value & {:keys [boost query-name
                         fuzziness
                         prefix-length max-expansions]
                  :or {prefix-length 0
                       max-expansions 50}}]
  (cond-> (FuzzyQueryBuilder. ^String (->es-value field) value)
    fuzziness (.fuzziness (->> fuzziness
                               (map #(case %
                                       :auto Fuzziness/AUTO
                                       :field Fuzziness/FIELD
                                       :one Fuzziness/ONE
                                       :two Fuzziness/TWO
                                       :x-field-name Fuzziness/X_FIELD_NAME
                                       :zero Fuzziness/ZERO))
                               into-array))
    query-name (.queryName query-name)
    boost (.boost ^double boost)))

(defn type
  "Finds documents matching the provided mapping type

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-type-query.html"
  [mapping-type]
  (TypeQueryBuilder. (->es-value mapping-type)))

(defn ids
  "Finds documents matching the provided sequence of IDs

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-ids-query.html"
  ([id-list]
   (ids nil id-list))
  ([types id-list]
   (doto (IdsQueryBuilder. (cond->> types types (into-array String (map ->es-value types))))
     (.addIds ^java.util.Collection id-list))))

(defn nested
  "Finds root documents based on executing queries against the nested objects.

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-nested-query.html"
  [path query & {:keys [score-mode query-name boost]
                 :or {score-mode :avg}}]
  {:pre [(#{:none :avg :min :max :sum} score-mode)]}
  (cond-> (NestedQueryBuilder. (->es-value path) query)
    score-mode (.scoreMode ^String (name score-mode))
    query-name (.queryName ^String query-name)
    boost (.boost ^double boost)))

(defn has-child
  "Finds parents documents whose children match the provided 'query'

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-has-child-query.html"
  [type query & {:keys [min-children max-children score-mode query-name boost]
                 :or {score-mode :none}}]
  {:pre [(#{:none :avg :min :max :sum} score-mode)]}
  (cond-> (HasChildQueryBuilder. (->es-value type) query)
    min-children (.minChildren min-children)
    max-children (.maxChildren max-children)
    score-mode (.scoreType (name score-mode))
    query-name (.queryName query-name)
    boost (.boost ^double boost)))

(defn has-parent
  "Finds child documents whose parents march the provided 'query'

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-has-parent-query.html"
  [type query & {:keys [score-mode query-name boost]
                 :or {score-mode :none}}]
  {:pre [(#{:none :score} score-mode)]}
  (cond-> (HasParentQueryBuilder. (->es-value type) query)
    score-mode (.scoreType (name score-mode))
    query-name (.queryName query-name)
    boost (.boost ^double boost)))
