(ns elastica.query.score
  "Functions for influencing the documents returned from elastica.core/search
  by adjusting their relevance score."
  (:require [elastica.impl.coercion :refer [->es-value]])
  (:import  [org.elasticsearch.index.query QueryBuilders
             BoostingQueryBuilder ConstantScoreQueryBuilder]
            [org.elasticsearch.index.query.functionscore.fieldvaluefactor
             FieldValueFactorFunctionBuilder]
            [org.elasticsearch.index.query.functionscore.random
             RandomScoreFunctionBuilder]
            [org.elasticsearch.index.query.functionscore.weight
             WeightBuilder]
            [org.elasticsearch.common.lucene.search.function
             FieldValueFactorFunction$Modifier]))

(defn boosting
  "Can be used to effectively demote results that match 'query'. Unlike the
   :must-not clause in bool query, this still selects documents that contain
  undesirable terms, but reduces their overall score

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-boosting-query.html"
  [& {:keys [positive-query negative-query
             positive-boost negative-boost]}]
  {:pre [(or positive-query negative-query)]}
  (cond-> (BoostingQueryBuilder.)
    positive-query (.positive positive-query)
    negative-query (.negative negative-query)
    positive-boost (.boost ^float positive-boost)
    negative-boost (.negativeBoost ^float negative-boost)))

(defn constant-score
  "Wraps another query and returns a constant score equal to the boost for
  every document.

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-constant-score-query.html"
  [query boost]
  (.boost (ConstantScoreQueryBuilder. query) ^float boost))

(defn function-score
  "Modifies the score of documents that are retrieved by 'query'. This can be
  useful if, for example, a score function is computationally expensive and
  it is sufficient to compute the score on a filtered set of documents.

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-function-score-query.html"
  [query ])

(defn field-value
  "Uses the value of a field to influence the overall score of the document.

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-function-score-query.html#function-field-value-factor"
  [field {:keys [factor missing modifier]}]
  {:pre [(#{:ln :ln1p :ln2p :log :log1p :log2p :none :reciprocal :sqrt :square} modifier)]}
  (let [field-modifiers {:ln FieldValueFactorFunction$Modifier/LN
                         :ln1p FieldValueFactorFunction$Modifier/LN1P
                         :ln2p FieldValueFactorFunction$Modifier/LN2P
                         :log FieldValueFactorFunction$Modifier/LOG
                         :log1p FieldValueFactorFunction$Modifier/LOG1P
                         :log2p FieldValueFactorFunction$Modifier/LOG2P
                         :none FieldValueFactorFunction$Modifier/NONE
                         :reciprocal FieldValueFactorFunction$Modifier/RECIPROCAL
                         :sqrt FieldValueFactorFunction$Modifier/SQRT
                         :square FieldValueFactorFunction$Modifier/SQUARE}]
    (cond-> (FieldValueFactorFunctionBuilder. (->es-value field))
      factor (.factor factor)
      missing (.missing missing)
      modifier (.modifier (get field-modifiers modifier)))))

(defn random-score
  "Generates a score using a hash of the _uid field with 'seed' for variation.
  If 'seed' is not provided, the current time is used."
  [& {:keys [seed]}]
  (cond-> (RandomScoreFunctionBuilder.) seed (.seed ^long seed)))

(defn weight
  "Multiplies the score with 'weight'"
  [weight]
  (.setWeight (WeightBuilder.) weight))
