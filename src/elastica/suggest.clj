;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://www.eclipse.org/legal/epl-v10.html)
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns elastica.suggest
  "Functions for the various suggestion types that can be used with
  elastica.core/suggest"
  (:require [elastica.impl.coercion :refer [->es-value]])
  (:import  [org.elasticsearch.search.suggest.completion
             CompletionSuggestionBuilder CompletionSuggestionFuzzyBuilder]
            [org.elasticsearch.search.suggest.term TermSuggestionBuilder]))


(defn completion
  "Returns a prefix suggestor that can be used to implement rudimentary
  auto-complete functionality.

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/search-suggesters-completion.html"
  [field text & {:keys [name fuzzy fuzzy-min-length fuzzy-prefix-length
                        fuzzy-transpositions unicode-aware]
                 :or {name (str (java.util.UUID/randomUUID))
                      fuzzy false
                      fuzzy-min-length 3
                      fuzzy-prefix-length 1
                      fuzzy-transpositions true
                      unicode-aware true}}]
  (if fuzzy
    (doto (CompletionSuggestionFuzzyBuilder. name)
      (.field (->es-value field))
      (.text (->es-value text))
      (.setFuzzyMinLength fuzzy-min-length)
      (.setFuzzyPrefixLength fuzzy-prefix-length)
      (.setFuzzyTranspositions fuzzy-transpositions)
      (.setUnicodeAware unicode-aware))
    (doto (CompletionSuggestionBuilder. name)
      (.field (->es-value field))
      (.text (->es-value text)))))
