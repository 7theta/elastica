;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://www.eclipse.org/legal/epl-v10.html)
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns elastica.core
  "Functions required to work with documents in Elasticsearch indices.

  If bulk interactions are required, the functions in the elastica.batch
  namespace should be used instead."
  (:refer-clojure :exclude [get type sort])
  (:require [elastica.impl.coercion :refer [->es-value ->clj-value]]
            [elastica.impl.request :refer [run extract-header
                                           index-request update-request
                                           upsert-request delete-request]]
            [utilis.fn :refer [apply-kw]])
  (:import  [org.elasticsearch.client Client]
            [org.elasticsearch.action.get GetRequest GetResponse]
            [org.elasticsearch.action.index IndexRequest IndexResponse]
            [org.elasticsearch.action.update UpdateRequest UpdateResponse]
            [org.elasticsearch.action.delete DeleteRequest DeleteResponse]
            [org.elasticsearch.index.query QueryBuilder]
            [org.elasticsearch.action.suggest SuggestRequestBuilder
             SuggestRequest SuggestResponse]
            [org.elasticsearch.search.suggest Suggest
             Suggest$Suggestion Suggest$Suggestion$Entry
             Suggest$Suggestion$Entry$Option]
            [org.elasticsearch.search.suggest.completion CompletionSuggestion$Entry$Option]
            [org.elasticsearch.action.search SearchRequestBuilder
             SearchRequest SearchResponse]
            [org.elasticsearch.search SearchHits SearchHit]))

;;; Public

(defn index
  "Return the index that 'doc' was retrieved from"
  [doc]
  (:_index doc))

(defn type
  "Return the type of 'doc'"
  [doc]
  (:_type doc))

(defn id
  "Return the id of 'doc'"
  [doc]
  (:_id doc))

(defn version
  "Return the version of 'doc'"
  [doc]
  (:_version doc))

(defn created?
  "Returns a boolean indicating whether 'doc' was freshly created by the last
  operation performed with it."
  [doc]
  (:_created? doc))

(defn found?
  "Returns a boolean indicating whether 'doc' was found in the index by the last
  operation performed with it."
  [doc]
  (:_found? doc))

(defn score
  "Return the score the 'doc' achieved in the query"
  [doc]
  (:_score doc))

(defn source
  "Return the source of 'doc'"
  [doc]
  (:_source doc))

(defn get
  "Returns the contents of the document of 'type' with 'id' from 'index'.
  The keys in the document will be converted to keywords. If the document is
  not found, a nil will be returned.

  The following optional keyword parameters can be used to control
  the behavior:
    :callback - The function that will be called when the response is
      received from Elasticsearch. The function must take one parameter
      and will be passed the result, which it must de-reference to receive
      the actual result. If the operation resulted in an error, de-referencing
      the result will cause the exception to be thrown within the callback.
      If a callback is supplied, the function will execute asynchronously."
  [cluster index type id & {:keys [callback]}]
  {:pre [(:started cluster)]}
  (let [^Client cluster (:es-client cluster)
        ^GetRequest request (GetRequest. index (->es-value type) id)
        response-parser (fn [^GetResponse response]
                          (when (and (.isExists response) (not (.isSourceEmpty response)))
                            (assoc (extract-header response)
                                   :_source (->clj-value (.getSourceAsMap response)
                                                         :keys->keyword true))))]
    (run cluster .get request response-parser callback)))

(defn put!
  "Puts 'doc' of 'type' into 'index'.

  The created? function can be used to check whether the put led to a fresh
  document being added or the replacement of an existing one.

  The following optional keyword parameters can be used used to control
  the behavior:
    :id - The id to be used for the document in the index. If a document
      with 'id' already exists in the index it will be replaced. if the
      'id' is not provided, it will be automatically generated.
    :parent - The id of the parent document if this is a child document
    :version - If the put! is replacing an existing document, the version
      provided must match the version of the document in the index for the
      put to succeed. This is useful as a form of optimistic locking.
    :ttl - A positive value in milliseconds can be provided to control how long
      a document will live in the index before it is automatically deleted.
      The type's mapping must enable the '_ttl' field and the sweep of the index
      can be controlled via the 'indiced.ttl.interval' and 'indices.ttl.bulk_size'
      index settings.
    :callback - The function that will be called when the response is
      received from Elasticsearch. The function must take one parameter
      and will be passed the result, which it must de-reference to receive
      the actual result. If the operation resulted in an error, de-referencing
      the result will cause the exception to be thrown within the callback.
      If a callback is supplied, the function will execute asynchronously."
  [cluster index type doc
   & {:keys [id parent version ttl callback] :as args}]
  {:pre [(:started cluster)]}
  (let [^Client cluster (:es-client cluster)
        ^IndexRequest request (apply-kw index-request index type doc args)
        response-parser (fn [^IndexResponse response]
                          (println response)
                          (assoc (extract-header response)
                                 :_created? (.isCreated response)))]
    (run cluster .index request response-parser callback)))

(defn update!
  "Merges the contents of 'update-doc' into the document identified by 'type'
  and 'id' in 'index'. The fields from 'update-doc' will entirely replace the
  fields in the existing document.

  If a document does not exist, an exception will be thrown.

  The following optional keyword parameters can be used to control
  the behavior:
    :detect-no-op - By default the document is only reindexed if the new
       version differs from the old. Setting detect-no-op to false will
       cause Elasticsearch to always update the document even if it hasn’t
       changed.
    :retry-on-conflict - In between the get and indexing phases of the update,
       it is possible that another process might have already updated the same
       document. By default, the update will fail with a version conflict
       exception. The retry-on-conflict parameter controls how many times to
       retry the update before finally throwing an exception.
    :parent - The id of the parent document if this is a child document
    :version - If the update! is replacing an existing document, the version
      provided must match the version of the document in the index for the
      update to succeed. This is useful as a form of optimistic locking.
    :callback - The function that will be called when the response is
      received from Elasticsearch. The function must take one parameter
      and will be passed the result, which it must de-reference to receive
      the actual result. If the operation resulted in an error, de-referencing
      the result will cause the exception to be thrown within the callback.
      If a callback is supplied, the function will execute asynchronously."
  [cluster index type id update-doc-or-script
   & {:keys [detect-no-op retry-on-conflict parent version callback] :as args}]
  {:pre [(:started cluster)]}
  (let [^Client cluster (:es-client cluster)
        ^UpdateRequest request (apply-kw update-request index type id
                                         update-doc-or-script args)
        response-parser (fn [^UpdateResponse response] (extract-header response))]
    (run cluster .update request response-parser callback)))

(defn upsert!
  "Merges the contents of 'update-doc' into the document identified by 'type'
  and 'id' in 'index'. The fields from 'update-doc' will entirely replace the
  fields in the existing document.

  If a document does not exist, 'insert-doc' will be inserted into the index.

  The created? function can be used to check whether the put led to a fresh
  document being added or the replacement of an existing one.

  The following optional keyword parameters can be used used to control
  the behavior:
    :detect-no-op - By default the document is only reindexed if the new
       version differs from the old. Setting detect-no-op to false will
       cause Elasticsearch to always update the document even if it hasn’t
       changed.
    :retry-on-conflict - In between the get and indexing phases of the update,
       it is possible that another process might have already updated the same
       document. By default, the update will fail with a version conflict
       exception. The retry-on-conflict parameter controls how many times to
       retry the update before finally throwing an exception.
    :parent - The id of the parent document if this is a child document
    :version - If the upsert! is replacing an existing document, the version
      provided must match the version of the document in the index for the
      upsert to succeed. This is useful as a form of optimistic locking.
    :callback - The function that will be called when the response is
      received from Elasticsearch. The function must take one parameter
      and will be passed the result, which it must de-reference to receive
      the actual result. If the operation resulted in an error, de-referencing
      the result will cause the exception to be thrown within the callback.
      If a callback is supplied, the function will execute asynchronously."
  [cluster index type id insert-doc update-doc-or-script
   & {:keys [detect-no-op retry-on-conflict parent version callback] :as args}]
  {:pre [(:started cluster)]}
  (let [^Client cluster (:es-client cluster)
        ^UpdateRequest request (apply-kw upsert-request index type id insert-doc
                                         update-doc-or-script args)
        response-parser (fn [^UpdateResponse response]
                          (assoc (extract-header response)
                                 :_created? (.isCreated response)))]
    (run cluster .update request response-parser callback)))

(defn delete!
  "Deletes a document of 'type' with 'id' from 'index'.

  If a document with 'id' does not exist, the :found? key in the result
  will be false.

  The following optional keyword parameters can be used used to control
  the behavior:
    :version - The version provided must match the version of the document in
       the index for the delete to succeed. This is useful to ensure that the
       document is not being deleted after another operation has updated it.
    :callback - The function that will be called when the response is
      received from Elasticsearch. The function must take one parameter
      and will be passed the result, which it must de-reference to receive
      the actual result. If the operation resulted in an error, de-referencing
      the result will cause the exception to be thrown within the callback.
      If a callback is supplied, the function will execute asynchronously."
  [cluster index type id & {:keys [version callback] :as args}]
  {:pre [(:started cluster)]}
  (let [^Client cluster (:es-client cluster)
        ^DeleteRequest request (apply-kw delete-request index type id args)
        response-parser (fn [^DeleteResponse response]
                          (assoc (extract-header response)
                                 :_found? (.isFound response)))]
    (run cluster .delete request response-parser callback)))

(defn search
  "Executes 'query' across the collection of 'indices' in the Elasticsearch
  'cluster' and returns the results.

  The following optional keyword parameters can be used to control
  the behavior:
    :types - A collection of types to restrict the set of returned documents
    :sorts - A collection of sort criteria to apply to the result set
    :start and :size - A start index and a page size for the results. It is
      recommended that the scan and scroll API be used if deep paging is
      required.
    :callback - The function that will be called when the response is
      received from Elasticsearch. The function must take one parameter
      and will be passed the result, which it must de-reference to receive
      the actual result. If the operation resulted in an error, de-referencing
      the result will cause the exception to be thrown within the callback.
      If a callback is supplied, the function will execute asynchronously."
  [cluster indices query  & {:keys [types sorts start size explain callback]}]
  (let [^Client cluster (:es-client cluster)
        ^SearchRequestBuilder builder (cond-> (.prepareSearch cluster (into-array indices))
                                        sorts (#(reduce (fn [^SearchRequestBuilder search sort]
                                                          (.addSort search sort))
                                                        % sorts)))
        ^SearchRequest request (cond-> (doto builder
                                         (.setVersion true)
                                         (.setQuery ^QueryBuilder query))
                                 start (.setFrom start)
                                 size (.setSize size)
                                 types (.setTypes (into-array (map ->es-value types)))
                                 explain (.setExplain explain)
                                 true .request)
        response-parser (fn [^SearchResponse response]
                          (let [^SearchHits hits (.getHits response)
                                docs (->> hits .iterator iterator-seq
                                          (mapv (fn [^SearchHit h]
                                                  (assoc (extract-header h)
                                                         :_score (.getScore h)
                                                         :_source (->clj-value (.getSource h)
                                                                               :keys->keyword true)))))]
                            {:took (.getTookInMillis response)
                             :_shards {:total (.getTotalShards response)
                                       :successful (.getSuccessfulShards response)
                                       :failed (.getFailedShards response)}
                             :timed_out (.isTimedOut response)
                             :terminated_early (.isTerminatedEarly response)
                             :hits {:total (.getTotalHits hits)
                                    :max_score (.getMaxScore hits)
                                    :hits docs}}))]
    (run cluster .search request response-parser callback)))

(defn suggest
  "Returns the matches for 'suggestions' across 'indices' in the 'cluster'.

  The following optional keyword parameters can be used to control
  the behavior:
    :callback - The function that will be called when the response is
      received from Elasticsearch. The function must take one parameter
      and will be passed the result, which it must de-reference to receive
      the actual result. If the operation resulted in an error, de-referencing
      the result will cause the exception to be thrown within the callback.
      If a callback is supplied, the function will execute asynchronously."
  [cluster indices suggestions & {:keys [callback]}]
  (let [^Client cluster (:es-client cluster)
        ^SuggestRequestBuilder builder (reduce (fn [^SuggestRequestBuilder builder s]
                                                 (.addSuggestion builder s))
                                               (.prepareSuggest cluster (into-array indices))
                                               suggestions)
        response-parser (fn [^SuggestResponse response]
                          (let [^Suggest suggestions (.getSuggest response)]
                            (->> suggestions
                                 (map (fn [^Suggest$Suggestion sr]
                                        (->> (.getEntries sr)
                                             (map (fn [^Suggest$Suggestion$Entry c]
                                                    [(.getName sr)
                                                     {:text (str (.getText c))
                                                      :offset (.getOffset c)
                                                      :length (.getLength c)
                                                      :options (map (fn [^CompletionSuggestion$Entry$Option o]
                                                                      {:text (str (.getText o))
                                                                       :score (.getScore o)
                                                                       :payload (->clj-value (.getPayloadAsMap o))})
                                                                    (.getOptions c))}]))
                                             (into {})))))))]
    (run builder response-parser callback)))
