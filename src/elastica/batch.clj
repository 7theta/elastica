;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://www.eclipse.org/legal/epl-v10.html)
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns elastica.batch
  "Contains a BatchProcessor that can be used to batch put!, update!, upsert!
  and delete! requests to an Elasticsearch cluster. Batching provides a
  significant throughput advantage over individual requests at the expense of
  visibility, i.e., there is a time lag between the operations and when their
  results will appear in the search results.

  The BatchProcessor batching policies are highly configurable and a number of
  processors can be created in an application with a variety of SLAs."
  (:require [elastica.impl.coercion :refer [->es-value ->clj-value]]
            [elastica.impl.request :refer [index-request update-request
                                           upsert-request delete-request]]
            [utilis.fn :refer [fsafe apply-kw]]
            [com.stuartsierra.component :as component])
  (:import  [org.elasticsearch.action.index IndexRequest]
            [org.elasticsearch.action.update UpdateRequest]
            [org.elasticsearch.action.delete DeleteRequest]
            [org.elasticsearch.common.unit ByteSizeValue ByteSizeUnit TimeValue]
            [org.elasticsearch.action.bulk BulkProcessor BulkRequest BulkResponse BulkProcessor$Listener]
            [java.util Map]))

(declare start-batch-processor stop-batch-processor)

;;; Types

(defrecord BatchProcessor [cluster name
                           count size interval
                           concurrency
                           batch-start-fn batch-end-fn batch-failure-fn]
  component/Lifecycle
  (start [component] (start-batch-processor component))
  (stop [component] (stop-batch-processor component))

  java.io.Closeable
  (close [component] (.stop component)))

;;; Public

(defn batch-processor
  "Creates an instance of a BatchProcessor with 'name' that can be used
  to batch put!, update!, upsert! and delete! requests sent to an
  Elasticsearch cluster.

  The processor supports the following optional keyword parameters:
    :count - The number of requests that will be queued before they are
      dispatched to the cluster.
    :size - The size of the requests that will be queued before they
      are dispatched to the cluster. The size is specified as [value units],
      where units can be :KB, :MB, :GB or :TB.
    :interval - The number of seconds the requests will be accumulated
      before they are dispatched to the cluster.

    :concurrency - The number of batches of requests that can be in
      flight with the Elasticsearch cluster while the processor is
      accepting additional requests.

  The features can be disabled by passing a 'nil' for their key.

  The batch of requests will be dispatched to the cluster when any of
  the above conditions is met, within the constraints of :concurrency.

  Additionally the following callback functions can be provided to tie
  into the batch lifecycle:
    :batch-start-fn - The function that will be called when the batch starts.
      The function must accept a single parameter and will be passed the
      request object.
    :batch-end-fn - The function that will be called when the batch finishes.
      The function must accept two parameters and will be passed the
      request and response objects.
    :batch-failure-fn - The function that will be called if there is an error
      The function must accept two parameters and will be passed the
      request and failure objects."
  [name & {:keys [count size interval concurrency
                  batch-start-fn batch-end-fn batch-failure-fn]
           :or {count 1000
                size [5 :MB]
                interval 5
                concurrency 1}}]
  (map->BatchProcessor {:name name
                        :count count
                        :size size
                        :interval interval
                        :concurrency concurrency
                        :batch-start-fn batch-start-fn
                        :batch-end-fn batch-end-fn
                        :batch-failure-fn batch-failure-fn}))

(defn put!
  "Puts 'doc' of 'type' into 'index' via the batch processor.

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
      index settings."
  [processor index type doc  & {:keys [id parent version ttl] :as args}]
  {:pre [(:started processor)]}
  (.add ^BulkProcessor (:es-bulk-processor processor)
        ^IndexRequest (apply-kw index-request index type doc args)))

(defn update!
  "Merges the contents of 'update-doc' into the document identified by 'type'
  and 'id' in 'index' via the batch processor. The fields from 'update-doc'
  will entirely replace the fields in the existing document.

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
      update to succeed. This is useful as a form of optimistic locking."
  [processor index type id update-doc-or-script
   & {:keys [detect-no-op retry-on-conflict parent version] :as args}]
  {:pre [(:started processor)]}
  (.add ^BulkProcessor (:es-bulk-processor processor)
        ^UpdateRequest (apply-kw update-request index type id
                                 update-doc-or-script args)))

(defn upsert!
  "Merges the contents of 'update-doc' into the document identified by 'type'
  and 'id' in 'index' via the batch processor. The fields from 'update-doc'
  will entirely replace the fields in the existing document.

  If a document does not exist, 'insert-doc' will be inserted into the index.

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
      upsert to succeed. This is useful as a form of optimistic locking."
  [processor index type id insert-doc update-doc-or-script
   & {:keys [detect-no-op retry-on-conflict parent version] :as args}]
  {:pre [(:started processor)]}
  (.add ^BulkProcessor (:es-bulk-processor processor)
        ^UpdateRequest (apply-kw upsert-request index type id insert-doc
                                 update-doc-or-script args)))

(defn delete!
  "Deletes a document of 'type' with 'id' from 'index' via the batch
  processor.

  The following optional keyword parameters can be used used to control
  the behavior:
    :version - The version provided must match the version of the document in
       the index for the delete to succeed. This is useful to ensure that the
       document is not being deleted after another operation has updated it."
  [processor index type id & {:keys [version] :as args}]
  {:pre [(:started processor)]}
  (.add ^BulkProcessor (:es-bulk-processor processor)
        ^DeleteRequest (apply-kw delete-request index type id args)))

(defn flush!
  "Flushes any requests queued against the batch processor. Tuning batch
  processor policies is preferred to the manual invocation of flush!."
  [processor]
  (.flush ^BulkProcessor (:es-bulk-processor processor)))

;;; Implementation

(defn- start-batch-processor
  [component]
  (if-not (:es-bulk-processor component)
    (let [{:keys [cluster name count size interval concurrency]} component
          [size units] size
          units (case units
                  :KB (ByteSizeUnit/KB)
                  :MB (ByteSizeUnit/MB)
                  :GB (ByteSizeUnit/GB)
                  :TB (ByteSizeUnit/TB))
          listener (reify BulkProcessor$Listener
                     (^void beforeBulk [^BulkProcessor$Listener this ^long execution-id
                                        ^BulkRequest request]
                      ((fsafe (:batch-start-fn component)) request))
                     (^void afterBulk [^BulkProcessor$Listener this ^long execution-id
                                       ^BulkRequest request ^BulkResponse response]

                      ((fsafe (:batch-end-fn component)) request response))
                     (^void afterBulk [^BulkProcessor$Listener this ^long execution-id
                                       ^BulkRequest request ^Throwable failure]
                      ((fsafe (:batch-failure-fn component)) request failure)))
          processor (-> (BulkProcessor/builder (:es-client cluster) listener)
                        (.setBulkActions (or count -1))
                        (.setBulkSize (if size (ByteSizeValue. size units) -1))
                        (cond-> interval (.setFlushInterval (TimeValue/timeValueSeconds interval)))
                        (.setConcurrentRequests (or concurrency 0))
                        (cond-> name (.setName name))
                        .build)]
      (assoc component :es-bulk-processor processor :started true))
    component))

(defn- stop-batch-processor
  [component]
  (if-let [^BulkProcessor processor (:es-bulk-processor component)]
    (do (.flush processor)
        (.close processor)
        (dissoc component :es-bulk-processor :started))
    component))
