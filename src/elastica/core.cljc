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
  "Functions required to work with documents in Elasticsearch indexes.

  If bulk interactions are required, the functions in the elastica.batch
  namespace should be used instead."
  (:refer-clojure :exclude [get sort])
  (:require [elastica.impl.client :as ec]
            [elastica.impl.coercion :refer [->es-key]]
            [elastica.impl.http :as http]
            [elastica.cluster :as cluster]
            [utilis.fn :refer [fsafe]]
            [utilis.map :refer [compact map-keys]]
            [utilis.logic :refer [xor]]
            [clojure.string :as st]))

;;; Public

(defn index
  "Return the index that 'doc' was retrieved from"
  [doc]
  (:_index doc))

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
  "Returns the contents of the document of 'id' from 'index'.
  The keys in the document will be converted to keywords. If the document is
  not found, a nil will be returned"
  [cluster index id]
  (cluster/run cluster
    {:uri (http/uri
           {:indexes index
            :segments ["_doc" id]})
     :method :get
     :response-xform http/missing-response-xform}))

(defn put!
  "Puts 'doc' into 'index'.

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
      can be controlled via the 'indiced.ttl.interval' and 'indexes.ttl.bulk_size'
      index settings"
  [cluster index doc
   & {:keys [id parent version ttl refresh]
      :or {refresh false}
      :as args}]
  {:pre [(contains? #{true false :wait-for} refresh)]}
  (cluster/run cluster
    {:method (if id :put :post)
     :uri (http/uri
           {:segments (concat ["_doc"] (when id [id]))
            :indexes index})
     :query (merge
             {:refresh refresh}
             (when version {:version version}))
     :body doc}))

(defn update!
  "Merges the contents of 'update-doc' into the document identified by 'id' in
  'index'. The fields from 'update-doc' will entirely replace the fields in the
  existing document. Alternatively a 'script' can be specified that will be run
  against the existing document to create the new version.

  If a document does not exist, an exception will be thrown.

  The following optional keyword parameters can be used to control
  the behavior:
    :update-doc - The document to merge into the existing document
    :script - The script to run against the existing document
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
      update to succeed. This is useful as a form of optimistic locking"
  [cluster index id & {:keys [update-doc script
                              detect-no-op retry-on-conflict parent
                              version]}]
  {:pre [(xor update-doc script)]}
  (cluster/run cluster
    {:method :post
     :uri (http/uri
           {:indexes index
            :segments ["_update" id]})
     :query (when version {:version version})
     :body (compact
            {(if script :script :doc) (or script update-doc)
             :detect_noop detect-no-op})}))

(defn upsert!
  "Merges the contents of 'update-doc' into the document identified by
  'id' in 'index'. The fields from 'update-doc' will entirely replace the
  fields in the existing document. Alternatively a 'script' can be specified
  that will be run against the existing document to create the new version.

  If a document does not exist, 'insert-doc' will be inserted into the index.

  The created? function can be used to check whether the put led to a fresh
  document being added or the replacement of an existing one.

  The following optional keyword parameters can be used used to control
  the behavior:
    :update-doc - The document to merge into the existing document
    :script - The script to run against the existing document
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
      upsert to succeed. This is useful as a form of optimistic locking"
  [cluster index id
   & {:keys [insert-doc doc-as-upsert
             update-doc script
             detect-no-op retry-on-conflict
             parent version callback]}]
  {:pre [(or insert-doc doc-as-upsert) (xor update-doc script)]}
  (cluster/run cluster
    {:method :post
     :uri (http/uri
           {:indexes index
            :segments ["_update" id]})
     :query (when version {:version version})
     :body (compact
            {(if script :script :doc) (or script update-doc)
             :detect_noop detect-no-op
             :upsert insert-doc
             :doc_as_upsert doc-as-upsert})}))

(defn delete!
  "Deletes a document with 'id' from 'index'.

  If a document with 'id' does not exist, the :found? key in the result
  will be false.

  The following optional keyword parameters can be used used to control
  the behavior:
    :version - The version provided must match the version of the document in
       the index for the delete to succeed. This is useful to ensure that the
       document is not being deleted after another operation has updated it"
  [cluster index id & {:keys [version]}]
  (cluster/run cluster
    {:method :delete
     :uri (http/uri
           {:indexes index
            :segments [id]})
     :query (when version {:version version})}))

(defn search
  "Executes 'query' across the collection of 'indexes' in the Elasticsearch
  'cluster' and returns the results.

  The following optional keyword parameters can be used to control
  the behavior:
    :types - A collection of types to restrict the set of returned documents
    :sorts - A collection of sort criteria to apply to the result set
    :start and :size - A start index and a page size for the results. It is
      recommended that the scan and scroll API be used if deep paging is
      required
    :timeout - Bound the search with a timeout. Return accumulated results up to
      the timeout point if the timeout occurs.
      https://www.elastic.co/guide/en/elasticsearch/reference/6.3/common-options.html#time-units
    :search-type - Type of search operation. Can be either :dfs-query-then-fetch
      or :query-then-fetch
    :request-cache - Set to true or false to enable or disable the caching of
      search results for requests where size is 0.
    :allow-partial-search-results - Set to false to return an overall failure if
      the request would produce partial results. Defaults to true, which will
      allow partial results in the case of timeouts or partial failures.
    :terminate-after - The maximum number of documents to collect for each
      shard, upon reaching which the query execution will terminate early.
    :batched-reduce-size - The number of shard results that should be reduced at
      once on the coordinating node.

  https://www.elastic.co/guide/en/elasticsearch/reference/6.3/search-request-body.html"
  [cluster indexes query
   & {:keys [types sorts
             suggest
             start size
             explain
             source-filter
             aggregations
             timeout
             search-type
             request-cache
             allow-partial-search-results
             terminate-after
             batched-reduce-size
             highlight]
      :or {start 0
           size 10}}]
  (cluster/run cluster
    {:method :get
     :uri (http/uri
           {:indexes indexes
            :segments ["_search"]})
     :body (merge
            (when query {:query query})
            suggest
            (->> {:_source (if (map? source-filter)
                             (compact
                              (update source-filter :excludes
                                      (partial map ->es-key)))
                             source-filter)
                  :aggregations aggregations
                  :timeout timeout
                  :from start
                  :size size
                  :search_type ((fsafe ->es-key) search-type)
                  :request_cache request-cache
                  :allow_partial_search_results allow-partial-search-results
                  :terminate_after terminate-after
                  :batched_reduce_size batched-reduce-size
                  :highlight (update highlight :fields (partial map-keys ->es-key))}
                 (filter (comp some? second))
                 (into {})))}))

(defn scroll
  "https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html"
  [cluster indexes query
   & {:keys [size scroll-id scan scroll source]
      :or {size 1000
           scroll "1m"}}]
  (cluster/run cluster
    {:method :post
     :uri (http/uri
           (merge
            (when-not scroll-id
              {:indexes indexes})
            {:segments
             (if scroll-id
               ["_search" "scroll"]
               ["_search"])}))
     :query {:scroll scroll}
     :body  (merge
             (when query {:query query})
             (when (and size (not scroll-id)) {:size size})
             (when scroll-id {:scroll-id scroll-id})
             (when-not scroll-id {:_source source}))}))

(defn clear-scroll
  "https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html"
  [cluster indexes & {:keys [scroll-id] :or {scroll-id :all}}]
  (cluster/run cluster
    (if (= scroll-id :all)
      {:method :delete
       :uri (http/uri {:segments ["_search" "scroll" "_all"]})}
      {:method :delete
       :uri (http/uri {:segments ["_search" "scroll"]})
       :body {:scroll-id scroll-id}})))

(defn exists?
  [cluster index doc-id]
  (cluster/run cluster
    {:method :head
     :response-xform (comp boolean http/missing-response-xform)
     :uri
     (http/uri
      {:indexes [index]
       :segments ["_doc" doc-id]})}))
