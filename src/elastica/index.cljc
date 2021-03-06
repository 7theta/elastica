;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://www.eclipse.org/legal/epl-v10.html)
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns elastica.index
  "Functions for manipulating the indices in an Elasticsearch cluster."
  (:require [elastica.impl.client :as ec]
            [elastica.impl.http :as http :refer [exists-response-xform]]
            [elastica.impl.coercion :refer [es->]]
            [utilis.types.string :refer [->string]]
            [utilis.fn :refer [apply-kw fsafe]]
            [utilis.map :refer [map-keys map-vals]]
            [clojure.set :refer [rename-keys]]
            [jsonista.core :as json]
            [elastica.cluster :as cluster]))

;;; Declarations

(def json-mapper (json/object-mapper {:decode-key-fn true}))

;;; Public

(defn index-exists?
  "Return a boolean indicating whether 'index' exists on 'cluster'"
  [cluster index]
  (cluster/run cluster
    {:method :head
     :uri (http/uri {:indexes index})
     :response-xform exists-response-xform}))

(defn create-index!
  "Creates 'index' on 'cluster'.

  The following optional keyword parameters can be used to control
  the behavior:
    :mappings - The mappings to be assigned to the index.
    :settings - The shard and replica settings for the index being created.
      The default is 5 shards with 1 replica."
  [cluster index & {:keys [mappings settings]
                    :or {settings {:shards 5 :replicas 1}}}]
  (cluster/run cluster
    {:method :put
     :uri (http/uri {:indexes index})
     :headers {"Content-Type" "application/json"}
     :body (merge
            {:settings
             (rename-keys
              settings
              {:shards :number_of_shards
               :replicas :number_of_replicas})}
            (when mappings {:mappings mappings}))}))

(defn delete-index!
  "Deletes the index references by 'index' on the cluster connected to by
  'cluster'. A boolean is returned indicating whether the delete was successful"
  [cluster index]
  (cluster/run cluster
    {:method :delete
     :uri (http/uri {:indexes index})}))

(defn ensure-index!
  "Creates 'index' on 'cluster' if it does not already exist.

  The following optional keyword parameters can be used to control
  the behavior:
    :mappings - The mappings to be assigned to the index.
    :settings - The shard and replica settings for the index being created.
      The default is 8 shards with 1 replica."
  [cluster index & {:keys [mappings settings]
                    :or {settings {:shards 8 :replicas 1}}
                    :as args}]
  (let [result (ec/promise)]
    (future
      (try
        (deliver
         result
         (or @(index-exists? cluster index)
             @(apply-kw create-index! cluster index args)))
        (catch Exception e
          (deliver result e))))
    result))

(defn update-settings!
  "Updates the index-level settings on 'index'.

  https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html"
  [cluster index settings & {:keys [preserve-existing]}]
  (cluster/run cluster
    {:method :put
     :uri (http/uri
           {:indexes index
            :segments ["_settings"]})
     :query (when (some? preserve-existing)
              {:preserve_existing preserve-existing})
     :headers {"Content-Type" "application/json"}
     :body settings}))

(defn index
  [cluster index]
  (cluster/run cluster
    {:uri (http/uri {:indexes index})}))

(defn index-mappings
  "Returns a collection of mappings assigned to 'indices' in the
  'cluster'"
  ([cluster indices]
   (index-mappings cluster indices nil))
  ([cluster indices mapping-type]
   (cluster/run cluster
     {:uri
      (http/uri
       {:indexes indices
        :segments
        (cons "_mapping"
              (when mapping-type
                [mapping-type]))})})))

(defn put-mapping!
  "Assigns the 'mapping-type' and 'mapping' to the 'indices' in the 'cluster'"
  ([cluster indices mapping]
   (put-mapping! cluster indices mapping nil))
  ([cluster indices mapping {:keys [mapping-type]}]
   (cluster/run cluster
     (merge
      {:method :put
       :uri (http/uri
             {:indexes indices
              :segments (cons "_mapping"
                              (when mapping-type
                                [mapping-type]))})
       :headers {"Content-Type" "application/json"}
       :body {:properties mapping}}
      (when mapping-type
        {:query {:include-type-name true}})))))

(defn type-exists?
  "Return a boolean indicating whether 'indices' contain 'type' on on 'cluster'"
  [cluster indices type]
  (cluster/run cluster
    {:method :head
     :uri (http/uri
           {:indexes indices
            :type type
            :segments ["_mapping"]})
     :response-xform exists-response-xform}))

(defn stats
  "Indices level stats provide statistics on different operations happening on
  an index. This API provides statistics on the index level scope."
  [cluster]
  (cluster/run cluster
    {:uri (http/uri {:segments ["_stats"]})}))

(defn list-indices
  "Return a seq of all indices known to the cluster"
  [cluster]
  (let [result (ec/promise)]
    (future
      (let [stats @(stats cluster)]
        (if (instance? Throwable stats)
          (deliver result stats)
          (->> stats :indices keys (deliver result)))))
    result))
