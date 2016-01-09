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
  (:require [elastica.impl.coercion :refer [->es-value ->clj-value]]
            [elastica.impl.request :refer [run extract-header]]
            [utilis.fn :refer [apply-kw]]
            [clojure.set :refer [rename-keys]])
  (:import  [org.elasticsearch.client Client]
            [org.elasticsearch.action.admin.indices.exists.indices IndicesExistsRequest IndicesExistsResponse]
            [org.elasticsearch.action.admin.indices.create CreateIndexRequest CreateIndexResponse]
            [org.elasticsearch.action.admin.indices.delete DeleteIndexRequest DeleteIndexResponse]
            [org.elasticsearch.action.admin.indices.mapping.get GetMappingsRequest GetMappingsResponse]
            [org.elasticsearch.action.admin.indices.mapping.put PutMappingRequest PutMappingResponse]
            [java.util Map]))

;;; Public

(defn index-exists?
  "Return a boolean indicating whether 'index' exists on 'cluster'.

  The following optional keyword parameters can be used to control
  the behavior:
   :callback - The function that will be called when the response is
      received from Elasticsearch. The function must take one parameter
      and will be passed the result, which it must de-reference to receive
      the actual result. If the operation resulted in an error, de-referencing
      the result will cause the exception to be thrown within the callback.
      If a callback is supplied, the function will execute asynchronously."
  [cluster ^String index & {:keys [callback]}]
  {:pre [(:started cluster)]}
  (let [^Client client (:es-client cluster)
        ^IndicesExistsRequest request (IndicesExistsRequest. (into-array [index]))]
    (run (-> client .admin .indices) .exists request
      (fn [^IndicesExistsResponse response] (.isExists response))
      callback)))

(defn create-index!
  "Creates 'index' on 'cluster'.

  The following optional keyword parameters can be used to control
  the behavior:
    :mappings - The mappings to be assigned to the index.
    :settings - The shard and replica settings for the index being created.
      The default is 8 shards with 1 replica.
    :callback - The function that will be called when the response is
      received from Elasticsearch. The function must take one parameter
      and will be passed the result, which it must de-reference to receive
      the actual result. If the operation resulted in an error, de-referencing
      the result will cause the exception to be thrown within the callback.
      If a callback is supplied, the function will execute asynchronously."
  [cluster ^String index & {:keys [mappings settings callback]
                            :or {settings {:shards 8 :replicas 1}}}]
  {:pre [(:started cluster)]}
  (let [^Client client (:es-client cluster)
        ^CreateIndexRequest request (CreateIndexRequest. index)
        _ (.settings request ^Map (->es-value (rename-keys settings
                                                           {:shards "number_of_shards"
                                                            :replicas "number_of_replicas"})))
        _ (when mappings
            (doseq [[k v] mappings]
              (.mapping request ^String (name k) ^Map (->es-value v))))]
    (run (-> client .admin .indices) .create request
      (fn [^CreateIndexResponse response] (.isAcknowledged response))
      callback)))

(defn delete-index!
  "Deletes the index references by 'index' on the cluster connected to by
  'cluster'. A boolean is returned indicating whether the delete was successful.

  The following optional keyword parameters can be used to control
  the behavior:
   :callback - The function that will be called when the response is
      received from Elasticsearch. The function must take one parameter
      and will be passed the result, which it must de-reference to receive
      the actual result. If the operation resulted in an error, de-referencing
      the result will cause the exception to be thrown within the callback.
      If a callback is supplied, the function will execute asynchronously."
  [cluster ^String index & {:keys [callback]}]
  {:pre [(:started cluster)]}
  (let [^Client client (:es-client cluster)
        ^DeleteIndexRequest request (DeleteIndexRequest. index)]
    (run (-> client .admin .indices) .delete request
      (fn [^DeleteIndexResponse response] (.isAcknowledged response))
      callback)))

(defn ensure-index!
  "Creates 'index' on 'cluster' if it does not already exist.

  The following optional keyword parameters can be used to control
  the behavior:
    :mappings - The mappings to be assigned to the index.
    :settings - The shard and replica settings for the index being created.
      The default is 8 shards with 1 replica.
    :callback - The function that will be called when the response is
      received from Elasticsearch. The function must take one parameter
      and will be passed the result, which it must de-reference to receive
      the actual result. If the operation resulted in an error, de-referencing
      the result will cause the exception to be thrown within the callback.
      If a callback is supplied, the function will execute asynchronously."
  [cluster ^String index & {:keys [mappings settings callback]
                            :or {settings {:shards 8 :replicas 1}}
                            :as args}]
  {:pre [(:started cluster)]}
  (if callback
    (index-exists? cluster index
                   :callback (fn [result]
                               (when-not @result
                                 (apply-kw create-index! cluster index args))))
    (when-not (index-exists? cluster index)
      (apply-kw create-index! cluster index args))))

(defn index-mappings
  "Returns a collection of mappings assigned to 'indices' in the
  'cluster'.

  The following optional keyword parameters can be used to control
  the behavior:
   :callback - The function that will be called when the response is
      received from Elasticsearch. The function must take one parameter
      and will be passed the result, which it must de-reference to receive
      the actual result. If the operation resulted in an error, de-referencing
      the result will cause the exception to be thrown within the callback.
      If a callback is supplied, the function will execute asynchronously."
  [cluster indices & {:keys [callback]}]
  {:pre [(:started cluster)]}
  (let [^Client client (:es-client cluster)
        ^GetMappingsRequest request (doto (GetMappingsRequest.)
                                      (.indices #^"[Ljava.lang.String;"
                                                (into-array String (map name indices))))]
    (run (-> client .admin .indices) .getMappings request
      (fn [^GetMappingsResponse response]
        (-> response .mappings (->clj-value :keys->keyword true)))
      callback)))

(defn put-mapping!
  "Assigns the 'mapping-type' and 'mapping' to the 'indices' in the 'cluster'.

  The following optional keyword parameters can be used to control
  the behavior:
   :callback - The function that will be called when the response is
      received from Elasticsearch. The function must take one parameter
      and will be passed the result, which it must de-reference to receive
      the actual result. If the operation resulted in an error, de-referencing
      the result will cause the exception to be thrown within the callback.
      If a callback is supplied, the function will execute asynchronously."
  [cluster mapping-type mapping indices & {:keys [callback]}]
  {:pre [(:started cluster)]}
  (let [^Client client (:es-client cluster)
        ^PutMappingRequest request (doto (PutMappingRequest. (into-array String (map name indices)))
                                     (.type mapping-type)
                                     (.source ^Map (->es-value mapping)))]

    (run (-> client .admin .indices) .putMapping request
      (fn [^PutMappingResponse response] (.isAcknowledged response))
      callback)))
