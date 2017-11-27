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
            [elastica.impl.coercion :refer [->es-value]]
            [utilis.fn :refer [apply-kw]]
            [org.httpkit.client :as http]
            [cheshire.core :as json]))

;;; Public

(defn index-exists?
  "Return a boolean indicating whether 'index' exists on 'cluster'"
  [cluster index]
  (let [result (ec/promise)]
    (http/head (ec/url cluster index)
               (fn [{:keys [status headers body error]}]
                 (deliver result
                          (case (long status)
                            200 true
                            404 false
                            (ex-info "index-exists?: error"
                                     {:es-error (json/decode body true)})))))
    result))

(defn create-index!
  "Creates 'index' on 'cluster'.

  The following optional keyword parameters can be used to control
  the behavior:
    :mappings - The mappings to be assigned to the index.
    :settings - The shard and replica settings for the index being created.
      The default is 5 shards with 1 replica."
  [cluster index & {:keys [mappings settings]
                    :or {settings {:shards 5 :replicas 1}}
                    :as args}]
  (let [request (merge
                 {:settings {:number_of_shards (:shards settings)
                             :number_of_replicas (:replicas settings)}}
                 (when mappings
                   {:mappings mappings}))
        result (ec/promise)]
    (http/put (ec/url cluster index)
              {:headers {"Content-Type" "application/json"}
               :body (json/encode request)}
              (fn [{:keys [status headers body error]}]
                (deliver result
                         (case (long status)
                           200 true
                           (ex-info "create-index: error"
                                    {:es-error (json/decode body true)})))))
    result))

(defn delete-index!
  "Deletes the index references by 'index' on the cluster connected to by
  'cluster'. A boolean is returned indicating whether the delete was successful"
  [cluster index]
  (let [result (ec/promise)]
    (http/delete (ec/url cluster index)
                 (fn [{:keys [status headers body error]}]
                   (deliver result
                            (case (long status)
                              200 true
                              (ex-info "delete-index: error"
                                       {:es-error (json/decode body true)})))))
    result))

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
        (when-not @(index-exists? cluster index)
          (deliver result @(apply-kw create-index! cluster index args)))
        (catch Exception e (deliver result e))))
    result))

(defn index
  [cluster index]
  (let [result (ec/promise)]
    (http/get (ec/url cluster index)
              (fn [{:keys [status headers body error]}]
                (deliver result
                         (case (long status)
                           200 (json/decode body true)
                           (ex-info "index: error"
                                    {:es-error (json/decode body true)})))))
    result))

(defn index-mappings
  "Returns a collection of mappings assigned to 'indices' in the
  'cluster'"
  ([cluster indices]
   (index-mappings cluster indices nil))
  ([cluster indices mapping-type]
   (let [result (ec/promise)]
     (http/get (str (ec/url cluster indices) "/_mapping/" (when mapping-type (name mapping-type)))
               (fn [{:keys [status headers body error]}]
                 (deliver result
                          (case (long status)
                            200 (json/decode body true)
                            (ex-info "index: error"
                                     {:es-error (json/decode body true)})))))
     result)))

(defn put-mapping!
  "Assigns the 'mapping-type' and 'mapping' to the 'indices' in the 'cluster'"
  [cluster indices mapping-type mapping]
  (let [result (ec/promise)]
    (http/put (str (ec/url cluster indices) "/_mapping/" (name mapping-type))
              {:headers {"Content-Type" "application/json"}
               :body (json/encode {:properties mapping})}
              (fn [{:keys [status headers body error]}]
                (deliver result
                         (case (long status)
                           200 true
                           (ex-info "put-mapping!: error"
                                    {:es-error (json/decode body true)})))))
    result))

(defn type-exists?
  "Return a boolean indicating whether 'indices' contain 'type' on on 'cluster'"
  [cluster indices type]
  (let [result (ec/promise)]
    (http/head (str (ec/url cluster indices) "/_mapping/" (name type))
               (fn [{:keys [status headers body error]}]
                 (deliver result
                          (case (long status)
                            200 true
                            404 false
                            (ex-info "type-exists?: error"
                                     {:es-error (json/decode body true)})))))
    result))
