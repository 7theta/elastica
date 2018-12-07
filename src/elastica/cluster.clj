;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://www.eclipse.org/legal/epl-v10.html)
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns elastica.cluster
  "The starting point for interacting with an Elasticsearch cluster via
  the creation of a REST client."
  (:require [elastica.impl.fx :refer [http-interceptor]]
            [elastica.impl.interceptors :as interceptors]
            [utilis.fn :refer [apply-kw]]
            [integrant.core :as ig]))

(declare client)

(defmethod ig/init-key :elastica.cluster/client [_ args]
  (apply-kw client args))

(defmethod ig/halt-key! :elastica.cluster/client [_ client]
  (swap! client dissoc :hosts))

(defn client
  "Creates an instance of a REST client that connects to a cluster.

  The following optional keyword parameters can be used to control
  the behavior:
    :hosts - A seq of hosts provided in the form of [hostname port]. If no host
      information is provided, an attempt will be made to connect to a locally
      running Elasticsearch node."
  [& {:keys [hosts interceptors]
      :or {hosts [["localhost" 9200]]}}]
  (atom {:hosts hosts
         :context {:stack []
                   :queue
                   (-> []
                       (concat interceptors)
                       (concat [http-interceptor]))}}))

(defn run
  [cluster effects]
  (->> effects
       (merge (:context @cluster))
       interceptors/run
       :http/result))

(defn health
  [cluster]
  (run cluster
    {:http
     {:method :get
      :id :cluster-health
      :url {:cluster cluster
            :segments ["_cluster" "health"]}}}))
