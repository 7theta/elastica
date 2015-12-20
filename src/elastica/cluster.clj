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
  the creation of a ClusterClient.

  The application can choose to join the cluster as a non-data node via
  'node-client' or choose to interact with it remotely via a
  'transport-client'."
  (:require [com.stuartsierra.component :as component])
  (:import  [org.elasticsearch.node NodeBuilder Node]
            [org.elasticsearch.client Client]
            [org.elasticsearch.client.transport TransportClient TransportClient$Builder]
            [org.elasticsearch.common.transport InetSocketTransportAddress]
            [org.elasticsearch.common.settings Settings Settings$Builder]
            [java.net InetAddress]))

(declare start-native-client stop-native-client)

;;; Types

(defrecord ClusterClient [client-type local path-home
                          cluster-name node-name
                          hosts auto-discover]
  component/Lifecycle
  (start [component] (start-native-client component))
  (stop [component] (stop-native-client component))

  java.io.Closeable
  (close [component] (.stop component)))

;;; Public

(defn node-client
  "Creates an instance of a ClusterClient that participates as a non-data node
  in the Elasticsearch cluster. The 'cluster-name' must be specified along with
  the 'home-path'.

  The following optional keyword parameters can be used to control
  the behavior:
    :node-name - The node name to assign to the current node. If one is not
       provided, one will be automatically assigned.
    :local - A boolean indicating whether the node should be started as a
       local embedded node. Useful primarily for testing."
  [cluster-name path-home & {:keys [node-name local]
                             :or {local false}}]
  (map->ClusterClient {:client-type :node
                       :node-name node-name
                       :path-home path-home
                       :local local
                       :cluster-name cluster-name}))

(defn transport-client
  "Creates an instance of a ClusterClient that connects to a remote cluster
  as a transport client and uses the nodes in a round robin request cycle.

  The following optional keyword parameters can be used to control
  the behavior:
    :hosts - A seq of hosts provided in the form of [hostname port]. If no host
      information is provided, an attempt will be made to connect to a locally
      running Elasticsearch node.
    :auto-discover - A boolean indicating whether the client should attempt
      to discover additional nodes in the cluster and connect to them."
  [cluster-name & {:keys [hosts auto-discover]
                   :or {hosts [["localhost" 9300]]
                        auto-discover true}}]
  (map->ClusterClient {:client-type :transport
                       :cluster-name cluster-name
                       :hosts hosts
                       :auto-discover auto-discover}))

;;; Implementation

(defmulti ^:private start-native-client :client-type)

(defmethod ^:private start-native-client :node
  [component]
  (if-not (:started component)
    (let [^NodeBuilder node-builder (doto (NodeBuilder/nodeBuilder)
                                      (.clusterName (:cluster-name component))
                                      (.data false) ; Do not hold data
                                      (.client true)) ; Do not accept external requests
          _ (when-let [local (:local component)] (.local node-builder local))
          _ (doto (.settings node-builder)
              (.put "path.home" ^String (:path-home component))
              (.put "http.enabled" false))
          _ (when-let [node-name (:node-name component)]
              (.put (.settings node-builder) "node.name" ^String node-name))
          ^Node node (.node node-builder)] ; Build and start the node
      (assoc component :es-node node :es-client (.client node) :started true))
    component))

(defmethod ^:private start-native-client :transport
  [component]
  (if-not (:started component)
    (let [^Settings$Builder settings (Settings/settingsBuilder)
          _ (doto settings
              (.put "cluster.name" ^String (:cluster-name component))
              (.put "client.transport.sniff" ^boolean (:auto-discover component)))
          ^TransportClient client (-> (TransportClient/builder)
                                      (.settings settings)
                                      .build)]
      (doseq [[^String host ^int port] (:hosts component)]
        (.addTransportAddress client
                              (InetSocketTransportAddress. (InetAddress/getByName host)
                                                           port)))
      (assoc component :es-client client :started true))
    component))

(defn- stop-native-client
  [component]
  (if (:started component)
    (if-let [^Client client (:es-client component)]
      (do (.close client)
          ;;(when-let [^Node node (:es-node component)] (.close node))
          (dissoc component :es-node :es-client :started))
      component)
    component))
