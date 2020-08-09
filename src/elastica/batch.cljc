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
  (:refer-clojure :exclude [flush])
  (:require [elastica.impl.client :as ec]
            [elastica.impl.coercion :refer [->es-key]]
            [elastica.impl.http :as http]
            [elastica.cluster :as cluster]
            [utilis.timer :as ut]
            [utilis.fn :refer [fsafe apply-kw]]
            [utilis.map :refer [compact map-keys]]
            [utilis.logic :refer [xor]]
            [clojure.string :as st]
            [integrant.core :as ig]))

(declare processor shutdown enqueue process-queue)

(defmethod ig/init-key :elastica.batch/processor [_ args]
  (apply-kw processor (:cluster args) args))

(defmethod ig/halt-key! :elastica.batch/processor [_ processor]
  (shutdown processor))

(defn processor
  [cluster & {:keys [count interval]}]
  (let [queue (atom [])]
    (merge
     {:cluster cluster
      :queue (atom [])}
     (when count
       {:count count})
     (when interval
       {:interval interval
        :timer (ut/run-every (partial process-queue cluster queue) interval)}))))

(defn shutdown
  [processor]
  (when (:timer processor) (ut/cancel (:timer processor)))
  (process-queue (:cluster processor) (:queue processor)))

(defn put!
  [processor index doc & {:keys [id] :as args}]
  (enqueue processor
           {"index" (merge {"_index" (->es-key index)} (when id {"_id" id}))}
           doc))

(defn flush
  [processor]
  (process-queue (:cluster processor) (:queue processor)))

;;; Private

(defn- enqueue
  [processor & operations]
  (let [queue (:queue processor)]
    (locking queue
      (swap! queue #(apply conj % operations))
      (when (and (:count processor)
                 (>= (count @queue) (:count processor)))
        (process-queue (:cluster processor) queue)))))

(defn- process-queue
  [cluster queue]
  (locking queue
    (when (not-empty @queue)
      (cluster/run cluster
        {:method :post
         :uri (http/uri {:segments ["_bulk"]})
         :headers {"Content-Type" "application/x-ndjson"}
         :body @queue})
      (reset! queue []))))
