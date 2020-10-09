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
            [clojure.core.async :refer [chan close! <!! >!!]]
            [utilis.timer :as ut]
            [utilis.fn :refer [fsafe apply-kw]]
            [utilis.map :refer [compact map-keys]]
            [utilis.logic :refer [xor]]
            [clojure.string :as st]
            [integrant.core :as ig]))

(declare processor shutdown enqueue process-queue enqueue-worker http-worker-pool)

(defmethod ig/init-key :elastica.batch/processor [_ args]
  (apply-kw processor (:cluster args) args))

(defmethod ig/halt-key! :elastica.batch/processor [_ processor]
  (shutdown processor))

(defn processor
  [cluster & {:keys [count
                     interval
                     enqueue-buffer-size
                     http-workers]
              :or {enqueue-buffer-size (or (when count
                                             (min 1024 (* 2 count)))
                                           100)
                   http-workers 8}}]
  (let [queue (atom [])
        enqueue-ch (chan enqueue-buffer-size)
        http-worker-ch (chan)
        processor {:cluster cluster
                   :promises (atom [])
                   :queue queue
                   :http-worker-ch http-worker-ch
                   :http-worker-pool (http-worker-pool http-worker-ch http-workers)
                   :enqueue-ch enqueue-ch
                   :enqueue-worker (enqueue-worker enqueue-ch)}]
    (cond-> processor
      count (merge {:count count})
      interval (merge {:interval interval
                       :timer (ut/run-every #(process-queue processor) interval)}))))

(defn shutdown
  [{:keys [timer enqueue-ch http-worker-ch] :as processor}]
  ((fsafe ut/cancel) timer)
  ((fsafe close!) enqueue-ch)
  ((fsafe close!) http-worker-ch)
  (process-queue processor))

(defn put!
  "Submit a document 'doc' to the batch processor to be indexed in 'index'. When
  this document has been indexed as part of a batch, the return promise 'p' will
  deliver the result of having indexed the batch."
  [processor index doc & {:keys [id] :as args}]
  (let [p (promise)]
    (>!! (:enqueue-ch processor)
         {:processor processor
          :index index
          :doc doc
          :args args
          :p p})
    p))

(defn flush
  [processor]
  (process-queue processor))

;;; Private

(defn- enqueue
  [{:keys [processor p index doc args]}]
  (let [{:keys [queue promises]} processor
        {:keys [id]} args]
    (locking queue
      (swap! promises conj p)
      (swap! queue #(apply conj %
                           [{"index" (merge {"_index" (->es-key index)}
                                            (when id {"_id" id}))}
                            doc]))
      (when (and (:count processor)
                 (>= (count @queue) (:count processor)))
        (process-queue processor)))))

(defn- process-queue
  [{:keys [cluster queue promises http-worker-ch]}]
  (when-let [batch-job (locking queue
                         (when (not-empty @queue)
                           (let [operations @queue
                                 promises* @promises]
                             (reset! promises [])
                             (reset! queue [])
                             (when (seq operations)
                               [operations promises*]))))]
    (>!! http-worker-ch [cluster batch-job])))

(defn- enqueue-worker
  [ch]
  (future
    (try
      (loop []
        (when-let [enqueue-job (<!! ch)]
          (enqueue enqueue-job)
          (recur)))
      (catch Exception e
        (println e "Exception occurred in enqueue worker.")))))

(defn- http-worker-pool
  [ch n]
  (dotimes [i n]
    (future
      (try
        (loop []
          (when-let [[cluster [operations promises]] (<!! ch)]
            (try (let [result @(cluster/run cluster
                                 {:method :post
                                  :uri (http/uri {:segments ["_bulk"]})
                                  :headers {"Content-Type" "application/x-ndjson"}
                                  :body operations})]
                   (doseq [p promises]
                     (deliver p result)))
                 (catch Exception e
                   (println e "Exception occurred submitting batch")))
            (recur)))
        (catch Exception e
          (println e "Exception occurred in enqueue worker."))))))
