;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://www.eclipse.org/legal/epl-v10.html)
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns elastica.impl.client
  (:refer-clojure :exclude [promise])
  (:require [elastica.impl.url :as url]
            [clojure.string :as st]))

(defn base-url
  [cluster & {:keys [method] :or {method :http}}]
  (let [[hostname port] (-> cluster deref :hosts first)]
    (url/url {:method method
              :hostname hostname
              :port port})))

(defn url
  [cluster
   & {:keys [indices method query segments query type]
      :or {method :http}}]
  (let [[hostname port] (-> cluster deref :hosts first)]
    (str (url/url
          {:method method
           :hostname hostname
           :port port
           :query query
           :segments (concat
                      (map name (cond
                                  (coll? indices) indices
                                  (nil? indices) nil
                                  :else [indices]))
                      (when type [type])
                      segments)}))))

(defn promise
  "Return a promise that behaves like clojure.core/promise, except
  that if the value contained is a Throwable, it will be thrown on
  deref."
  []
  (let [d (java.util.concurrent.CountDownLatch. 1)
        v (atom d)]
    (reify
      clojure.lang.IDeref
      (deref [_] (.await d) (if (instance? Throwable @v) (throw @v) @v))
      clojure.lang.IBlockingDeref
      (deref
          [_ timeout-ms timeout-val]
        (if (.await d timeout-ms java.util.concurrent.TimeUnit/MILLISECONDS)
          (if (instance? Throwable @v) (throw @v) @v)
          timeout-val))
      clojure.lang.IPending
      (isRealized [this]
        (zero? (.getCount d)))
      clojure.lang.IFn
      (invoke
          [this x]
        (when (and (pos? (.getCount d))
                   (compare-and-set! v d x))
          (.countDown d)
          this)))))
