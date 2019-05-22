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
  (:require [elastica.impl.client :as ec]
            [elastica.impl.coercion :refer [->es-key]]
            [elastica.impl.http :as http]
            [elastica.cluster :as cluster]
            [utilis.fn :refer [fsafe]]
            [utilis.map :refer [compact map-keys]]
            [utilis.logic :refer [xor]]
            [clojure.string :as st]))

(defn bulk!
  [cluster ops]
  (cluster/run cluster
    {:method :post
     :uri (http/uri
           {:segments
            ["_bulk"]})
     :body ops}))
