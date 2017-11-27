;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://www.eclipse.org/legal/epl-v10.html)
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns dev
  "Tools for interactive development with the REPL. This file should
  not be included in a production build of the application."
  (:require [elastica.cluster :as ec]
            [elastica.index :as ei]
            [elastica.core :as e]

            ;; [elastica.batch :as ecb]
            ;; [elastica.query :as eq]g

            [integrant.core :as ig]

            [integrant.repl :refer [clear go halt init reset reset-all set-prep!]]
            [integrant.repl.state :refer [system]]
            [clojure.tools.namespace.repl :refer [refresh refresh-all disable-reload!]]

            [clojure.repl :refer [apropos dir doc find-doc pst source]]
            [clojure.reflect :refer [reflect]]
            [clojure.pprint :refer [pprint]]

            [clojure.test :refer [run-tests run-all-tests]]))

(disable-reload! (find-ns 'integrant.core))

(def dev-config
  {:elastica.cluster/client
   {}})

(ig/load-namespaces dev-config)

(set-prep! (constantly dev-config))
