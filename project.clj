;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://www.eclipse.org/legal/epl-v10.html)
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(defproject com.7theta/elastica "1.0.1"
  :description "An idiomatic clojure interface to native Elasticsearch"
  :url "https://github.com/7theta/elastica"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]

                 [com.7theta/utilis "0.8.2"]

                 [org.elasticsearch/elasticsearch "2.4.0"]
                 [com.vividsolutions/jts "1.13" :exclusions [xerces/xercesImpl]]

                 [com.stuartsierra/component "0.3.1"]]
  :profiles {:dev {:global-vars {*warn-on-reflection* true}
                   :dependencies [[reloaded.repl "0.2.3"]
                                  [org.clojure/tools.namespace "0.2.11"]
                                  [org.clojure/test.check "0.9.0"]
                                  [com.gfredericks/test.chuck "0.2.7"]]
                   :source-paths ["dev"]}}
  :scm {:name "git"
        :url "https://github.com/7theta/elastica"})
