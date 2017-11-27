;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://www.eclipse.org/legal/epl-v10.html)
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(defproject com.7theta/elastica "6.0.0-0.1"
  :description "An idiomatic clojure interface to Elasticsearch"
  :url "https://github.com/7theta/elastica"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]

                 [com.7theta/utilis "1.0.4"]

                 [http-kit "2.2.0"]
                 [cheshire "5.8.0"]
                 [clj-time "0.14.2"]
                 [inflections "0.13.0"]

                 [integrant "0.6.1"]
                 [clojure-future-spec "1.9.0-beta4"]]
  :profiles {:dev {:global-vars {*warn-on-reflection* true}
                   :dependencies [[integrant/repl "0.2.0"]]
                   :source-paths ["dev"]}}
  :scm {:name "git"
        :url "https://github.com/7theta/elastica"})
