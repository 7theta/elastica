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
            [elastica.query :as eq]
            [elastica.batch :as ecb]

            [integrant.core :as ig]

            [integrant.repl :refer [clear go halt init reset reset-all set-prep!]]
            [integrant.repl.state :refer [system]]
            [clojure.tools.namespace.repl :refer [refresh refresh-all disable-reload!]]

            [clojure.repl :refer [apropos dir doc find-doc pst source]]
            [clojure.reflect :refer [reflect]]
            [clojure.pprint :refer [pprint]]

            [clojure.test :refer [run-tests run-all-tests]]
            [clojure.string :as st]))

(disable-reload! (find-ns 'integrant.core))

(def dev-config
  {:elastica.cluster/client {}})

(ig/load-namespaces dev-config)

(set-prep! (constantly dev-config))

;;; Test Utilities

(def prn-monitor (Object.))
(defn prn*
  [& args]
  (locking prn-monitor
    (apply prn args)))

(defn client [] (:elastica.cluster/client system))

(defn gen-index-name
  []
  (st/lower-case (str "test_index_" (gensym))))

(defn gen-document
  []
  {:bar "baz"})

(defn gen-type
  []
  :foo)

(defn gen-id
  []
  (st/lower-case (str "test_doc_" (gensym))))

(defn with-index
  [f]
  (let [index-name (gen-index-name)]
    (try
      (f index-name)
      (catch Exception e
        (throw e))
      (finally
        (try @(ei/delete-index! (client) index-name)
             (catch Exception e))))))

;;; Index Tests

(defn test-create-index
  []
  (with-index
    (fn [index-name]
      @(ei/create-index! (client) index-name)
      @(ei/index-exists? (client) index-name))))

(defn test-delete-index
  []
  (with-index
    (fn [index-name]
      @(ei/create-index! (client) index-name)
      (and @(ei/index-exists? (client) index-name)
           (do @(ei/delete-index! (client) index-name)
               (false? @(ei/index-exists? (client) index-name)))))))

(defn test-get-index
  []
  (with-index
    (fn [index-name]
      @(ei/create-index! (client) index-name)
      (let [index-map @(ei/index (client) index-name)]
        (some? (get index-map (keyword index-name)))))))

(defn test-index-exists
  []
  (with-index
    (fn [index-name]
      (and
       (true?
        (do @(ei/create-index! (client) index-name)
            @(ei/index-exists? (client) index-name)))
       (false?
        (do @(ei/delete-index! (client) index-name)
            @(ei/index-exists? (client) index-name)))))))

(defn test-ensure-index
  []
  (with-index
    (fn [index-name]
      @(ei/ensure-index! (client) index-name)
      @(ei/index-exists? (client) index-name))))

;;; Document Tests

(defn test-put-get
  []
  (with-index
    (fn [index-name]
      (let [document (gen-document)
            type (gen-type)]
        @(ei/ensure-index! (client) index-name)
        (boolean
         (when-let [id (e/id @(e/put! (client) index-name type document))]
           (= document (:_source @(e/get (client) index-name type id)))))))))

(defn test-update
  []
  (with-index
    (fn [index-name]
      (let [document (gen-document)
            update-doc {:r (rand-int 1000)}
            type (gen-type)]
        @(ei/ensure-index! (client) index-name)
        (boolean
         (when-let [id (e/id @(e/put! (client) index-name type document))]
           @(e/update! (client) index-name type id :update-doc update-doc)
           (= (merge document update-doc)
              (:_source @(e/get (client) index-name type id)))))))))

(defn test-upsert
  []
  (with-index
    (fn [index-name]
      (let [document (gen-document)
            update-doc {:r (rand-int 1000)}
            type (gen-type)
            id (gen-id)]
        @(ei/ensure-index! (client) index-name)
        (dotimes [_ 2]
          @(e/upsert! (client) index-name type id :insert-doc document :update-doc update-doc))
        (= (merge document update-doc)
           (:_source @(e/get (client) index-name type id)))))))

(defn test-delete
  []
  (with-index
    (fn [index-name]
      (let [document (gen-document)
            type (gen-type)]
        @(ei/ensure-index! (client) index-name)
        (boolean
         (when-let [id (e/id @(e/put! (client) index-name type document))]
           (try @(e/get (client) index-name type id)
                (catch Exception e
                  (-> e ex-data :es-error :found false?)))))))))

;;; Search/Query Tests

(defn test-exact-match-search
  []
  (with-index
    (fn [index-name]
      (let [document (gen-document)
            type (gen-type)]
        @(ei/ensure-index! (client) index-name)
        (when-let [id (e/id
                       @(e/put!
                         (client) index-name type document
                         :refresh :wait-for))]
          (let [k (first (keys document))]
            (->> @(e/search
                   (client) [index-name]
                   (eq/match (name k) (get document k)))
                 :hits
                 :hits
                 first
                 :_source
                 (= document))))))))

(defn test-metaphone-search
  "More options: https://www.elastic.co/guide/en/elasticsearch/plugins/6.x/analysis-phonetic-token-filter.html"
  []
  (with-index
    (fn [index-name]
      (let [document1 {:name "John Smith"}
            document2 {:name "Jonnie Smythe"}
            type (name (gen-type))]
        @(ei/ensure-index!
          (client) index-name
          :settings {:shards 8
                     :replicas 1
                     "analysis"
                     {"filter"
                      {"dbl_metaphone"
                       {"type" "phonetic"
                        "encoder" "double_metaphone"}}
                      "analyzer" {"dbl_metaphone"
                                  {"tokenizer" "standard"
                                   "filter" "dbl_metaphone"}}}})
        @(ei/put-mapping!
          (client) index-name type
          {"name"
           {"type" "text"
            "fields" {"phonetic"
                      {"type" "text"
                       "analyzer" "dbl_metaphone"}}}})
        @(e/put! (client) index-name type document1 :refresh :wait-for)
        @(e/put! (client) index-name type document2 :refresh :wait-for)
        (->> @(e/search
               (client) [index-name]
               (eq/match "name.phonetic" "Jahnnie Smith" :operator :and))
             :hits :hits count
             (= 2))))))

;;; Run All

;; TODO - convert everything to defspec/test.check and move to test/check
(defn run-dev-tests
  []
  (do (halt) (go))
  (->> (ns-publics 'dev)
       keys
       (filter (comp (partial re-find #"^test") str))
       (map (fn [test-fn] [(str test-fn) @(resolve test-fn)]))
       (map (fn [[test-name test-fn]]
              (println "test results:" [test-name {:result (test-fn)}])))
       doall)
  nil)
