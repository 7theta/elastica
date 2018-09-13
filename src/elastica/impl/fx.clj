;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://www.eclipse.org/legal/epl-v10.html)
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns elastica.impl.fx
  (:require [elastica.impl.coercion :refer [->es ->es-key es-> es-key->]]
            [elastica.impl.interceptors :refer [->interceptor]]
            [elastica.impl.client :as ec]
            [utilis.fn :refer [fsafe]]
            [utilis.map :refer [map-vals compact]]
            [jsonista.core :as json]
            [org.httpkit.client :as http]))

(def json-mapper (json/object-mapper {:decode-key-fn true}))

(def exists-response-xform
  #(get {200 true 404 false} (:status %)
        (ex-info "HTTP Error"
                 {:status (:status %)
                  :request (:opts %)
                  :es-error (:body %)})))

(defn- intercept-before
  [context]
  (if-let [http (:http context)]
    (update context :http
            (fn [http]
              (-> http
                  (update :url (fsafe
                                (fn [url]
                                  (if (string? url)
                                    url
                                    (ec/url
                                     (:cluster url)
                                     :indices (if (coll? (:indices url))
                                                (map ->es-key (:indices url))
                                                (->es-key (:indices url)))
                                     :type (->es-key (:type url))
                                     :segments (:segments url)
                                     :query (->> url :query ->es (map-vals ->es-key)))))))
                  (update :body (fsafe (comp json/write-value-as-string ->es)))
                  (update :es-> #(or % es->)))))
    context))

(defn- intercept-after
  [context]
  (if-let [http (:http context)]
    (if-let [verb ({:put http/put
                    :post http/post
                    :get http/get
                    :delete http/delete
                    :head http/head
                    :options http/options} (:method http))]
      (let [result (ec/promise)]
        (verb
         (str (:url http))
         (compact (dissoc http :url :method :es-> :response-xform))
         (fn [response]
           (let [es-> (:es-> http)
                 response (update
                           response :body
                           (comp (fsafe
                               #(es-> (json/read-value % json-mapper)))
                              not-empty))
                 response-xform
                 (or (:response-xform http)
                     (fn [{:keys [status headers body error]}]
                       (if (#{200 201} status)
                         body
                         (ex-info "HTTP Error"
                                  {:status status
                                   :request (dissoc http :url :es-> :response-xform)
                                   :response response
                                   :es-error (:body response)}))))]
             (deliver result (response-xform response)))))
        (assoc context :http/result result))
      (throw (ex-info "Unrecognized HTTP verb" {:http http})))
    context))

(def http-interceptor
  (->interceptor
   :id :elastica.impl.coercion/es-interceptor
   :before (fn [context] (intercept-before context))
   :after (fn [context] (intercept-after context))))
