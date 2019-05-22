(ns elastica.impl.http
  (:require [elastica.impl.coercion :refer [->es ->es-key es-> es-key->]]
            [elastica.impl.client :as ec]
            [elastica.impl.spec :refer [validate]]
            [elastica.impl.valid :as valid]
            [utilis.fn :refer [fsafe]]
            [utilis.map :refer [map-vals compact]]
            #?(:clj [jsonista.core :as json])
            [org.httpkit.client :as http]
            [clojure.string :as st]

            #?(:clj [clojure.spec.alpha :as s]
               :cljs [cljs.spec.alpha :as s])))

;;; Declarations

#?(:clj (def json-mapper (json/object-mapper {:decode-key-fn true})))

(declare query-string base-url serialize-body)

;;; API

(defn uri
  [{:keys [indexes type segments] :as args}]
  (validate :uri/args args)
  (->> segments
       (map
        #(if (keyword? %)
           (name %)
           (str %)))
       (concat
        (map ->es-key
             (cond
               (coll? indexes) indexes
               (nil? indexes) nil
               :else [indexes]))
        (when type [(->es-key type)]))
       (st/join "/")))

(defn default-response-xform
  [{:keys [status headers body error context] :as response}]
  #?(:clj
     (let [parse-body #(try (if (re-find #"application/json" (:content-type headers))
                              (-> % (json/read-value json-mapper) es->)
                              %)
                            (catch Exception e %))]
       (if (#{200 201} status)
         (parse-body body)
         (ex-info
          "HTTP Error"
          (update response :body
                  #(or
                    (try
                      (parse-body %)
                      (catch Exception e nil))
                    %)))))
     :cljs
     (throw (ex-info "Not implemented" {:response response}))))

(defn exists-response-xform
  [{:keys [status] :as response}]
  (if (#{200 404} status)
    ({200 true 404 false} status)
    (default-response-xform response)))

(defn missing-response-xform
  [{:keys [status] :as response}]
  (if (#{404} status)
    nil
    (default-response-xform response)))

(defn run
  [{:keys [url protocol hostname port uri query body headers method response-xform]
    :or {protocol :http
         method :get
         response-xform default-response-xform}
    :as request}]
  (validate :run/args request)
  (let [context (ex-info "Context" {})
        url (or url (->> query
                         ->es
                         (map-vals ->es-key)
                         query-string
                         (str (base-url protocol hostname port) "/" uri)))
        body (when body
               (cond
                 (map? body) (serialize-body body)
                 (coll? body) (str
                               (->> body
                                    (map serialize-body)
                                    (st/join "\n"))
                               "\n")))
        headers (when body (merge {"Content-Type" "application/json"} headers))]
    #?(:clj
       (let [verb ({:put http/put
                    :post http/post
                    :get http/get
                    :delete http/delete
                    :head http/head
                    :options http/options} method)
             result (ec/promise)]
         (verb url {:body body :headers headers}
               (comp (partial deliver result)
                  response-xform
                  #(assoc % :context context)))
         result)
       :cljs (throw (ex-info "Not implemented" {:request request})))))

;;; Private

(defn- serialize-body
  [body]
  #?(:clj (json/write-value-as-string (->es body))
     :cljs (js/JSON.stringify (->es body))))

(defn- base-url
  ([protocol hostname] (base-url protocol hostname nil))
  ([protocol hostname port]
   (str
    (name protocol)
    "://"
    hostname
    (when (and port (not= port 80))
      (str ":" port)))))

(defn- query-string
  [m]
  (when (seq (compact m))
    (->> m
         (map (fn [[k v]] (str (name k) "=" v)))
         (st/join "&")
         (str "?"))))

;;; Specs

(s/def :arg/keyword-or-string
  #(or (keyword? %)
       (string? %)))

(s/def :arg/indexes
  (s/or
   ::coll (s/coll-of :arg/keyword-or-string)
   ::prim :arg/keyword-or-string))
(s/def :arg/type :arg/keyword-or-string)
(s/def :arg/segments
  (s/coll-of
   (s/or
    ::a :arg/keyword-or-string
    ::b integer?)))

(s/def :uri/args
  (s/keys
   :opt-un
   [:arg/indexes
    :arg/type
    :arg/segments]))

(s/def :arg/url valid/url?)
(s/def :arg/protocol #{:http :https})
(s/def :arg/hostname string?)
(s/def :arg/port integer?)
(s/def :arg/uri valid/uri?)
(s/def :arg/body (s/or ::map map? ::coll (s/coll-of map?)))
(s/def :arg/headers (s/map-of :arg/keyword-or-string any?))
(s/def :arg/method #{:put :post :get :delete :head :options})
(s/def :arg/response-xform fn?)
(s/def :run/args
  (s/keys
   :opt-un
   [:arg/url
    :arg/protocol
    :arg/hostname
    :arg/port
    :arg/uri
    :arg/body
    :arg/headers
    :arg/method
    :arg/response-xform]))
