;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://www.eclipse.org/legal/epl-v10.html)
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns elastica.impl.request
  "Functions used to generate and dispatch Elasticsearch requests.

  These functions are an implementation detail and should not be called directly
  by consumers of the library."
  (:refer-clojure :exclude [await])
  (:require [elastica.impl.coercion :refer [->es-value ->clj-value]]
            [utilis.fn :refer [apply-kw]])
  (:import  [org.elasticsearch.action ActionListener]
            [org.elasticsearch.action.index IndexRequest]
            [org.elasticsearch.action.update UpdateRequest]
            [org.elasticsearch.action.delete DeleteRequest]
            [org.elasticsearch.script Script]
            [java.util Map]))

(defmacro prepare-callback
  "Wraps the callback in an ActionListener"
  [callback response-parser]
  `(reify ActionListener
     (onResponse [_ response#] (~callback (delay (~response-parser response#))))
     (onFailure [_ error#] (~callback (delay error#)))))

(defmacro run-async
  "Generate code that will perform the request asynchronously and call
  the provided callback once it is completed."
  ([cluster method request response-parser callback]
   `(~method ~cluster ~request (prepare-callback ~callback ~response-parser)))
  ([builder response-parser callback]
   `(.execute ~builder (prepare-callback ~callback ~response-parser))))

(defmacro run-sync
  "Generate code that will perform the request synchronously on the
  current thread."
  ([cluster method request response-parser]
   `(~response-parser (.actionGet (~method ~cluster ~request))))
  ([builder response-parser]
   `(~response-parser (.actionGet (.execute ~builder)))))

(defmacro run
  "Generate code that will perform the request synchronously or asynchronously
  if a 'callback' is provided."
  ([cluster method request response-parser]
   `(run ~cluster ~method ~request ~response-parser nil))
  ([cluster method request response-parser callback]
   `(if ~callback
      (run-async ~cluster ~method ~request ~response-parser ~callback)
      (run-sync ~cluster ~method ~request ~response-parser)))
  ([builder response-parser]
   `(run ~builder ~response-parser nil))
  ([builder response-parser callback]
   `(if ~callback
      (run-async ~builder ~response-parser ~callback)
      (run-sync ~builder ~response-parser))))

(defmacro extract-header
  "Extract the common header values from the response.

  This is implemented as a macro instead of a function to take
  advantage of the type hints on 'response' in the calling location."
  [response]
  `{:_index (.getIndex ~response)
    :_type (.getType ~response)
    :_id (.getId ~response)
    :_version (.getVersion ~response)})

(defn ^IndexRequest index-request
  [index type doc & {:keys [id parent version ttl]}]
  (cond->
      (.source (if id
                 (IndexRequest. ^String index ^String (->es-value type) ^String (str id))
                 (IndexRequest. ^String index ^String (->es-value type)))
               ^Map (->es-value doc))
    parent (.parent parent)
    version (.version version)
    ttl (.ttl ^long ttl)))

(defn ^UpdateRequest update-request
  [index type id update-doc-or-script & {:keys [detect-no-op retry-on-conflict
                                                parent version]}]
  (let [[update-doc script] (if (instance? Script update-doc-or-script)
                              [nil update-doc-or-script]
                              [update-doc-or-script nil])]
    (cond-> (UpdateRequest. ^String index ^String (->es-value type) ^String (str id))
      (not (nil? detect-no-op)) (.detectNoop detect-no-op)
      (not (nil? retry-on-conflict)) (.retryOnConflict retry-on-conflict)
      parent (.parent parent)
      update-doc (.doc ^Map (->es-value update-doc))
      script (.script ^Script script)
      version (.version version))))

(defn ^UpdateRequest upsert-request
  [index type id insert-doc update-doc-or-script & {:as args}]
  (doto ^UpdateRequest (apply-kw update-request index type id update-doc-or-script args)
    (.upsert ^IndexRequest (apply-kw index-request index type id insert-doc args))))

(defn ^DeleteRequest delete-request
  [index type id & {:keys [version]}]
  (cond-> (DeleteRequest. index (->es-value type) id)
    version (.version version)))
