(ns elastica.impl.spec
  (:require #?(:clj [clojure.spec.alpha :as s]
               :cljs [cljs.spec.alpha :as s])))

(defn validate
  [spec form]
  (when (not (s/valid? spec form))
    (throw
     (ex-info
      "Spec validation failed"
      {:spec spec
       :form form
       :explain (with-out-str (s/explain spec form))})))
  form)
