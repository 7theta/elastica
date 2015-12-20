;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://www.eclipse.org/legal/epl-v10.html)
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns elastica.script
  (:require [elastica.impl.coercion :refer [->es-value]])
  (:import  [org.elasticsearch.script Script ScriptService$ScriptType]))

(defn script
  "Creates and returns scripts that can be used as part of updates and queries."
  ([script-source]
   (Script. script-source))
  ([script-source type lang parameters]
   {:pre [(#{:inline :indexed :file} type)]}
   (Script. script-source
            (case type
              :inline ScriptService$ScriptType/INLINE
              :indexed ScriptService$ScriptType/INDEXED
              :file ScriptService$ScriptType/FILE)
            (->es-value lang)
            (->es-value parameters))))
