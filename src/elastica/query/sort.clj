;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://www.eclipse.org/legal/epl-v10.html)
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns elastica.query.sort
  "Functions for generating various sort criteria that can be passed to
  elastica.core/search"
  (:require [elastica.impl.coercion :refer [->es-value]])
  (:import  [org.elasticsearch.search.sort SortOrder FieldSortBuilder
             ScriptSortBuilder GeoDistanceSortBuilder]
            [org.elasticsearch.common.unit DistanceUnit]
            [org.elasticsearch.script Script]))

(defn field
  "Used to sort the results by the supplied 'field' in the given
  'order' (:asc or :desc.)"
  [field order]
  {:pre [(#{:asc :desc} order)]}
  (doto (FieldSortBuilder. (->es-value field))
    (.order (case order
              :asc SortOrder/ASC
              :desc SortOrder/DESC))))

(defn geo-distance
  "Used to sort the results by the distance from the provided
  'lon' and 'lat' in the 'units' and 'order' (:asc or :desc.)
  specified. The distance is computed based on the location
  in the 'field' of each result."
  [field lon lat unit order]
  {:pre [(#{:asc :desc} order)
         (#{:mm :cm :m :km
            :in :ft :yard :mi
            :nm} unit)]}
  (doto (GeoDistanceSortBuilder. (->es-value field))
    (.point lat lon)
    (.unit (case unit
             :mm DistanceUnit/MILLIMETERS
             :cm DistanceUnit/CENTIMETERS
             :m DistanceUnit/METERS
             :km DistanceUnit/KILOMETERS

             :in DistanceUnit/INCH
             :ft DistanceUnit/FEET
             :yard DistanceUnit/YARD
             :mi DistanceUnit/MILES
             :nm DistanceUnit/NAUTICALMILES))
    (.order (case order
              :asc SortOrder/ASC
              :desc SortOrder/DESC))))

(defn script
  "Used to sort the results by the return value of 'script' of 'type'
  in the 'order' (:asc or :desc.) specified."
  [script type order]
  {:pre [(#{:asc :desc} order)]}
  (doto (ScriptSortBuilder. ^Script script ^String (->es-value type))
    (.order (case order
              :asc SortOrder/ASC
              :desc SortOrder/DESC))))
