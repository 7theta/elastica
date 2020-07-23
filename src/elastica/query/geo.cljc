;;   Copyright (c) 7theta. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://www.eclipse.org/legal/epl-v10.html)
;;   which can be found in the LICENSE file at the root of this
;;   distribution.
;;
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any others, from this software.

(ns elastica.query.geo
  "Functions for the generation of geo-spatial queries")

(defn bounding-box
  "A query for documents based on a point location using a bounding box.

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-bounding-box-query.html"
  [field top-left bottom-right]
  {:geo_bounding_box {field {:top_left top-left
                             :bottom_right bottom-right}}})

(defn distance
  "A query for documents that exist within a specific distance from a geo point

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-distance-query.html"
  [field point distance & {:keys [distance-type]}]
  {:pre [(#{:arc :plane} distance-type)]}
  {:geo_distance {:distance distance
                  field point
                  :distance_type distance-type}})

(defn point
  "Generates a point shape that can be used in a elastica.query.geo/shape query"
  [lon lat]
  {:lon lon :lat lat})

(defn envelope
  "Generates a envelope (rectangle) shape that can be used in a
  elastica.query.geo/shape query"
  [top-left bottom-right]
  [[(:lon top-left) (:lat top-left)]
   [(:lon bottom-right) (:lat bottom-right)]])

(defn shape
  "A query for documents that satisfy the given 'relation'(:disjoint :intersects
  :within) with the 'shape'

  https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-shape-query.html"
  [field shape relation & {:keys [boost ignore-unmapped]}]
  {:pre [(#{:disjoint :intersects :within} relation)]}
  {:geo_shape {field {:shape shape
                      :relation relation
                      :boost boost
                      :ignore_unmapped ignore-unmapped}}})
