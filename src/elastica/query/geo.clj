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
  "Functions for the generation of geo-spatial queries"
  (:require [elastica.impl.coercion :refer [->es-value]])
  (:import  [org.elasticsearch.index.query
             GeoBoundingBoxQueryBuilder GeoDistanceQueryBuilder
             GeoShapeQueryBuilder]
            [org.elasticsearch.common.geo ShapeRelation]
            [org.elasticsearch.common.geo.builders ShapeBuilder
             PointBuilder PolygonBuilder EnvelopeBuilder]
            [com.vividsolutions.jts.geom Coordinate]))

(defn bounding-box
  "A query for documents based on a point location using a bounding box.

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-geo-bounding-box-query.html"
  [field top-left bottom-right & {:keys [query-name]}]
  (cond-> (doto (GeoBoundingBoxQueryBuilder. (->es-value field))
            (.topLeft (:lat top-left) (:lon top-left))
            (.bottomRight (:lat bottom-right) (:lon bottom-right)))
    query-name (.queryName query-name)))

(defn distance
  "A query for documents that exist within a specific distance from a geo point

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-geo-distance-query.html"
  [field point distance & {:keys [query-name]}]
  (cond-> (doto (GeoDistanceQueryBuilder. (->es-value field))
            (.point (:lat point) (:lon point))
            (.distance distance))
    query-name (.queryName query-name)))

(defn point
  "Generates a point shape that can be used in a elastica.query.geo/shape query"
  [lon lat]
  (doto (PointBuilder.)
    (.coordinate (Coordinate. lon lat))))

(defn envelope
  "Generates a envelope (rectangle) shape that can be used in a
  elastica.query.geo/shape query"
  [top-left bottom-right]
  (doto (EnvelopeBuilder.)
    (.topLeft (:lon top-left) (:lat top-left))
    (.bottomRight (:lon bottom-right) (:lat bottom-right))))

(defn shape
  "A query for documents that satisfy the given 'relation'(:disjoint :intersects
  :within) with the 'shape'

  https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-geo-shape-query.html"
  [field shape relation & {:keys [boost query-name]}]
  {:pre [(#{:disjoint :intersects :within} relation)]}
  (cond-> (GeoShapeQueryBuilder. (->es-value field) shape
                                 (case relation
                                   :disjoint ShapeRelation/DISJOINT
                                   :intersects ShapeRelation/INTERSECTS
                                   :within ShapeRelation/WITHIN))
    query-name (.queryName query-name)
    boost (.boost ^double boost)))
