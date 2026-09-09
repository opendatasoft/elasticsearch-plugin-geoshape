### 8.19.19.1

The README documents each param in full.

* Add an optional `tile` param to the geoshape aggregation: clip shapes to a bbox (+ buffer), reproject them to
  web mercator and, with `extent`, quantize them to a tile-local integer grid. Returned geometry is valid, keeps
  the dimension of the stored shape and follows the MVT winding. A bbox crossing the antimeridian is rejected
* **Breaking**: geojson output no longer carries the `crs` member, which always announced `EPSG:0`. This affects
  every geojson response, with or without `tile`
* Shapes that the clip empties, or that collapse under quantization, are dropped from the response and counted in
  `sum_other_doc_count`
* Fix bucket merging: two distinct shapes that simplified or quantized alike were merged into one
* Fix a latent NPE when a bucket was skipped on unreadable WKB
* A shape that JTS cannot process no longer fails the whole search; its bucket is dropped
* **Upgrade note**: the aggregation's wire format changes, even for requests that use none of the new params.
  Nodes on 8.19.19.0 and 8.19.19.1 cannot exchange a geoshape aggregation, so during a rolling restart a search
  whose geoshape aggregation spans both versions fails

### 7.17.28.0

* Repackaging for Elasticsearch 7.17.28

### 7.17.6.1

* Fix bbox on linestrings and points

### 7.17.6.0

* Repackaging for ES 7.17.6

### 7.17.1.2

* Simplify consistency: script is now using the same tolerance value as the agg one

### 7.17.1.1

* Fix deduplication of points
* Fix handling of GeometryCollections
* Add tests
