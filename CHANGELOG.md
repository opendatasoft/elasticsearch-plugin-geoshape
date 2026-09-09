### 8.19.19.1

* Add an optional `tile` param to the geoshape aggregation: clip shapes to a bbox (+ buffer), reproject
  to web mercator and quantize to a tile-local integer grid, in a single pass inside the plugin.
  Returned rings follow the MVT winding convention and returned geometry is always valid, so a vector
  tile consumer needs no per-coordinate pass of its own
* **Breaking**: geojson output no longer carries the `crs` member. It always announced `EPSG:0`, which
  is not a CRS, and RFC 7946 removed the member from GeoJSON. This affects every geojson response,
  including those that do not use `tile`. The coordinate space is determined by the request and is
  documented in the README
* Shapes the clip leaves empty, and shapes that collapse under quantization, are dropped from the
  response instead of being returned empty or invalid
* Clipping no longer returns geometry of a lower dimension than the shape it came from: a polygon
  merely tangent to the window is dropped instead of being returned as a line or a point
* `tile.bbox` is validated: coordinates must be within +/-180 and +/-90, and longitude must increase.
  A box crossing the antimeridian is rejected rather than silently reinterpreted as its complement
* Fix bucket merging: the coordinator keyed buckets on the hash of the *returned* geometry, so two
  distinct shapes that simplified or quantized alike were merged into one and a feature was lost
* Fix a latent NPE: buckets skipped on unreadable WKB used to leave null holes in the returned array
* A shape that cannot be processed no longer fails the whole search; the bucket is dropped

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
