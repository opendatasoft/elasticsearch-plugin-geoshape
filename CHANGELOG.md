### 8.19.19.1

The README documents each param in full.

* Add an optional `tile` param to the geoshape aggregation: name an XYZ slippy-map tile (`z`, `x`, `y`), clip
  shapes to it (+ buffer), reproject them to web mercator and quantize them to a tile-local integer grid
  (`extent`, default 4096). Returned geometry is valid, keeps the dimension of the stored shape and follows the
  MVT winding. `simplify.zoom` defaults to `tile.z`
* Add the `mvt` value to `output_format`: return each shape as the command stream of an MVT feature, an array of
  unsigned integers, instead of a serialized geometry. Requires `tile`; the `geo_simplify` script rejects it
* Add an optional `collect_fields` param: return, per bucket, the doc-values of some keyword, numeric, date or
  boolean fields of the documents it holds, read from at most `max_docs_per_bucket` documents (default 10,
  maximum 100)
* **Breaking**: a `simplify` without `zoom` or without `algorithm` was silently ignored. `algorithm` now defaults
  to `DOUGLAS_PEUCKER`, as documented, and a missing `zoom` is rejected unless a `tile` provides one
* **Breaking**: geojson output no longer carries the `crs` member, which always announced `EPSG:0`. This affects
  every geojson response, with or without `tile`
* Shapes that the clip empties, or that collapse under quantization or MVT encoding, are dropped from the
  response and counted in `sum_other_doc_count`
* Fix bucket merging: two distinct shapes that simplified or quantized alike were merged into one
* Fix ranking ties: shapes of equal perimeter and doc count, such as points, were kept or dropped by `size` at
  random, so two identical requests could return different shapes
* Fix bucket sub-aggregations, such as `terms`, which failed the search as soon as two shapes were returned
* Reduce sub-aggregations and collected values only for the shapes `size` keeps: those of the dropped shapes
  were computed anyway, and counted against `search.max_buckets`
* Fix a latent NPE when a bucket was skipped on unreadable WKB, and an NPE under a parent that builds the
  aggregation empty, such as a `nested` aggregation whose path is not mapped
* Fix the rendering of the aggregation request, which threw wherever elasticsearch prints a search (slow log,
  task descriptions) and left out `simplify`, `size` and `shard_size`
* Reject the geoshape aggregation under a multi-bucket parent such as `terms`, where every parent bucket
  silently came back with the shapes of all of them
* A shape that JTS cannot process no longer fails the whole search; its bucket is dropped

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
