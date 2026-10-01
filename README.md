# Elasticsearch GeoShape Plugin


This plugin can be used to index geo_shape objects in elasticsearch, then aggregate and/or script-simplify them.

This is an `Ingest`, `Search` and `Script` plugin.


## Installation

Current supported version is Elasticsearch 8.x.
You can find past releases [here](https://github.com/opendatasoft/elasticsearch-plugin-geoshape/releases).

The first 3 digits of the plugin version is the corresponding Elasticsearch version. The last digit is used for plugin versioning.

To install it, launch this command in Elasticsearch directory replacing the url by the correct link for your Elasticsearch version (see table)
`bin/elasticsearch-plugin install https://github.com/opendatasoft/elasticsearch-plugin-geoshape/releases/download/v8.19.19.1/elasticsearch-plugin-geoshape-8.19.19.1.zip"`


## Build

Built with Java 17.


## Usage

### Ingest processor and indexing

A new processor `geo_extension` adds custom fields to the desired geo_shape data object at ingest time.

#### Params

Processor name: `geo_extension`.

|Name|Required|Default|Description|
|----|--------|-------|-----------|
| `field` | yes | - | The geo shape field to use. This parameter accepts wildcard to match multiple `geo_shape` fields
| `path`  | no  | - | The field that contains the field to expand. When using wildcard in `field`, matching will be done under this path only
| `keep_original_shape` | no | `true` | Keep the original unfixed shape in a `shape` field
| `shape_field` | no | `shape` | Name of sub `shape` field
| `fix_shape` | no | `true` |  Fix invalid shape. For the moment it only fixes duplicate consecutive coordinates in polygon (https://github.com/elastic/elasticsearch/issues/14014)
| `fixed_field` | no | `fixed_shape` | Name of sub `fixed_shape` field
| `wkb` | no | `true` | Compute wkb from shape field
| `wkb_field` | no | `wkb` | name of wkb subfield
| `type` | no | `true` | Compute geo shape type (Polygon, point, LineString, ...)
| `type_field` | no | `type` | name of type subfield
| `area` | no | `true` | Compute area of shape
| `area_field` | no | `area` | name of `area` subfield
| `bbox` | no | `true` | Compute geo_point array containing topLeft and bottomRight points of shape envelope
| `bbox_field` | no | `bbox` | name of `bbox` subfield
| `centroid` | no | `true` | Compute geo_point representing shape centroid
| `centroid_field` | no | `centroid` | name of `centroid` subfield
| `hash` | no | `true` | Compute shape digest to perform exact request on shape (in other words: used as a primary key. we may want to use the wkt in the future?)
| `hash_field` | no | `hash` | name of `hash` subfield


#### Example
```

PUT _ingest/pipeline/geo_extension
{
  "description": "Add extra geo fields to geo_shape objects.",
  "processors": [
    {
      "geo_extension": {
        "field": "geoshape_*"
      }
    }
  ]
}
PUT main
{
  "mappings": {
    "dynamic_templates": [
      {
        "geoshapes": {
          "match": "geoshape_*",
          "mapping": {
            "properties": {
              "geoshape": {"type": "geo_shape"},
              "hash": {"type": "keyword"},
              "wkb": {"type": "binary", "doc_values": true},
              "type": {"type": "keyword"},
              "area": {"type": "half_float"},
              "bbox": {"type": "geo_point"},
              "centroid": {"type": "geo_point"}
            }
          }
        }
      }
    ]
  }
}
GET main/_mapping
```

Result:
```
{
  "main": {
    "mappings": {
      "_doc": {
        "dynamic_templates": [
          {
            "geoshapes": {
              "match": "geoshape_*",
              "mapping": {
                "properties": {
                  "geoshape": {
                    "type": "geo_shape"
                  },
                  "hash": {
                    "type": "keyword"
                  },
                  "wkb": {
                    "type": "binary",
                    "doc_values": true
                  },
                  "type": {
                    "type": "keyword"
                  },
                  "area": {
                    "type": "half_float"
                  },
                  "bbox": {
                    "type": "geo_point"
                  },
                  "centroid": {
                    "type": "geo_point"
                  }
                }
              }
            }
          }
        ]
      }
    }
  }
}
```

Document indexing with shape fixing:
```
POST main/_doc?pipeline=geo_extension
{
  "geoshape_0": {
    "type": "Polygon",
    "coordinates": [
      [
                [
                1.6809082031249998,
                49.05227025601607
              ],
              [
                2.021484375,
                48.596592251456705
              ],
              [
                2.021484375,
                48.596592251456705
              ],
              [
                3.262939453125,
                48.922499263758255
              ],
              [
                2.779541015625,
                49.196064000723794
              ],
              [
                2.0654296875,
                49.23194729854559
              ],
              [
                1.6809082031249998,
                49.05227025601607
              ]
      ]
    ]
  }
}
GET main/_search
```

Result:
```
"hits": [
  {
    "_source": {
      "geoshape_0": {
        "area": 0.594432056845634,
        "centroid": {
          "lat": 48.95553463671871,
          "lon": 2.3829210191713015
        },
        "bbox": [
          {
            "lat": 48.596592251456705,
            "lon": 1.6809082031249998
          },
          {
            "lat": 49.23194729854559,
            "lon": 3.262939453125
          }
        ],
        "type": "Polygon",
        "geoshape": {
          "coordinates": [
            [
              [
                1.6809082031249998,
                49.05227025601607
              ],
              [
                2.021484375,
                48.596592251456705
              ],
              [
                3.262939453125,
                48.922499263758255
              ],
              [
                2.779541015625,
                49.196064000723794
              ],
              [
                2.0654296875,
                49.23194729854559
              ],
              [
                1.6809082031249998,
                49.05227025601607
              ]
            ]
          ],
          "type": "Polygon"
        },
        "hash": "-5012816342630707936",
        "wkb": "AAAAAAMAAAABAAAABkAALAAAAAAAQEhMXSKIhttAChqAAAAAAEBIdhR0tDaAQAY8gAAAAABASJkYoAuEDEAAhgAAAAAAQEidsHL20w4/+uT//////0BIhrDKsBJAQAAsAAAAAABASExdIoiG2w=="
      }
    }
  }
```
Note that the duplicated point has been deduplicated.



### Geoshape aggregation

This aggregation creates a bucket for each input shape (based on the hash of its WKB representation) and compute a simplified version of the shape in the bucket.
The simplification part is similar to what is done with the simplify script.
The `size` parameter allows you to retain only the biggest (longer) N shapes.
Moreover, compared to regular search results, results of an aggregation can be [cached by ElasticSearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html#agg-caches).



#### Params

- `field` (mandatory): the field used for aggregating. Must be of wkb type. E.g.: "geoshape_0.wkb".
- `output_format`: the output_format in [`geojson`, `wkt`, `wkb`, `mvt`]. Default to `geojson`. `mvt` returns the MVT command stream instead of a serialized geometry and requires `tile.extent`; see [MVT command stream output](#mvt-command-stream-output) below.
- `simplify`:
  - `zoom`: the zoom level in range [0, 20]. 0 is the most simplified and 20 is the least. Default to 0.
  - `algorithm`: simplify algorithm in [`DOUGLAS_PEUCKER`, `TOPOLOGY_PRESERVING`]. Default to `DOUGLAS_PEUCKER`.
- `tile` (optional): cut the returned shapes down to a bounding box and deliver them in that box's own coordinate space. See [Tile clipping and quantization](#tile-clipping-and-quantization) below.
  - `bbox` (mandatory): the WGS84 window, as `[lon1, lat1, lon2, lat2]`. Longitude must increase (`lon1 < lon2`); latitude may be given in either order, so both mercantile's `(west, south, east, north)` and elasticsearch's envelope ordering `(west, north, east, south)` are accepted. Coordinates must be within +/-180 and +/-90. A box crossing the antimeridian cannot be expressed and is rejected rather than silently reinterpreted as its complement: split it into two requests. Latitudes beyond +/-85.0511, where web mercator ends, are accepted but squash the grid: on a `-90` to `90` box the data only fills a thin band of `[0, extent]`.
  - `extent` (optional): rescale coordinates to the integer grid `[0, extent]` local to `bbox`, e.g. `4096`. Must be at least `1`. When omitted, shapes are returned in web mercator meters without being quantized.
  - `buffer` (optional): fraction of the bounding box size kept on each side, so adjacent windows do not show a seam. Must be a number `>= 0`. Default to `0.0625` (6.25%, the PostGIS default).
- `collect_fields` (optional): return, per bucket, the values of some doc-values fields of the documents that bucket holds. See [Collecting document fields](#collecting-document-fields) below.
  - `fields` (mandatory): the field names to read, e.g. `["id"]`. Each must have doc values.
  - `max_docs_per_bucket` (optional): how many documents per bucket the values are read from. Default to `10`, maximum `100`.
- `size`: can be set to define how many buckets should be returned. See elasticsearch official terms aggregation documentation for more explanation. Buckets are ordered by the length (perimeter for polygons) of their shape, longer shapes first.
- `shard_size`: can be used to minimize the extra work that comes with bigger requested `size`. See elasticsearch official terms aggregation documentation for more explanation.

The aggregation must sit at the top level or under a single-bucket aggregation such as `filter`: under a
multi-bucket one such as `terms`, it is rejected.

#### Coordinate space of the returned shapes

The request determines the space the shapes come back in:

| Request | Coordinate space |
|---|---|
| no `tile` | WGS84 lon/lat (EPSG:4326) |
| `tile` without `extent` | web mercator meters (EPSG:3857) |
| `tile` with `extent` | integer grid local to `bbox`, `[0, extent]`, origin top-left, y downwards. No EPSG code describes this space. |

`output_format` decides how those coordinates are written, not what they are: `mvt` delivers the last
line of the table as a command stream rather than as a geometry.


#### Example

```
GET main/_search?size=0
{
  "aggs": {
    "geo_preview": {
      "geoshape": {
        "field": "geoshape_0.wkb",
        "output_format": "wkb",
        "simplify": {
          "zoom": 8,
          "algorithm": "douglas_peucker"
        },
        "size": 10,
        "shard_size": 10
      }
    }
  }
}
```

Result:
```
"aggregations": {
  "geo_preview": {
    "sum_other_doc_count": 0,
    "buckets": [
      {
        "key": "AAAAAAMAAAABAAAABkAALAAAAAAAQEhMXSKIhts/+uT//////0BIhrDKsBJAQACGAAAAAABASJ2wcvbTDkAGPIAAAAAAQEiZGKALhAxAChqAAAAAAEBIdhR0tDaAQAAsAAAAAABASExdIoiG2w==",
        "digest": "-5012816342630707936",
        "type": "Polygon",
        "doc_count": 1
      }
    ]
  }
}
```

`sum_other_doc_count` is the total number of documents carried by the shapes that are **not** returned (because of `size` or `shard_size`). It is `0` when every shape is returned, and `> 0` when the result is truncated. It is **exact**, including shapes dropped per-shard by `shard_size`, and mirrors the field of the same name on elasticsearch's `terms` aggregation.

Note: because buckets are ranked by perimeter (an intrinsic property of each shape, identical on every shard), `shard_size` does not need to exceed `size` to return the exact top-`size` largest shapes (unlike `terms`, where `shard_size` trades off accuracy).


#### Tile clipping and quantization

The `tile` parameter turns the aggregation's output into a purely local view of a bounding box. It applies, in order:

1. **clip** the shape to `bbox` widened by `buffer`, in WGS84;
2. **simplify** it, if `simplify` was given (so simplification only ever runs on the part of the shape that can be seen);
3. **reproject** to web mercator, and **rescale** to `[0, extent]` when an extent is given;
4. **repair** whatever the rounding broke, dropping the pieces that collapsed;
5. **orient** its rings, on the coordinates that are actually emitted.

**Filter the query on the same window too.** Shapes whose bounding box misses the window are kept
out of the `size`/`shard_size` ranking, so they never take a slot. That test is on the bounding box
only: a shape whose box overlaps the window while the shape itself does not reach it (a long diagonal
line, an L-shaped polygon wrapped around the window) still competes for a slot, and is only dropped
after the ranking. A spatial filter on the query leaves those shapes out as well, and lets
elasticsearch skip the out-of-window documents through its index rather than handing them to the
aggregation, which on a country-wide index is the difference between visiting a few thousand
documents and visiting all of them.

This exists to keep per-coordinate work out of the client. Without it, a client rendering vector tiles reprojects, quantizes and re-winds every coordinate of every shape itself, and pays that cost on the *whole* shape even when only a sliver of it falls inside the tile being served. It is the same job as PostGIS' `ST_AsMVTGeom(geom, bounds, extent, buffer)`.

Clipping in WGS84 is exact, not an approximation: web mercator is axis-separable (x depends on longitude only, y on latitude only), so a rectangle stays a rectangle through the projection.

Things worth knowing about the output:
- The grid origin is the **top-left** corner of `bbox` and **y grows downwards**, the usual tile-local pixel convention. Coordinates are rounded to integers.
- Rings are oriented so that **exteriors have a positive signed area and holes a negative one**, under the surveyor's formula applied to the returned coordinates: on the y-down grid that is the winding the Mapbox Vector Tile spec requires (section 4.3.3.3), and in mercator meters it is the RFC 7946 right-hand rule. A client can encode the rings as they come. To check it, compare signed areas: libraries reading raw ordinates (JTS `Orientation.isCCW`, shapely's `is_ccw`) call these exteriors counter-clockwise, although they read as clockwise on screen once y points down.
- Coordinates falling inside `buffer` land **outside** `[0, extent]`, on purpose: `extent` measures the box, not the buffered window.
- A shape with nothing inside the window is **dropped from the response** rather than returned empty, and its documents are counted in `sum_other_doc_count`. Neighbouring windows that do cover the shape still return it.
- **Returned geometry keeps the dimension of the stored shape.** A polygon merely tangent to the window intersects it along a line, or at a single point; such a result is dropped rather than returned, so a polygon layer never receives a linear feature. A `GeometryCollection` crossing the window edge keeps only its members of the highest dimension, as `ST_AsMVTGeom` does; one entirely inside the window comes back whole.
- **Returned geometry is always valid.** Rounding onto the grid is destructive: sub-pixel holes and parts land on a single point, and a notch narrower than one grid unit closes into a zero-width spike that makes a ring touch itself. Those pieces are repaired away, keeping the visible area intact, and a shape left with nothing at all is dropped like a clipped-away one. A client does not need a geometry-validity repair pass of its own.
- `perimeter`, which orders the buckets, keeps measuring the whole WGS84 shape. Ranking therefore stays comparable across shards and does not depend on how much of a shape a given window happens to show.
- `type` reports the type of the stored shape. A clip can turn a `Polygon` into a `MultiPolygon`; read the returned geometry itself if the effective type matters.


#### Tile example

```
GET main/_search?size=0
{
  "query": {
    "geo_shape": {
      "geoshape_0.fixed_shape": {
        "shape": {
          "type": "envelope",
          "coordinates": [[-5.625, 48.92249926375824], [0.0, 45.089035564831015]]
        },
        "relation": "intersects"
      }
    }
  },
  "aggs": {
    "geo_preview": {
      "geoshape": {
        "field": "geoshape_0.wkb",
        "output_format": "wkb",
        "simplify": {
          "zoom": 6,
          "algorithm": "douglas_peucker"
        },
        "tile": {
          "bbox": [-5.625, 45.089035564831015, 0.0, 48.92249926375824],
          "extent": 4096,
          "buffer": 0.0625
        },
        "size": 100
      }
    }
  }
}
```


#### MVT command stream output

`output_format: mvt` returns each shape as the integer stream a Mapbox Vector Tile feature carries,
rather than as WKB, WKT or GeoJSON. It requires `tile.extent`: the stream describes a pen moving over
the tile grid, so without a grid there is nothing to describe. A request that asks for it without one
is rejected.

```
GET main/_search?size=0
{
  "aggs": {
    "geo_preview": {
      "geoshape": {
        "field": "geoshape_0.wkb",
        "simplify": {"zoom": 5, "algorithm": "topology_preserving"},
        "tile": {"bbox": [0.0, 40.9799, 22.5, 55.7766], "extent": 4096, "buffer": 0.0625},
        "size": 20000,
        "output_format": "mvt"
      }
    }
  }
}
```

`key` then holds an array of unsigned integers instead of a string:

```json
{
  "key": [9, 4102, 3560, 26, 5, 0, 0, 7, 2, 1, 15],
  "digest": "3722138163066086183",
  "type": "MultiPolygon",
  "doc_count": 1
}
```

That array is the value of an MVT feature's `geometry` field: it can be appended to a protobuf
`repeated uint32` as it stands, with no decoding step. The plugin emits the geometry of one shape and
nothing else; assembling layers, keys, values and the tile envelope around it stays the caller's job.

Elasticsearch's own
[vector tile search API](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-vector-tile-api.html)
(`_mvt`) builds whole tiles, but from search hits: one feature per document, at most 10,000 of them.
This aggregation returns each distinct shape once, however many documents share it, ranked by
perimeter and without that cap. On a basic license, `_mvt` on a `geo_shape` field also needs
`grid_precision: 0`, since its default aggregation layer runs `geotile_grid` on that field, a paid
feature.

The encoding follows section 4.3 of the MVT 2.1 spec:

- a **command integer** packs a command id in its low 3 bits and a repeat count in the other 29, as
  `count << 3 | id`. `MoveTo` is 1, `LineTo` is 2, `ClosePath` is 7;
- a point is a pair of **deltas from the pen's current position**, zigzag encoded (`n << 1 ^ n >> 31`)
  so that a small negative delta stays a small unsigned integer;
- the **cursor is shared** across the rings of a polygon and the parts of a multi-geometry;
- a ring is `MoveTo 1`, `LineTo n-1`, `ClosePath`, and does **not** repeat its first point, which
  `ClosePath` already draws.

What this format drops, compared to returning the same geometry as WKB:

- vertices that quantization moved onto the **same grid cell** as their predecessor, and a last vertex
  that landed on the ring's first one. They would draw nothing and cost two integers each;
- a **ring left with fewer than two `LineTo` pairs**, which encloses no area. It is dropped whole,
  header included, and a polygon whose exterior went this way is dropped with its holes;
- a **shape whose every ring went this way**. Its bucket is left out of the response and its documents
  are counted in `sum_other_doc_count`, exactly as for a shape the window emptied.

A `GeometryCollection` has no MVT counterpart, since a feature carries a single geometry type, so a
stored collection is dropped from the response under this format. The other output formats still
return it.

Rings come back already wound the way the spec asks, because `tile` orients them (see above). There is
nothing left for the caller to do per coordinate.

#### Collecting document fields

A bucket groups the documents that share a shape, and `collect_fields` returns some of their field
values alongside it, so a caller can resolve a returned shape back to the docs behind it:

```
GET main/_search?size=0
{
  "aggs": {
    "geo_preview": {
      "geoshape": {
        "field": "geoshape_0.wkb",
        "output_format": "wkb",
        "size": 10000,
        "collect_fields": {
          "fields": ["id"],
          "max_docs_per_bucket": 10
        }
      }
    }
  }
}
```

Each bucket then carries two extra members:

```
{
  "key": "AAAAAAMAAAAB...",
  "digest": "-5012816342630707936",
  "type": "Polygon",
  "doc_count": 3,
  "collected_fields": { "id": ["a7dfd900", "d5c8b04b", "a0210ba1"] },
  "collected_docs_truncated": false
}
```

This is the service a `top_hits` sub-aggregation or `docvalue_fields` provides, without their
per-bucket collector and fetch phase: values are read in the pass that already walks the shape's doc
values. Measured on two real datasets (34,746 polygons with one document each, and 7.1M documents on
2,953 lines), reading a 40-character id from 10 documents per shape adds 0% to 66% to the
aggregation's `took` over two runs. A `top_hits` sub-aggregation reading the same id from doc values
adds 24% to 142% and returns about twice the payload. The ranges move between runs; the ranking does
not. A `terms` sub-aggregation on the id multiplies the `took`
by up to 16 when many documents share a shape, and its buckets count against `search.max_buckets`:
with the 20,000 limit of the measured cluster, it failed on both full datasets.

Things worth knowing about the output:
- **Values come back as their doc-values string representation.** A `keyword` gives its term; a
  numeric, date or boolean field gives the number, as a string (an `unsigned_long` goes through a
  double, so values above 2^53 lose precision). Other field types (`ip`, `binary`, geo fields...)
  have no textual doc-values representation and are rejected, as is a field mapped without doc
  values. A field that is not mapped at all returns an empty array, so a search spanning indices that
  do not all carry it still works.
- **Documents are neither sorted nor filtered.** A bucket returns the first `max_docs_per_bucket`
  documents in collection order, which is index order within a shard and unspecified across shards.
  Sorting is what makes `top_hits` expensive, and it is deliberately not done here.
- **`max_docs_per_bucket` bounds documents, not values.** Every value of every collected document is
  returned, so a multi-valued field can return more values than there are documents. The cut always
  falls on a document boundary, including when the coordinator merges what several shards collected
  for the same shape. The bound is checked against the bucket's `doc_count`, so on an index carrying
  `_doc_count` (rollups, downsampled indices) a single document can exhaust it: fewer documents are
  collected than the bound allows, and `collected_docs_truncated` is `true`.
- **`collected_docs_truncated` is per bucket**, not per field, because the bound is on documents:
  when it bites, it bites on every field at the same document. It is `true` when the bucket holds
  more documents than the values came from, and it is the only thing that distinguishes a complete
  bucket from a cut one.
- A field the collected documents have no value for returns an empty array, not a missing key.
- Memory is bounded by `distinct shapes on the shard x max_docs_per_bucket x value length`, is
  allocated against the request circuit breaker and is released with the aggregation: about 360 bytes
  per collected document for a 40-character id. Values are collected for every shape the shard sees,
  including those `size`/`shard_size` later drops.




### Geoshape simplify script

Search script for simplifying shapes dynamically.


#### Script params

- `field`: the field to apply the script to.
- `zoom`: the zoom level in range [0, 20]. 0 is the most simplified and 20 is the least. Default to 0.
- `algorithm`: simplify algorithm in [`DOUGLAS_PEUCKER`, `TOPOLOGY_PRESERVING`]. Default to `DOUGLAS_PEUCKER`.
- `output_format`: the output_format in [`geojson`, `wkt`, `wkb`]. Default to `geojson`. `mvt` is rejected: it is specific to the geoshape aggregation.


#### Example

```
GET main/_search
{
  "script_fields": {
    "simplified_shape": {
      "script": {
        "lang": "geo_extension_scripts",
        "source": "geo_simplify",
        "params": {
          "field": "geoshape_0",
          "zoom": 8,
          "output_format": "wkt"
        }
      }
    }
  }
}
```

Result:
```
"hits": [
  {
    "fields": {
      "simplified_shape": [
        {
          "real_type": "Polygon",
          "geom": "POLYGON ((2.021484375 48.596592251456705, 1.6809082031249998 49.05227025601607, 2.0654296875 49.23194729854559, 2.779541015625 49.196064000723794, 3.262939453125 48.922499263758255, 2.021484375 48.596592251456705))",
          "type": "Polygon"
        }
      ]
    }
  }
```

## Development Environment Setup

Built with Java 17 and Gradle 8.10.2.

Build the plugin using gradle:

```sh
./gradlew build
```

or
```sh
./gradlew assemble  # (to avoid the test suite)
```

Then you can find the current version of the plugin at e.g. `elasticsearch-plugin-geoshape-7.17.z.d.zip`

In case you have to upgrade Gradle, you can do it with `./gradlew wrapper --gradler-version x.y.z`.

Then the following command will start a dockerized ES and will install the previously built plugin:

```sh
docker compose up
```

Check the Elasticsearch instance at `localhost:9200` and the plugin version with `localhost:9200/_cat/plugins`.

Please be careful during development: you'll need to manually rebuild the .zip using `./gradlew build` on each code
change before running `docker-compose` up again.

> NOTE: In `docker-compose.yml` you can uncomment the debug env and attach a REMOTE JVM on `*:5005` to debug the plugin.

## License

This software is under AGPL (GNU Affero General Public License).
