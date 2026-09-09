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
- `output_format`: the output_format in [`geojson`, `wkt`, `wkb`]. Default to `geojson`.
- `simplify`:
  - `zoom`: the zoom level in range [0, 20]. 0 is the most simplified and 20 is the least. Default to 0.
  - `algorithm`: simplify algorithm in [`DOUGLAS_PEUCKER`, `TOPOLOGY_PRESERVING`]. Default to `DOUGLAS_PEUCKER`.
- `tile` (optional): cut the returned shapes down to a bounding box and deliver them in that box's own coordinate space. See [Tile clipping and quantization](#tile-clipping-and-quantization) below.
  - `bbox` (mandatory): the WGS84 window, as `[lon1, lat1, lon2, lat2]`. Longitude must increase (`lon1 < lon2`); latitude may be given in either order, so both mercantile's `(west, south, east, north)` and elasticsearch's envelope ordering `(west, north, east, south)` are accepted. Coordinates must be within +/-180 and +/-90. A box crossing the antimeridian cannot be expressed and is rejected rather than silently reinterpreted as its complement: split it into two requests.
  - `extent` (optional): rescale coordinates to the integer grid `[0, extent]` local to `bbox`, e.g. `4096`. When omitted, shapes are returned in web mercator meters without being quantized.
  - `buffer` (optional): fraction of the bounding box size kept on each side, so adjacent windows do not show a seam. Default to `0.0625` (6.25%, the PostGIS default).
- `size`: can be set to define how many buckets should be returned. See elasticsearch official terms aggregation documentation for more explanation. Buckets are ordered by the length (perimeter for polygons) of their shape, longer shapes first.
- `shard_size`: can be used to minimize the extra work that comes with bigger requested `size`. See elasticsearch official terms aggregation documentation for more explanation.

#### Coordinate space of the returned shapes

The request determines the space the shapes come back in:

| Request | Coordinate space |
|---|---|
| no `tile` | WGS84 lon/lat (EPSG:4326) |
| `tile` without `extent` | web mercator meters (EPSG:3857) |
| `tile` with `extent` | integer grid local to `bbox`, `[0, extent]`, origin top-left, y downwards. No EPSG code describes this space. |


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

**Filter the query on the same window too, for performance.** The answer does not depend on it:
shapes that the clip would empty are kept out of the `size`/`shard_size` ranking, so a slot is never
spent on something invisible. But a spatial filter lets elasticsearch skip the out-of-window
documents through its index rather than handing them to the aggregation, which on a country-wide
index is the difference between visiting a few thousand documents and visiting all of them.

This exists to keep per-coordinate work out of the client. Without it, a client rendering vector tiles reprojects, quantizes and re-winds every coordinate of every shape itself, and pays that cost on the *whole* shape even when only a sliver of it falls inside the tile being served. It is the same job as PostGIS' `ST_AsMVTGeom(geom, bounds, extent, buffer)`.

Clipping in WGS84 is exact, not an approximation: web mercator is axis-separable (x depends on longitude only, y on latitude only), so a rectangle stays a rectangle through the projection.

Things worth knowing about the output:
- The grid origin is the **top-left** corner of `bbox` and **y grows downwards**, the usual tile-local pixel convention. Coordinates are rounded to integers.
- Rings are oriented so that **exteriors have a positive signed area and holes a negative one**, under the surveyor's formula applied to the returned coordinates. In the y-down grid that is exactly what the Mapbox Vector Tile spec requires of exteriors (section 4.3.3.3), and it reads as clockwise on screen. A client can encode the rings as they come, with no winding check of its own. Without `extent` the same rule applies to mercator meters, where a positive exterior area is the RFC 7946 right-hand rule.
- Beware when checking this yourself: orientation is a property of the coordinate *values*, so the y-down flip reverses it, and "clockwise on screen" corresponds to what most libraries report as counter-clockwise (JTS `Orientation.isCCW`, shapely's `is_ccw`), since they read raw ordinates. Compare signed areas rather than reasoning about the flip.
- Coordinates falling inside `buffer` land **outside** `[0, extent]`, on purpose: `extent` measures the box, not the buffered window.
- A shape with nothing inside the window is **dropped from the response** rather than returned empty, and its documents are counted in `sum_other_doc_count`. Neighbouring windows that do cover the shape still return it.
- **Returned geometry keeps the dimension of the stored shape.** A polygon merely tangent to the window intersects it along a line, or at a single point; such a result is dropped rather than returned, so a polygon layer never receives a linear feature.
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




### Geoshape simplify script

Search script for simplifying shapes dynamically.


#### Script params

- `field`: the field to apply the script to.
- `zoom`: the zoom level in range [0, 20]. 0 is the most simplified and 20 is the least. Default to 0.
- `algorithm`: simplify algorithm in [`DOUGLAS_PEUCKER`, `TOPOLOGY_PRESERVING`]. Default to `DOUGLAS_PEUCKER`.
- `output_format`: the output_format in [`geojson`, `wkt`, `wkb`]. Default to `geojson`.


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
