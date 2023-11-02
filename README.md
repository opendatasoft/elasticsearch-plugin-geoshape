Elasticsearch GeoShape Plugin
==================================

This plugin can be used to index geo_shape objects in elasticsearch, then aggregate and/or script-simplify them. 

This is an `Ingest`, `Search` and `Script` plugin.



## Installation

`bin/elasticsearch-plugin install https://github.com/opendatasoft/elasticsearch-plugin-geoshape/releases/download/v7.17.6.1/elasticsearch-plugin-geoshape-7.17.6.1.zip"`


## Build
-----
Built with Java 17 and gradle 7.3.1 (but you should use the packaged gradlew included in this repo anyway).


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
- `size`: can be set to define how many buckets should be returned. See elasticsearch official terms aggregation documentation for more explanation. Buckets are ordered by the length (perimeter for polygons) of their shape, longer shapes first.
- `shard_size`: can be used to minimize the extra work that comes with bigger requested `size`. See elasticsearch official terms aggregation documentation for more explanation.


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





## Installation

Current supported version is Elasticsearch 7.x (7.17.6).
You can find past releases [here](https://github.com/opendatasoft/elasticsearch-plugin-geoshape/releases).

The first 3 digits of the plugin version is the corresponding Elasticsearch version. The last digit is used for plugin versioning.

To install it, launch this command in Elasticsearch directory replacing the url by the correct link for your Elasticsearch version (see table)
`bin/elasticsearch-plugin install https://github.com/opendatasoft/elasticsearch-plugin-geoshape/releases/download/v7.17.6.1/elasticsearch-plugin-geoshape-7.17.6.1.zip"`


## Development Environment Setup

Build the plugin using gradle:
```sh
./gradlew build
```

or
```sh
./gradlew assemble  # (to avoid the test suite)
```

Then the following command will start a dockerized ES and will install the previously built plugin:
```sh
docker-compose up
```

Please be careful during development: you'll need to manually rebuild the .zip using `./gradlew build` on each code
change before running `docker-compose` up again.

> NOTE: In `docker-compose.yml` you can uncomment the debug env and attach a REMOTE JVM on `*:5005` to debug the plugin.


## License

This software is under The MIT License (MIT).
