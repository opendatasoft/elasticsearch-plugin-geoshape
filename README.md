Elasticsearch GeoShape Plugin
==================================

This plugin can be used to index geo_shape objects in elasticsearch, then aggregate and/or script-simplify them. 

This is an `Ingest`, `Search` and `Script` plugin.



## Installation

`bin/plugin --install geoshape-plugin --url https://github.com/opendatasoft/elasticsearch-plugin-geoshape/releases/download/v7.0.1.0/elasticsearch-plugin-geoshape-7.0.1.0.zip"`



## Usage

### Ingest processor and indexing 
A new processor `geo_extension` adds custom fields to the desired geo_shape data object at ingest time.

#### Params

Processor name: `geo_extension`.

|Name|Required|Default|Description|
|----|--------|-------|-----------|
| `field` | yes | - | The geo shape field to use. This parameter accepts wildcard to match multiple `geo_shape` fields.
| `path`  | no  | - | The field that contains the field to expand. When using wildcard in `field`, matching will be done under this path only.
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
PUT main/_mapping
{
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
POST main/_doc
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
GET main/_search?pipeline=geo_extension
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

Geoshape aggregation based on auto-computed shape hash.



#### Params
- `field` (mandatory): the field used for aggregating. Must be of wkb type. E.g.: "geoshape_0.wkb".
- `output_format`: the output_format in [`geojson`, `wkt`, `wkb`]. Default to `geojson`.
- `simplify`:
  - `zoom`: the zoom level in range [0, 20]. 0 is the most simplified and 20 is the least. Default to 0.
  - `algorithm`: simplify algorithm in [`DOUGLAS_PEUCKER`, `TOPOLOGY_PRESERVING`]. Default to `DOUGLAS_PEUCKER`.
- `size`: can be set to define how many term buckets should be returned out of the overall terms list. See elasticsearch official terms aggregation documentation for more explanation.
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

Plugin versions are available for (at least) all minor versions of Elasticsearch since 6.3.

The first 3 digits of plugin version is Elasticsearch versioning. The last digit is used for plugin versioning under an elasticsearch version.

To install it, launch this command in Elasticsearch directory replacing the url by the correct link for your Elasticsearch version (see table)
`bin/plugin --install geoshape-plugin --url "https://github.com/opendatasoft/elasticsearch-plugin-geoshape/releases/download/v7.0.1.0/elasticsearch-plugin-geoshape-7.0.1.0.zip"`

| elasticsearch version | plugin version | plugin url |
| --------------------- | -------------- | ---------- |
| 1.6.0 | 1.6.0.5 | https://github.com/opendatasoft/elasticsearch-plugin-geoshape/releases/download/v1.6.0.5/elasticsearch-geo-plugin-1.6.0.5.zip|
| 6.3.2 | 6.3.2.0 | https://github.com/opendatasoft/elasticsearch-plugin-geoshape/releases/download/v6.3.2.0/elasticsearch-plugin-geoshape-6.3.2.0.zip |
| 6.4.3 | 6.4.3.0 | https://github.com/opendatasoft/elasticsearch-plugin-geoshape/releases/download/v6.4.3.0/elasticsearch-plugin-geoshape-6.4.3.0.zip |
| 6.5.4 | 6.5.4.2 | https://github.com/opendatasoft/elasticsearch-plugin-geoshape/releases/download/v6.5.4.2/elasticsearch-plugin-geoshape-6.5.4.2.zip |
| 6.6.2 | 6.6.2.0 | https://github.com/opendatasoft/elasticsearch-plugin-geoshape/releases/download/v6.6.2.0/elasticsearch-plugin-geoshape-6.6.2.0.zip |
| 6.7.1 | 6.7.1.0 | https://github.com/opendatasoft/elasticsearch-plugin-geoshape/releases/download/v6.7.1.0/elasticsearch-plugin-geoshape-6.7.1.0.zip |
| 6.8.0 | 6.8.0.0 | https://github.com/opendatasoft/elasticsearch-plugin-geoshape/releases/download/v6.8.0.0/elasticsearch-plugin-geoshape-6.8.0.0.zip |
| 7.0.1 | 7.0.1.0 | https://github.com/opendatasoft/elasticsearch-plugin-geoshape/releases/download/v7.0.1.0/elasticsearch-plugin-geoshape-7.0.1.0.zip |


## License

This software is under The MIT License (MIT).
