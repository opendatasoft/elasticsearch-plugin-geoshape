# Elasticsearch geo shape plugin

This geo shape plugin can be used to index and aggregate geo shapes in elasticsearch.

This plugin adds a `geo` mapping type, a `geoshape` aggregation, a `geohash_clustering` aggregation and a `geo` REST entry point.

Installation
------------

```
bin/plugin --install geoshape-plugin --url "https://github.com/opendatasoft/elasticsearch-plugin-geoshape/releases/download/v1.6.0.4/elasticsearch-geo-plugin-1.6.0.4.zip"
```

| elasticsearch  | Geoshape Plugin     |
|----------------|---------------------|
| 1.6.0          | 1.6.0.3             |
| 1.5.2          | 1.5.2.4             |
| 1.4.5          | 1.4.5.2             |

### Geo mapping type

`geo` mapping can replace `geo_shape` type. It parses `GeoJSON` shapes, and indexes them in elasticsearch. Compared to geo_shape type it adds extra sub fields:
 - wkb : indexed wkb field, used to aggregate shapes
 - type : geo shape type (Polygon, point, LineString, ...) for searching on a specific type
 - area : area of Shape
 - bbox : geoPoint array containing topLeft and bottomRight points of shape envelope
 - hash : shape digest to perform exact request on shape
 - centroid : geoPoint representing shape centroid

Geo mapper computes, depending on shape length, a specific precision for each shape (corresponding to `precision` parameter).
This precision can be boosted with `boost_precision` parameter.

Its parameters are slightly different than geoshape ones:
 - tree : geohash or quadtree
 - distance_error_pct : same as geo_shape type
 - boost_precision : double that can be used to boost shape precision. Between 0 and 1, defaults to 0.

### Geoshape aggregation

`geoshape` aggregation is a multi-bucket aggregation that returns a bucket for each shape.

`geoshape` parameters are :
 - field : a wkb field. (Maybe change to `geo` type field instead)
 - size : restrict number of results
 - output_format : define output shape format ('wkt', 'wkb', 'geojson'). Defaults to geojson
 - simplify : used to simplify aggregated shapes. Takes a parameter dict
  - zoom : level of zoom. Mandatory in simplify dict
  - algorithm : algorithm used for shape simplification (DOUGLAS_PEUCKER, TOPOLOGY_PRESERVING). Default to DOUGLAS_PEUCKER
 - clipped : used to return a shape that are clipped to a defined envelope. Take a dict. WARNING, when used, zoom must be set in simplify in order to work (will be fixed in a future release)
  - envelope : elasticsearch envelope where shapes must be clipped on. Mandatory when clipped is used
  - buffer : number of pixels added to envelope for clipping

Example of use :
```
{
  "aggs": {
    "geo": {
      "geoshape": {
        "field": "geo_shape.wkb",
        "output_format": "wkt",
        "simplify": {
          "zoom": 8,
          "algorithm": "DOUGLAS_PEUCKER"
        }
      }
    }
  }
}
```

Result :

```
{
  "aggregations": {
    "geo": {
      "buckets": [
      {
         "key": "POINT (2.2538285063 48.865022534)",
         "digest": "4521908731506274962",
         "type": "Point",
         "doc_count": 1
      },
      {
         "key": "POINT (2.31333976248 48.8652536076)",
         "digest": "-3513121227624068596",
         "type": "Point",
         "doc_count": 1
      },
      {
         "key": "POINT (2.2529555706 48.846044762)",
         "digest": "-5055786055234076365",
         "type": "Point",
         "doc_count": 1
      },
      {
         "key": "POINT (2.25320074406 48.867043584)",
         "digest": "167833499021969215",
         "type": "Point",
         "doc_count": 1
      },
      {
         "key": "POINT (2.4126175672099994 48.8333849101)",
         "digest": "7300553048122261648",
         "type": "Point",
         "doc_count": 1
      },
      {
         "key": "POINT (2.2924448277 48.8619170093)",
         "digest": "-2232618493206154845",
         "type": "Point",
         "doc_count": 1
      }
    }
  }
}
```

### Geohash clustering aggregation

This aggregations computes a geohash precision from a `zoom` and a `distance` (in pixel).
It groups points (from `field` parameter) into buckets that represent geohash cells and computes each bucket's center.
Then it merges these cells if the distance between two clusters' centers is lower than the `distance` parameter.

```json
{
  "aggregations": {
    "<aggregation_name>": {
      "geohash_clustering": {
        "field": "<field_name>",
        "zoom": "<zoom>"
      }
    }
  }
}
```
Input parameters :
 - `field` must be of type geo_point.
 - `zoom` is a mandatory integer parameter between 0 and 20. It represents the zoom level used in the request to aggregate geo points.

The plugin aggregates these points in geohash with a "good" precision depending on the zoom provided. Then it merges clusters based on distance (in pixels).
Default distance is set to 100, but it can be set to another integer in the request.

For example :

```json
{
    "aggregations" : {
        "my_cluster_aggregation" : {
            "geohash_clustering": {
                "field": "geo_point",
                "zoom": 0,
                "distance": 50
            }
        }
    }
}
```

```json
{
    "aggregations": {
         "my_cluster_aggregation": {
            "buckets": [
               {
                  "key": "u0",
                  "doc_count": 90293,
                  "geohash_grids": [
                     [
                        "u0"
                     ]
                  ],
                  "cluster_center": {
                     "type": "point",
                     "coordinates": [
                        2.32920361762,
                        48.8449899502
                     ]
                  }
               }
            ]
         }
    }

}
```

### Geo tile REST entry

This entry point generates "smart" results for a specific geo tile. The main purpose of this REST entry point is to return a tile that can be easily rendered with mapping applications like `mapnik`.

Format is based on TMS format : /{index}/{type}/_geo/{zoom}/{x}/{y}
For more information about this format : http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames

It takes parameters that can passed as `GET` parameters or in request body:
 - `field` (mandatory): `geo` field name. Must be of type `geo`
 - `tile_size` : tile size in pixel. Defaults to 256
 - `output_format` :  'wkt', 'wkb' or 'geojson'. Defaults to geojson
 - `output_projection` : projection to apply on result geometries. Defaults to 'EPSG:4326'
 - `aggs` or `aggregations` (only on request body) : Performs elasticsearch aggregations https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html. Aggregations results are added for each shape object in an `aggregations` field.

It returns :
 - shape in wkt, wkb or geojson
 - digest : geoshape digest (in order to perform search request in it)
 - doc_count : number of identical shapes
 - cluster_count : number of shapes in this bucket. Small shapes aggregation only.
 - grid : geohash grid for this bucket. Small shapes aggregation only.

Example :

`curl -XGET 'localhost:9200/my_index/geo_type/_geo/12/1036/704' -d '{"field": "geo_field", "aggs": {"term": {"terms":{"field": "term_field"}}}}'`

```
{
   "time": 12,
   "count": 40,
   "shapes": [
      {
         "shape": "POINT (2.26174532022 48.8633555664)",
         "digest": "5980304178196219950",
         "type": "Point",
         "doc_count": 6,
         "cluster_count": 6,
         "grid": "u09tgr",
         "aggregations": {
            "term": {
                ...
            }
         }
      },
      {
         "shape": "POINT (2.24587597613 48.8691433358)",
         "digest": "9079382294515706678",
         "type": "Point",
         "doc_count": 5,
         "cluster_count": 5,
         "grid": "u09w50",
         "aggregations": {
            "term": {
                ...
            }
         }
      },
      {
         "shape": "POINT (2.29329076205 48.8577766649)",
         "digest": "6585224492059141239",
         "type": "Point",
         "doc_count": 3,
         "cluster_count": 3,
         "grid": "u09tun",
         "aggregations": {
            "term": {
                ...
            }
         }
      },
      {
         "shape": "POINT (2.2599223736999994 48.8606198209)",
         "digest": "1243503675762065766",
         "type": "Point",
         "doc_count": 3,
         "cluster_count": 3,
         "grid": "u09tgq",
         "aggregations": {
            "term": {
                ...
            }
         }
      },
      {
         "shape": "POINT (2.25120424857 48.864782427799994)",
         "digest": "2446849682065168706",
         "type": "Point",
         "doc_count": 3,
         "cluster_count": 3,
         "grid": "u09tgp",
         "aggregations": {
            "term": {
                ...
            }
         }
      },
      {
         "shape": "POINT (2.25293802515 48.8471789821)",
         "digest": "4881855873819433358",
         "type": "Point",
         "doc_count": 3,
         "cluster_count": 3,
         "grid": "u09tgk",
         "aggregations": {
            "term": {
                ...
            }
         }
      }
   ]
}
```

## Geo preview with shape simplification and conversion

This script adds a generated field containing the simplified or converted geoshape for the requested field.

It takes parameters that can passed in a `POST` request body:
 - `geoshapestringfield` (mandatory): `geo` field name. Must be of type `geo`
 - `simplify` : boolean for whether or not the shape should be simplified according to zoom level
 - `zoom` : the zoom level for simplification (1 giving the most simplified result, mandatory if simplify=true)
 - `output_format` : format of the output (can be `geojson`, `wkt` or `wkb`, defaults to `geojson`)
 - `algorithm` : algorithm used for simplification, can be `douglas_peucker` (default) or `topology_preserving`
 - `is_point` : boolean (defaults to false) used to indicate the shape is a point

Example:

```
# POST /test_index_geo/records/_search

{
  "script_fields": {
    "simplified": {
      "script": "geopreview",
      "lang":"native",
      "params": {
        "geoshapestringfield": "geo_shape",
        "zoom": 3,
        "simplify": true
      }
    }
  }
}

# Response

{
  "took": 89,
  "timed_out": false,
  "_shards": {
    "total": 5,
    "successful": 5,
    "failed": 0
  },
  "hits": {
    "total": 1,
    "max_score": 1,
    "hits": [
      {
        "_index": "test_index_geo",
        "_type": "records",
        "_id": "AVrsDq2oNWoYxp2XjII_",
        "_score": 1,
        "fields": {
          "simplified": [
            {
              "real_type": "Polygon",
              "geom": "{\"type\":\"Polygon\",\"coordinates\":[[[-84.22119140625,34.985003130171066],[-84.32281494140625,34.9895035675793],[-84.29122924804688,35.21981940793435],[-84.04266357421875,35.27701633139884],[-84.01931762695312,35.41479572901859],[-83.88473510742186,35.516578738902936],[-83.49746704101562,35.563512051219696],[-82.96737670898438,35.793310688351724],[-82.91244506835938,35.92353244718235],[-82.69546508789062,36.04465753921525],[-82.61306762695312,36.060201412392914],[-82.5677490234375,35.951329861522666],[-82.35488891601562,36.117908916563685],[-82.03628540039062,36.12900165569652],[-81.86325073242188,36.33504067209607],[-81.70669555664062,36.33504067209607],[-81.68197631835938,36.58686302344181],[-75.79605102539062,36.54936246839778],[-75.3936767578125,35.639441068973916],[-75.487060546875,35.18727767598896],[-75.9210205078125,35.04798673426734],[-76.53076171875,34.53371242139567],[-76.761474609375,34.63320791137959],[-77.376708984375,34.45674800347809],[-77.5909423828125,34.3207552752374],[-77.9754638671875,33.73804486328907],[-78.11279296875,33.8521697014074],[-78.4808349609375,33.815666308702774],[-79.6728515625,34.8047829195724],[-80.782470703125,34.836349990763864],[-80.9307861328125,35.092945313732635],[-81.0516357421875,35.02999636902566],[-81.0516357421875,35.137879119634185],[-82.40295410156249,35.22318504970181],[-83.1060791015625,35.003003395276714],[-84.22119140625,34.985003130171066]],[[-75.5914306640625,35.74205383068037],[-75.706787109375,35.74205383068037],[-75.706787109375,35.634976650677295],[-76.0858154296875,35.29943548054543],[-76.4208984375,35.25907654252574],[-76.5252685546875,35.10642805736423],[-76.2066650390625,34.994003757575776],[-75.56396484375,35.32633026307483],[-75.5914306640625,35.74205383068037]]]}",
              "type": "Polygon"
            }
          ]
        }
      }
    ]
  }
}
```

