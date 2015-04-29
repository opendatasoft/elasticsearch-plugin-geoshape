# Elasticsearch geo shape plugin

This geo shape plugin can be used to index and aggregate geo shapes in elasticsearch.

This plugin adds a `geo` mapping type, a `geoshape` aggregation, a `geohash_clustering` aggregation and a `geo` REST entry point.

Installation
------------

```
bin/plugin --install geoshape-plugin --url "https://github.com/opendatasoft/elasticsearch-plugin-geoshape/releases/download/v1.5.2.0/elasticsearch-geo-plugin-1.5.2.0.zip"
```

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

It takes `GET` parameters :
 - field (mandatory): `geo` field name. Must be of type `geo`
 - tile_size : tile size in pixel. Defaults to 256
 - output_format :  'wkt', 'wkb' or 'geojson'. Defaults to geojson
 - output_projection: projection to apply on result geometries. Defaults to 'EPSG:4326'

It returns :
 - shape in wkt, wkb or geojson
 - digest : geoshape digest (in order to perform search request in it)
 - doc_count : number of identical shapes
 - cluster_count : number of shapes in this bucket. Small shapes aggregation only.
 - grid : geohash grid for this bucket. Small shapes aggregation only.

Example :

`localhost:9200/my_index/geo_type/_geo/12/1036/704?field=geo_field`

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
         "grid": "u09tgr"
      },
      {
         "shape": "POINT (2.24587597613 48.8691433358)",
         "digest": "9079382294515706678",
         "type": "Point",
         "doc_count": 5,
         "cluster_count": 5,
         "grid": "u09w50"
      },
      {
         "shape": "POINT (2.29329076205 48.8577766649)",
         "digest": "6585224492059141239",
         "type": "Point",
         "doc_count": 3,
         "cluster_count": 3,
         "grid": "u09tun"
      },
      {
         "shape": "POINT (2.2599223736999994 48.8606198209)",
         "digest": "1243503675762065766",
         "type": "Point",
         "doc_count": 3,
         "cluster_count": 3,
         "grid": "u09tgq"
      },
      {
         "shape": "POINT (2.25120424857 48.864782427799994)",
         "digest": "2446849682065168706",
         "type": "Point",
         "doc_count": 3,
         "cluster_count": 3,
         "grid": "u09tgp"
      },
      {
         "shape": "POINT (2.25293802515 48.8471789821)",
         "digest": "4881855873819433358",
         "type": "Point",
         "doc_count": 3,
         "cluster_count": 3,
         "grid": "u09tgk"
      }
   ]
}
```
