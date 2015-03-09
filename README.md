# Elasticsearch geo shape plugin

This geo shape plugin can be used to index and aggregate geo shapes in elasticsearch.

For achieving this, the plugin adds a `geo` mapping type, a `geoshape` aggregation and and a `geo` rest entry point.

### geo mapping type

`geo` mapping can be used exactly where `geo_shape` could be used. It parsed `GeoJSON` shapes, and indexing them in elasticsearch. Compared to geo_shape type it adds extra sub fields :
 - wkb : indexed wkb field, used to aggregate shapes
 - type : geo shape type (Polygon, point, LineString, ...) for searching on a specific type
 - area : area of Shape
 - bbox : geoPoint array containing topLeft and bottomRigth points of shape envelope
 - hash : shape digest to make exact request on shape
 - centroid : geoPoint representing shape centroid

### Geoshape aggregation

`geoshape` aggregation is a multi buckets aggregation that returns a bucket for each shape.

`geoshape` parameters are :
 - field : a wkb field. (Maybe change to `geo` type field instead)
 - size : restrict number of results
 - output_format : define output shape format ('wkt', 'wkb', 'geojson'). Default to geojson
 - simplify : used to simplify aggregated shapes. Take a parameter dict
  - zoom : level of zoom. Mandatory in simplify dict
  - algorithm : algorithm used for shape simplification (DOUGLAS_PEUCKER, TOPOLOGY_PRESERVING). Default to DOUGLAS_PEUCKER
 - clipped : used to return a shape that are clipped to a defined envelope. Take a dict. WARNING, when used, zoom must be set in simplify in order to work (will be fixed in a future release)
  - envelope : elasticsearch envelope where shapes must be clipped on. Mandatory when clipped is used
  - buffer : nb pixel buffer add to envelope for clipping

Example of use :
```
{
  "aggs": {
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
```

### Geo tile REST entry

This entry point generates "smart" results for a specific geo tile.

Format is based on TMS format : /{index}/{type}/_geo/{zoom}/{x}/{y}
For more information about this format : http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames

It takes `Get` parameters :
 - field (mandatory): `geo` field to return
 - tile_size : tile size in pixel. Default to 256
 - output_format :  'wkt', 'wkb' or 'geojson'. Default to geojson
