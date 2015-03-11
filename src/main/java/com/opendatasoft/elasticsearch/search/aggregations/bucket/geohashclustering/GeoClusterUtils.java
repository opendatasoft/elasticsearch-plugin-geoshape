package com.opendatasoft.elasticsearch.search.aggregations.bucket.geohashclustering;

import org.elasticsearch.common.geo.GeoUtils;

public class GeoClusterUtils {

    public static double getMeterByPixel(int zoom, double lat) {
        return (GeoUtils.EARTH_EQUATOR / 256) * (Math.cos(Math.toRadians(lat)) / Math.pow(2, zoom));
    }
}

