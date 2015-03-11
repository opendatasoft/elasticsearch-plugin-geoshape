package com.opendatasoft.elasticsearch.search.aggregations.bucket.geohashclustering;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.Collection;

/**
 * A {@code geohash_grid} aggregation. Defines multiple buckets, each representing a cell in a geo-grid of a specific
 * precision.
 */
public interface GeoHashClustering extends MultiBucketsAggregation {

    /**
     * A bucket that is associated with a {@code geohash_grid} cell. The key of the bucket is the {@cod geohash} of the cell
     */
    public static interface Bucket extends MultiBucketsAggregation.Bucket {

        /**
         * @return  The geohash of the cell as a geo point
         */
        GeoPoint getKeyAsGeoPoint();

        /**
         * @return  A numeric representation of the geohash of the cell
         */
        Number getKeyAsNumber();

        /**
         * @return  center of cluster
         */
        GeoPoint getClusterCenter();
    }

    /**
     * @return  The buckets of this aggregation (each bucket representing a geohash grid cell)
     */
    @Override
    Collection<Bucket> getBuckets();

    @Override
    Bucket getBucketByKey(String key);

    Bucket getBucketByKey(Number key);

    Bucket getBucketByKey(GeoPoint key);

}
