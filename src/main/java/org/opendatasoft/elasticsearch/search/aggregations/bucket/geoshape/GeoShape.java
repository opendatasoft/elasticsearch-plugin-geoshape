package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

/**
 * An aggregation of geo_shape using wkb field.
 */
public interface GeoShape extends MultiBucketsAggregation {
    interface Bucket extends MultiBucketsAggregation.Bucket {}
    enum Algorithm {
        DOUGLAS_PEUCKER,
        TOPOLOGY_PRESERVING
    }

    @Override
    List<? extends Bucket> getBuckets();

}
