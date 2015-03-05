package com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.Collection;

public interface GeoShape extends MultiBucketsAggregation {

    public static enum Algorithm {
        DOUGLAS_PEUCKER,
        TOPOLOGY_PRESERVING
    }


    public static interface Bucket extends MultiBucketsAggregation.Bucket {

        /**
         * @return  The geohash of the cell as a geo point
         */
        BytesRef getShapeAsByte();

        long getShapeHash();

        String getType();

        String getHash();
    }

    @Override
    Collection<Bucket> getBuckets();

    @Override
    Bucket getBucketByKey(String key);
}
