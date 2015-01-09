package com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.Collection;

/**
 * Created with IntelliJ IDEA.
 * User: clement
 * Date: 11/12/14
 * Time: 10:16
 * To change this template use File | Settings | File Templates.
 */
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


    }

    @Override
    Collection<Bucket> getBuckets();

    @Override
    Bucket getBucketByKey(String key);
}
