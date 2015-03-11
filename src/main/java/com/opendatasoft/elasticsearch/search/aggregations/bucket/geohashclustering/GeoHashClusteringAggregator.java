package com.opendatasoft.elasticsearch.search.aggregations.bucket.geohashclustering;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GeoHashClusteringAggregator extends BucketsAggregator {

    private static final int INITIAL_CAPACITY = 50; // TODO sizing

    private final ValuesSource.GeoPoint valuesSource;
    private LongHash bucketOrds;
    private LongObjectPagedHashMap<ClusterCollector> clusterCollectors;
    private MultiGeoPointValues geoValues;
    private int zoom;
    private int distance;

    public static class ClusterCollector {
//        private List<GeoPoint> geoPoints;
        private double latMin;
        private double latMax;
        private double lonMin;
        private double lonMax;


        public ClusterCollector(GeoPoint geoPoint) {
//            geoPoints = new ArrayList<GeoPoint>();
//            geoPoints.add(geoPoint);
            latMin = latMax = geoPoint.getLat();
            lonMin = lonMax = geoPoint.getLon();
        }

        public void addPoint(GeoPoint point) {
//            geoPoints.add(point);
            latMin = Math.min(point.getLat(), latMin);
            latMax = Math.max(point.getLat(), latMax);
            lonMin = Math.min(point.getLon(), lonMin);
            lonMax = Math.max(point.getLon(), lonMax);
        }

        public double getLatMin() {
            return latMin;
        }

        public double getLatMax() {
            return latMax;
        }

        public double getLonMin() {
            return lonMin;
        }

        public double getLonMax() {
            return lonMax;
        }
    }

    public GeoHashClusteringAggregator(String name, AggregatorFactories factories, ValuesSource.GeoPoint valuesSource,
                                       AggregationContext aggregationContext, Aggregator parent,
                                       int zoom, int distance) {
        super(name, BucketAggregationMode.PER_BUCKET, factories, INITIAL_CAPACITY, aggregationContext, parent);
        this.valuesSource = valuesSource;
        bucketOrds = new LongHash(INITIAL_CAPACITY, aggregationContext.bigArrays());
        clusterCollectors = new LongObjectPagedHashMap<ClusterCollector>(INITIAL_CAPACITY, aggregationContext.bigArrays());
        this.zoom = zoom;
        this.distance = distance;
    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        geoValues = valuesSource.geoPointValues();
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;

        geoValues.setDocument(doc);
        final int valuesCount = geoValues.count();

        for (int i = 0; i<valuesCount; i++) {

            GeoPoint geoPoint = geoValues.valueAt(i);

            double meterByPixel = GeoClusterUtils.getMeterByPixel(zoom, geoPoint.getLat());
            int pointPrecision = GeoUtils.geoHashLevelsForPrecision(distance * meterByPixel);
            pointPrecision = pointPrecision > 1 ? pointPrecision - 1: pointPrecision ;
            final long clusterHashLong = GeoHashUtils.encodeAsLong(geoPoint.getLat(), geoPoint.getLon(), pointPrecision);
            ClusterCollector collec;
            long bucketOrdinal2 = bucketOrds.add(clusterHashLong);
            if (bucketOrdinal2 < 0) { // already seen
                bucketOrdinal2 = - 1 - bucketOrdinal2;
                collec = clusterCollectors.get(clusterHashLong);
                collec.addPoint(geoPoint);
                collectExistingBucket(doc, bucketOrdinal2);
            } else {
                collec = new ClusterCollector(geoPoint);
                collectBucket(doc, bucketOrdinal2);
            }

            clusterCollectors.put(clusterHashLong, collec);
        }
    }

    // private impl that stores a bucket ord. This allows for computing the aggregations lazily.
    static class OrdinalBucket extends InternalGeoHashClustering.Bucket {

        public OrdinalBucket() {
            super(0, null, 0, null);
        }

    }

    @Override
    public InternalGeoHashClustering buildAggregation(long owningBucketOrdinal) {
        assert owningBucketOrdinal == 0;


        List<InternalGeoHashClustering.Bucket> res = new ArrayList<InternalGeoHashClustering.Bucket>();
        for (long i = 0; i < bucketOrds.size(); i++) {
            long clusterHash = bucketOrds.get(i);

            ClusterCollector coll = clusterCollectors.get(clusterHash);
            InternalGeoHashClustering.Bucket bucket = new OrdinalBucket();

            bucket.docCount = bucketDocCount(i);
            bucket.aggregations = bucketAggregations(i);
            bucket.geohashAsLong = clusterHash;
            bucket.geohashesList.add(clusterHash);

            bucket.centroid = new GeoPoint((coll.getLatMin() + coll.getLatMax()) /2, (coll.getLonMin() + coll.getLonMax()) /2);

            res.add(bucket);
        }

        return new InternalGeoHashClustering(name, res, distance, zoom);
    }

    @Override
    public InternalGeoHashClustering buildEmptyAggregation() {
        return new InternalGeoHashClustering(name, Collections.<InternalGeoHashClustering.Bucket>emptyList(), distance, zoom);
    }


    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
        Releasables.close(clusterCollectors);
    }

}