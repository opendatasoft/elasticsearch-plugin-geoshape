package com.opendatasoft.elasticsearch.search.aggregations.bucket.geohashclustering;

import com.spatial4j.core.distance.DistanceUtils;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.*;

/**
 * Represents a grid of cells where each cell's location is determined by a geohash.
 * All geohashes in a grid are of the same precision and held internally as a single long
 * for efficiency's sake.
 */
public class InternalGeoHashClustering extends InternalAggregation implements GeoHashClustering {

    public static final Type TYPE = new Type("geohash_clustering", "ghclustering");

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalGeoHashClustering readResult(StreamInput in) throws IOException {
            InternalGeoHashClustering buckets = new InternalGeoHashClustering();
            buckets.readFrom(in);
            return buckets;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    static class Bucket implements GeoHashClustering.Bucket {

        // geohash of first cluster (for ID purpose)
        protected long geohashAsLong;
        // List of aggregated geohash
        protected Set<Long> geohashesList;
        protected long docCount;
        protected GeoPoint centroid;
        protected InternalAggregations aggregations;

        public Bucket(long geohashAsLong, GeoPoint centroid, long docCount, InternalAggregations aggregations) {
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.geohashAsLong = geohashAsLong;
            this.geohashesList = new HashSet<Long>();
            this.centroid = centroid;
        }

        @Override
        public GeoPoint getKeyAsGeoPoint() {
            return GeoHashUtils.decode(geohashAsLong);
        }

        @Override
        public Number getKeyAsNumber() {
            return geohashAsLong;
        }

        long getGeohashAsLong() {
            return geohashAsLong;
        }

        void setGeohashAsLong(long geohashAsLong) {
            this.geohashAsLong = geohashAsLong;
        }

        @Override
        public GeoPoint getClusterCenter() {
            return centroid;
        }

        @Override
        public String getKey() {
            return GeoHashUtils.toString(geohashAsLong);
        }

        @Override
        public Text getKeyAsText() {
            return new StringText(getKey());
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        public Bucket reduce(List<? extends Bucket> buckets, ReduceContext reduceContext) {
            List<InternalAggregations> aggregationsList = new ArrayList<InternalAggregations>(buckets.size());
            Bucket reduced = null;
            for (Bucket bucket : buckets) {
                if (reduced == null) {
                    reduced = bucket;
                } else {
                    reduced.docCount += bucket.docCount;
                }
                aggregationsList.add(bucket.aggregations);
            }
            reduced.aggregations = InternalAggregations.reduce(aggregationsList, reduceContext);
            return reduced;
        }

        public void merge(Bucket bucketToMergeIn, ReduceContext reduceContext) {

            long mergedDocCount = bucketToMergeIn.docCount + docCount;
            double newCentroidLat = (centroid.getLat() * docCount + bucketToMergeIn.centroid.getLat() * bucketToMergeIn.docCount) / mergedDocCount;
            double newCentroidLon = (centroid.getLon() * docCount + bucketToMergeIn.centroid.getLon() * bucketToMergeIn.docCount) / mergedDocCount;
            bucketToMergeIn.centroid = new GeoPoint(newCentroidLat, newCentroidLon);
            bucketToMergeIn.geohashesList.addAll(geohashesList);
            bucketToMergeIn.docCount = mergedDocCount;

            List<InternalAggregations> aggregationsList = new ArrayList<InternalAggregations>();
            aggregationsList.add(aggregations);
            aggregationsList.add(bucketToMergeIn.aggregations);

            bucketToMergeIn.aggregations = InternalAggregations.reduce(aggregationsList, reduceContext);


        }

    }

    private Collection<Bucket> buckets;
    protected Map<String, Bucket> bucketMap;
    private int distance;
    private int zoom;

    InternalGeoHashClustering() {
    } // for serialization

    public InternalGeoHashClustering(String name, Collection<Bucket> buckets, int distance, int zoom) {
        super(name);
        this.buckets = buckets;
        this.distance = distance;
        this.zoom = zoom;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public Collection<GeoHashClustering.Bucket> getBuckets() {
        Object o = buckets;
        return (Collection<GeoHashClustering.Bucket>) o;
    }

    @Override
    public GeoHashClustering.Bucket getBucketByKey(String geohash) {
        if (bucketMap == null) {
            bucketMap = new HashMap<String, Bucket>(buckets.size());
            for (Bucket bucket : buckets) {
                bucketMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketMap.get(geohash);
    }

    @Override
    public GeoHashClustering.Bucket getBucketByKey(Number key) {
        return getBucketByKey(GeoHashUtils.toString(key.longValue()));
    }

    @Override
    public GeoHashClustering.Bucket getBucketByKey(GeoPoint key) {
        return getBucketByKey(key.geohash());
    }

    @Override
    public InternalGeoHashClustering reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();

        LongObjectPagedHashMap<List<Bucket>> buckets = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalGeoHashClustering grid = (InternalGeoHashClustering) aggregation;
            if (buckets == null) {
                buckets = new LongObjectPagedHashMap<List<Bucket>>(grid.buckets.size(), reduceContext.bigArrays());
            }
            for (Bucket bucket : grid.buckets) {
                List<Bucket> existingBuckets = buckets.get(bucket.geohashAsLong);
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<Bucket>(aggregations.size());
                    buckets.put(bucket.geohashAsLong, existingBuckets);
                }
                existingBuckets.add(bucket);
            }
        }

        LongObjectPagedHashMap<Bucket> clusterMap = new LongObjectPagedHashMap<Bucket>(buckets.size(), reduceContext.bigArrays());

        List<Long> bucketList = new ArrayList<Long>();

        for (LongObjectPagedHashMap.Cursor<List<Bucket>> cursor : buckets) {
            List<Bucket> sameCellBuckets = cursor.value;
            Bucket bucket = sameCellBuckets.get(0).reduce(sameCellBuckets, reduceContext);
            clusterMap.put(sameCellBuckets.get(0).geohashAsLong, bucket);
            bucketList.add(sameCellBuckets.get(0).geohashAsLong);
        }

        Collections.sort(bucketList);

        Iterator<Long> iterBucket = bucketList.iterator();
        loop1:
        while(iterBucket.hasNext()) {
            Long bucketHash = iterBucket.next();

            Bucket bucket = clusterMap.get(bucketHash);

            Collection<? extends CharSequence> neighbors = GeoHashUtils.neighbors(bucket.getKey());

            Iterator<? extends CharSequence> iterator = neighbors.iterator();

            for (int i = 0; iterator.hasNext(); i++) {
                String neigh = iterator.next().toString();
                GeoPoint geoPointNeighbor = GeoHashUtils.decode(neigh);
                Bucket neighborBucket = clusterMap.get(GeoHashUtils.encodeAsLong(geoPointNeighbor.getLat(), geoPointNeighbor.getLon(), neigh.length()));
                if (neighborBucket == null) {
                    // We test parent neighbor
                    if (neigh.length() > 1) {
                        neighborBucket = clusterMap.get(GeoHashUtils.encodeAsLong(geoPointNeighbor.getLat(), geoPointNeighbor.getLon(), neigh.length() - 1));
                    }
                    if (neighborBucket == null) {
                        continue;
                    }
                }
                if (neighborBucket.geohashesList.contains(bucket.geohashAsLong)){
                    continue;
                }

                if (shouldCluster(bucket, neighborBucket)) {
                    bucket.merge(neighborBucket, reduceContext);
                    for (long superClusterHash: bucket.geohashesList) {
                        clusterMap.put(superClusterHash, neighborBucket);
                    }
                    iterBucket.remove();
                    continue loop1;
                }
            }

        }

        Set<Long> added = new HashSet<Long>();

        List<Bucket> res = new ArrayList<Bucket>();

        for (LongObjectPagedHashMap.Cursor<Bucket> cursor : clusterMap) {
            Bucket buck = cursor.value;
            if (added.contains(buck.geohashAsLong)) {
                continue;
            }
            res.add(buck);
            added.add(buck.geohashAsLong);
        }

        // Add sorting

        return new InternalGeoHashClustering(getName(), res, distance, zoom);
    }

    public boolean shouldCluster(Bucket bucket, Bucket bucket2) {
        double lat1 = bucket.getClusterCenter().getLat();
        double lon1 = bucket.getClusterCenter().getLon();
        double lat2 = bucket2.getClusterCenter().getLat();
        double lon2 = bucket2.getClusterCenter().getLon();

        double curDistance = GeoUtils.EARTH_MEAN_RADIUS * DistanceUtils.distHaversineRAD(DistanceUtils.toRadians(lat1), DistanceUtils.toRadians(lon1), DistanceUtils.toRadians(lat2), DistanceUtils.toRadians(lon2));

        double meterByPixel = GeoClusterUtils.getMeterByPixel(zoom, (lat1 + lat2) / 2 );

        return distance >= curDistance / meterByPixel;

    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        distance = in.readInt();
        zoom = in.readInt();
        this.name = in.readString();
        int size = in.readVInt();
        List<Bucket> buckets = new ArrayList<Bucket>(size);
        for (int i = 0; i < size; i++) {

            double centroidLat = in.readDouble();
            double centroidLon = in.readDouble();
            int nbGeohash = in.readInt();
            Set<Long> geohashList = new HashSet<Long>(nbGeohash);
            for (int j=0; j<nbGeohash;j++) {
                geohashList.add(in.readLong());
            }
            Bucket bucket = new Bucket(in.readLong(), new GeoPoint(centroidLat, centroidLon), in.readVLong(), InternalAggregations.readAggregations(in));

            bucket.geohashesList = geohashList;
            buckets.add(bucket);
        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(distance);
        out.writeInt(zoom);
        out.writeString(name);
        out.writeVInt(buckets.size());
        for (Bucket bucket : buckets) {
            out.writeDouble(bucket.getClusterCenter().getLat());
            out.writeDouble(bucket.getClusterCenter().getLon());

            out.writeInt(bucket.geohashesList.size());
            for (long geohash: bucket.geohashesList) {
                out.writeLong(geohash);
            }

            out.writeLong(bucket.geohashAsLong);
            out.writeVLong(bucket.getDocCount());
            ((InternalAggregations) bucket.getAggregations()).writeTo(out);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS);
        for (Bucket bucket : buckets) {
            builder.startObject();
            builder.field(CommonFields.KEY, bucket.getKeyAsText());
            builder.field(CommonFields.DOC_COUNT, bucket.getDocCount());
            Set<String> geohashGridsString = new HashSet<String>(bucket.geohashesList.size());
            for (long geohash: bucket.geohashesList) {
                geohashGridsString.add(GeoHashUtils.toString(geohash));
            }
            builder.array(new XContentBuilderString("geohash_grids"), geohashGridsString);
            builder.field(new XContentBuilderString("cluster_center"));
            ShapeBuilder.newPoint(bucket.getClusterCenter().getLon(), bucket.getClusterCenter().getLat()).toXContent(builder,params);
            ((InternalAggregations) bucket.getAggregations()).toXContentInternal(builder, params);
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

}
