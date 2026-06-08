package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.geojson.GeoJsonWriter;
import org.opendatasoft.elasticsearch.plugin.GeoUtils;
import org.opendatasoft.elasticsearch.plugin.GeoUtils.OutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An internal implementation of {@link InternalMultiBucketAggregation} which extends {@link Aggregation}.
 */
public class InternalGeoShape extends InternalMultiBucketAggregation<InternalGeoShape, InternalGeoShape.InternalBucket>
    implements
        GeoShape {

    /**
     * The bucket class of InternalGeoShape.
     * @see MultiBucketsAggregation.Bucket
     */
    public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucket
        implements
            Writeable,
            ToXContentFragment,
            GeoShape.Bucket,
            KeyComparable<InternalBucket> {

        protected BytesRef wkb;
        protected String wkbHash;
        protected String realType;
        protected double perimeter;
        long bucketOrd;
        protected long docCount;
        protected InternalAggregations subAggregations;

        public InternalBucket(
            BytesRef wkb,
            String wkbHash,
            String realType,
            double perimeter,
            long docCount,
            InternalAggregations subAggregations
        ) {
            this.wkb = wkb;
            this.wkbHash = wkbHash;
            this.realType = realType;
            this.docCount = docCount;
            this.subAggregations = subAggregations;
            this.perimeter = perimeter;
        }

        /**
         * Read from a stream.
         */
        public InternalBucket(StreamInput in) throws IOException {
            wkb = in.readBytesRef();
            wkbHash = in.readString();
            realType = in.readString();
            perimeter = in.readDouble();
            docCount = in.readLong();
            subAggregations = InternalAggregations.readFrom(in);
        }

        /**
         * Write to a stream.
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesRef(wkb);
            out.writeString(wkbHash);
            out.writeString(realType);
            out.writeDouble(perimeter);
            out.writeLong(docCount);
            subAggregations.writeTo(out);
        }

        @Override
        public String getKey() {
            return wkb.toString();
        }

        @Override
        public String getKeyAsString() {
            return wkb.utf8ToString();
        }

        @Override
        public int compareKey(InternalGeoShape.InternalBucket other) {
            return wkb.compareTo(other.wkb);
        }

        private long getShapeHash() {
            return wkb.hashCode();
        }

        private String getType() {
            return realType;
        }

        private int compareTo(InternalBucket other) {
            if (this.docCount > other.docCount) {
                return 1;
            } else if (this.docCount < other.docCount) {
                return -1;
            } else return 0;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public InternalAggregations getAggregations() {
            return subAggregations;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            subAggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

    }

    private List<InternalBucket> buckets;
    private final int requiredSize;
    private final int shardSize;
    // Total doc count of the shapes that are NOT returned (dropped by `size` at the coordinator or by
    // `shard_size` at the shard). Exact: each shard knows precisely how many docs it dropped.
    private final long otherDocCount;
    private OutputFormat output_format;
    private GeoJsonWriter geoJsonWriter;

    public InternalGeoShape(
        String name,
        List<InternalBucket> buckets,
        OutputFormat output_format,
        int requiredSize,
        int shardSize,
        long otherDocCount,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.buckets = buckets;
        this.output_format = output_format;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.otherDocCount = otherDocCount;
        geoJsonWriter = new GeoJsonWriter();
    }

    /**
     * Read from a stream.
     */
    public InternalGeoShape(StreamInput in) throws IOException {
        super(in);
        output_format = OutputFormat.valueOf(in.readString());
        requiredSize = readSize(in);
        shardSize = readSize(in);
        otherDocCount = in.readVLong();
        this.buckets = in.readCollectionAsList(InternalBucket::new);
    }

    /**
     * Write to a stream.
     */
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(output_format.name());
        writeSize(requiredSize, out);
        writeSize(shardSize, out);
        out.writeVLong(otherDocCount);
        out.writeCollection(buckets);
    }

    @Override
    public String getWriteableName() {
        return GeoShapeBuilder.NAME;
    }

    @Override
    public InternalGeoShape create(List<InternalBucket> buckets) {
        return new InternalGeoShape(this.name, buckets, output_format, requiredSize, shardSize, otherDocCount, this.metadata);
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return new InternalBucket(
            prototype.wkb,
            prototype.wkbHash,
            prototype.realType,
            prototype.perimeter,
            prototype.docCount,
            aggregations
        );
    }

    @Override
    public List<InternalBucket> getBuckets() {
        return buckets;
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            private LongObjectPagedHashMap<List<InternalBucket>> buckets = new LongObjectPagedHashMap<>(size, reduceContext.bigArrays());
            // Carry the doc count of shapes already dropped upstream (per-shard `shard_size`, earlier partial reduces).
            private long otherDocCountSum = 0;

            @Override
            public void accept(InternalAggregation aggregation) {
                InternalGeoShape shape = (InternalGeoShape) aggregation;
                otherDocCountSum += shape.otherDocCount;

                if (buckets == null) {
                    buckets = new LongObjectPagedHashMap<>(shape.buckets.size(), reduceContext.bigArrays());
                }

                for (InternalBucket bucket : shape.buckets) {
                    List<InternalBucket> existingBuckets = buckets.get(bucket.getShapeHash());
                    if (existingBuckets == null) {
                        existingBuckets = new ArrayList<>();
                        buckets.put(bucket.getShapeHash(), existingBuckets);
                    }
                    existingBuckets.add(bucket);
                }
            }

            @Override
            public InternalAggregation get() {
                final boolean isFinalReduce = reduceContext.isFinalReduce();
                final long distinctShapes = buckets.size();
                final int size = !isFinalReduce ? (int) distinctShapes : Math.min(requiredSize, (int) distinctShapes);

                BucketPriorityQueue ordered = new BucketPriorityQueue(size);
                long totalDocCount = 0;
                for (LongObjectPagedHashMap.Cursor<List<InternalBucket>> cursor : buckets) {
                    List<InternalBucket> sameCellBuckets = cursor.value;
                    InternalBucket reducedBucket = reduceBucket(sameCellBuckets, reduceContext);
                    totalDocCount += reducedBucket.docCount;
                    ordered.insertWithOverflow(reducedBucket);
                }
                buckets.close();
                InternalBucket[] list = new InternalBucket[ordered.size()];
                long returnedDocCount = 0;
                for (int i = ordered.size() - 1; i >= 0; i--) {
                    list[i] = ordered.pop();
                    returnedDocCount += list[i].docCount;
                }

                // Docs hidden = those dropped upstream + those merged here but dropped by `size`.
                // At a non-final reduce nothing is dropped by `size` (the queue keeps every shape), so this
                // just carries otherDocCountSum forward.
                long reducedOtherDocCount = otherDocCountSum + (totalDocCount - returnedDocCount);

                return new InternalGeoShape(
                    getName(),
                    Arrays.asList(list),
                    output_format,
                    requiredSize,
                    shardSize,
                    reducedOtherDocCount,
                    getMetadata()
                );
            }
        };
    }

    public InternalBucket reduceBucket(List<InternalBucket> buckets, AggregationReduceContext context) {
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        InternalBucket reduced = null;
        for (InternalBucket bucket : buckets) {
            if (reduced == null) {
                reduced = bucket;
            } else {
                reduced.docCount += bucket.docCount;
            }
            aggregationsList.add(bucket.subAggregations);
        }
        reduced.subAggregations = InternalAggregations.reduce(aggregationsList, context);
        return reduced;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field("sum_other_doc_count", otherDocCount);
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (InternalBucket bucket : buckets) {
            builder.startObject();
            try {
                builder.field(CommonFields.KEY.getPreferredName(), GeoUtils.exportWkbTo(bucket.wkb, output_format, geoJsonWriter));
                builder.field("digest", bucket.wkbHash);
                builder.field("type", bucket.getType());
            } catch (ParseException e) {
                continue;
            }
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), bucket.getDocCount());
            bucket.getAggregations().toXContentInternal(builder, params);
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), buckets, output_format, requiredSize, shardSize, otherDocCount);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (!super.equals(obj)) return false;

        InternalGeoShape that = (InternalGeoShape) obj;
        return Objects.equals(buckets, that.buckets)
            && Objects.equals(output_format, that.output_format)
            && Objects.equals(requiredSize, that.requiredSize)
            && Objects.equals(shardSize, that.shardSize)
            && Objects.equals(otherDocCount, that.otherDocCount);
    }

    // The priority queue is used to retain the top N buckets (i.e. shapes)
    // Buckets are here ordered by area (!) then by hash
    static class BucketPriorityQueue extends PriorityQueue<InternalBucket> {

        BucketPriorityQueue(int size) {
            super(size);
        }

        @Override
        protected boolean lessThan(InternalBucket o1, InternalBucket o2) {

            double i = o2.perimeter - o1.perimeter;
            if (i == 0) {
                i = o2.compareTo(o1);
                if (i == 0) {
                    i = System.identityHashCode(o2) - System.identityHashCode(o1);
                }
            }
            return i > 0;
        }
    }
}
