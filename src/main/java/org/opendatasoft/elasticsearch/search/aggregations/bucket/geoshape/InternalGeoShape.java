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
        /**
         * Key the coordinator merges buckets on: the 64-bit hash of the shape <b>as stored</b>, not of
         * {@link #wkb}, in which two distinct shapes that simplified or quantized alike would collide.
         */
        protected long shapeHash;
        protected String realType;
        protected double perimeter;
        long bucketOrd;
        protected long docCount;
        protected InternalAggregations subAggregations;
        /** What {@code collect_fields} read from this bucket's documents, or {@code null} when it was not asked for. */
        protected CollectedValues collected;
        /**
         * The MVT command stream drawing the returned geometry, or {@code null} unless
         * {@code output_format} is {@code mvt}. When it is set, {@link #wkb} is left empty: the stream
         * replaces the serialized geometry.
         */
        protected int[] mvt;

        public InternalBucket(
            BytesRef wkb,
            int[] mvt,
            long shapeHash,
            String realType,
            double perimeter,
            long docCount,
            InternalAggregations subAggregations,
            CollectedValues collected
        ) {
            this.wkb = wkb;
            this.mvt = mvt;
            this.shapeHash = shapeHash;
            this.realType = realType;
            this.docCount = docCount;
            this.subAggregations = subAggregations;
            this.perimeter = perimeter;
            this.collected = collected;
        }

        /**
         * Read from a stream.
         */
        public InternalBucket(StreamInput in) throws IOException {
            wkb = in.readBytesRef();
            mvt = in.readBoolean() ? in.readVIntArray() : null;
            shapeHash = in.readLong();
            realType = in.readString();
            perimeter = in.readDouble();
            docCount = in.readLong();
            subAggregations = InternalAggregations.readFrom(in);
            collected = in.readOptionalWriteable(CollectedValues::new);
        }

        /**
         * Write to a stream.
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesRef(wkb);
            out.writeBoolean(mvt != null);
            if (mvt != null) {
                // Variable-width: the stream is mostly small zigzag deltas, which a vint fits in one byte.
                out.writeVIntArray(mvt);
            }
            out.writeLong(shapeHash);
            out.writeString(realType);
            out.writeDouble(perimeter);
            out.writeLong(docCount);
            subAggregations.writeTo(out);
            out.writeOptionalWriteable(collected);
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

        private String getType() {
            return realType;
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
    // Optional: when null, buckets carry no doc-values payload. Held here rather than on each bucket
    // so the field names and the bound travel once per shard response, not once per shape.
    private final CollectFieldsParams collectFields;
    private GeoJsonWriter geoJsonWriter;

    public InternalGeoShape(
        String name,
        List<InternalBucket> buckets,
        OutputFormat output_format,
        CollectFieldsParams collectFields,
        int requiredSize,
        int shardSize,
        long otherDocCount,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.buckets = buckets;
        this.output_format = output_format;
        this.collectFields = collectFields;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.otherDocCount = otherDocCount;
        geoJsonWriter = GeoUtils.createGeoJsonWriter();
    }

    /**
     * Read from a stream.
     */
    public InternalGeoShape(StreamInput in) throws IOException {
        super(in);
        output_format = OutputFormat.valueOf(in.readString());
        collectFields = in.readOptionalWriteable(CollectFieldsParams::new);
        requiredSize = readSize(in);
        shardSize = readSize(in);
        otherDocCount = in.readVLong();
        this.buckets = in.readCollectionAsList(InternalBucket::new);
        // Also needed here: doXContentBody uses it, and a deserialized instance can reach it.
        geoJsonWriter = GeoUtils.createGeoJsonWriter();
    }

    /**
     * Write to a stream.
     */
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(output_format.name());
        out.writeOptionalWriteable(collectFields);
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
        return new InternalGeoShape(
            this.name,
            buckets,
            output_format,
            collectFields,
            requiredSize,
            shardSize,
            otherDocCount,
            this.metadata
        );
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return new InternalBucket(
            prototype.wkb,
            prototype.mvt,
            prototype.shapeHash,
            prototype.realType,
            prototype.perimeter,
            prototype.docCount,
            aggregations,
            prototype.collected
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
                    List<InternalBucket> existingBuckets = buckets.get(bucket.shapeHash);
                    if (existingBuckets == null) {
                        existingBuckets = new ArrayList<>();
                        buckets.put(bucket.shapeHash, existingBuckets);
                    }
                    existingBuckets.add(bucket);
                }
            }

            @Override
            public InternalAggregation get() {
                final boolean isFinalReduce = reduceContext.isFinalReduce();
                final long distinctShapes = buckets.size();
                final int size = !isFinalReduce ? (int) distinctShapes : Math.min(requiredSize, (int) distinctShapes);

                // Rank first, reduce after: every copy of a shape carries the same perimeter, and only the doc
                // count has to be summed to rank it, so the shapes `size` drops never have their
                // sub-aggregations or collected values reduced.
                PriorityQueue<List<InternalBucket>> ordered = new PriorityQueue<>(size) {
                    @Override
                    protected boolean lessThan(List<InternalBucket> a, List<InternalBucket> b) {
                        return BucketPriorityQueue.ranksLower(a.get(0), b.get(0));
                    }
                };
                long totalDocCount = 0;
                for (LongObjectPagedHashMap.Cursor<List<InternalBucket>> cursor : buckets) {
                    List<InternalBucket> sameShapeBuckets = cursor.value;
                    InternalBucket first = sameShapeBuckets.get(0);
                    for (int i = 1; i < sameShapeBuckets.size(); i++) {
                        first.docCount += sameShapeBuckets.get(i).docCount;
                    }
                    totalDocCount += first.docCount;
                    ordered.insertWithOverflow(sameShapeBuckets);
                }
                buckets.close();
                InternalBucket[] list = new InternalBucket[ordered.size()];
                long returnedDocCount = 0;
                for (int i = ordered.size() - 1; i >= 0; i--) {
                    list[i] = reduceBucket(ordered.pop(), reduceContext);
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
                    collectFields,
                    requiredSize,
                    shardSize,
                    reducedOtherDocCount,
                    getMetadata()
                );
            }
        };
    }

    /** Merge the copies of one shape into the first, whose doc count the ranking has already summed. */
    private InternalBucket reduceBucket(List<InternalBucket> buckets, AggregationReduceContext context) {
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        InternalBucket reduced = buckets.get(0);
        for (InternalBucket bucket : buckets) {
            aggregationsList.add(bucket.subAggregations);
        }
        reduced.subAggregations = InternalAggregations.reduce(aggregationsList, context);
        if (collectFields != null && buckets.size() > 1) {
            // A single contribution was already bounded by its shard: the common case has nothing to redo.
            List<CollectedValues> contributions = new ArrayList<>(buckets.size());
            for (InternalBucket bucket : buckets) {
                contributions.add(bucket.collected);
            }
            reduced.collected = CollectedValues.merge(contributions, collectFields.getMaxDocsPerBucket());
        }
        return reduced;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field("sum_other_doc_count", otherDocCount);
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (InternalBucket bucket : buckets) {
            builder.startObject();
            try {
                keyToXContent(builder, bucket);
                builder.field("digest", String.valueOf(bucket.shapeHash));
                builder.field("type", bucket.getType());
            } catch (ParseException e) {
                continue;
            }
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), bucket.getDocCount());
            collectedFieldsToXContent(builder, bucket);
            bucket.getAggregations().toXContentInternal(builder, params);
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    /** Render the bucket's geometry under {@code key}: a string, or an array of integers under {@code mvt}. */
    private void keyToXContent(XContentBuilder builder, InternalBucket bucket) throws IOException, ParseException {
        if (output_format == OutputFormat.MVT) {
            builder.array(CommonFields.KEY.getPreferredName(), bucket.mvt);
            return;
        }
        builder.field(CommonFields.KEY.getPreferredName(), GeoUtils.exportWkbTo(bucket.wkb, output_format, geoJsonWriter));
    }

    /**
     * Render the collected values, flattened: the per-document grouping only exists so the merge can
     * cut on a document boundary, and a caller asking for the values of a bucket has no use for it.
     */
    private void collectedFieldsToXContent(XContentBuilder builder, InternalBucket bucket) throws IOException {
        if (collectFields == null) {
            return;
        }
        builder.startObject("collected_fields");
        List<String> fields = collectFields.getFields();
        for (int field = 0; field < fields.size(); field++) {
            builder.startArray(fields.get(field));
            for (BytesRef[] values : bucket.collected.valuesOf(field)) {
                for (BytesRef value : values) {
                    builder.value(value.utf8ToString());
                }
            }
            builder.endArray();
        }
        builder.endObject();
        builder.field("collected_docs_truncated", bucket.collected.truncated());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), buckets, output_format, collectFields, requiredSize, shardSize, otherDocCount);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (!super.equals(obj)) return false;

        InternalGeoShape that = (InternalGeoShape) obj;
        return Objects.equals(buckets, that.buckets)
            && Objects.equals(output_format, that.output_format)
            && Objects.equals(collectFields, that.collectFields)
            && Objects.equals(requiredSize, that.requiredSize)
            && Objects.equals(shardSize, that.shardSize)
            && Objects.equals(otherDocCount, that.otherDocCount);
    }

    // The priority queue is used to retain the top N buckets (i.e. shapes)
    // Buckets are ordered by perimeter, longest first, then by doc count, then by shape hash, so that ties
    // (every point has a perimeter of 0) resolve the same way on every request.
    static class BucketPriorityQueue extends PriorityQueue<InternalBucket> {

        BucketPriorityQueue(int size) {
            super(size);
        }

        @Override
        protected boolean lessThan(InternalBucket o1, InternalBucket o2) {
            return ranksLower(o1, o2);
        }

        static boolean ranksLower(InternalBucket o1, InternalBucket o2) {
            int c = Double.compare(o1.perimeter, o2.perimeter);
            if (c == 0) {
                c = Long.compare(o1.docCount, o2.docCount);
            }
            if (c == 0) {
                c = Long.compare(o1.shapeHash, o2.shapeHash);
            }
            return c < 0;
        }
    }
}
