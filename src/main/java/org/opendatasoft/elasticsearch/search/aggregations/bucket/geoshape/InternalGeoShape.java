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
        /**
         * Values read from the documents of this bucket, as {@code [field][document][value]}, or
         * {@code null} when the aggregation was not asked for any.
         *
         * <p>Grouped by document rather than flattened so that the coordinator can cut the merge of
         * several shards on a document boundary, which is what {@code max_docs_per_bucket} bounds.
         * Every field holds the same number of document groups, empty ones included.
         */
        protected BytesRef[][][] collectedValues;
        /** Whether the bucket holds more documents than the ones {@link #collectedValues} came from. */
        protected boolean collectedDocsTruncated;

        public InternalBucket(
            BytesRef wkb,
            String wkbHash,
            String realType,
            double perimeter,
            long docCount,
            InternalAggregations subAggregations,
            BytesRef[][][] collectedValues,
            boolean collectedDocsTruncated
        ) {
            this.wkb = wkb;
            this.wkbHash = wkbHash;
            this.realType = realType;
            this.docCount = docCount;
            this.subAggregations = subAggregations;
            this.perimeter = perimeter;
            this.collectedValues = collectedValues;
            this.collectedDocsTruncated = collectedDocsTruncated;
        }

        /**
         * Read from a stream.
         *
         * <p>{@code collectFieldCount} is not part of the bucket's own payload: it is a property of
         * the request, carried once by the enclosing {@link InternalGeoShape}, and the writer uses
         * that very same number.
         */
        public InternalBucket(StreamInput in, int collectFieldCount) throws IOException {
            wkb = in.readBytesRef();
            wkbHash = in.readString();
            realType = in.readString();
            perimeter = in.readDouble();
            docCount = in.readLong();
            subAggregations = InternalAggregations.readFrom(in);
            if (collectFieldCount > 0) {
                int documents = in.readVInt();
                collectedValues = new BytesRef[collectFieldCount][][];
                for (int field = 0; field < collectFieldCount; field++) {
                    BytesRef[][] perDocument = new BytesRef[documents][];
                    for (int document = 0; document < documents; document++) {
                        BytesRef[] values = new BytesRef[in.readVInt()];
                        for (int value = 0; value < values.length; value++) {
                            values[value] = in.readBytesRef();
                        }
                        perDocument[document] = values;
                    }
                    collectedValues[field] = perDocument;
                }
                collectedDocsTruncated = in.readBoolean();
            }
        }

        /**
         * Write to a stream.
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            writeTo(out, collectedValues == null ? 0 : collectedValues.length);
        }

        /**
         * Write to a stream, emitting the payload of exactly {@code collectFieldCount} fields.
         */
        public void writeTo(StreamOutput out, int collectFieldCount) throws IOException {
            out.writeBytesRef(wkb);
            out.writeString(wkbHash);
            out.writeString(realType);
            out.writeDouble(perimeter);
            out.writeLong(docCount);
            subAggregations.writeTo(out);
            if (collectFieldCount > 0) {
                out.writeVInt(getCollectedDocCount());
                for (int field = 0; field < collectFieldCount; field++) {
                    for (BytesRef[] values : collectedValuesOf(field)) {
                        out.writeVInt(values.length);
                        for (BytesRef value : values) {
                            out.writeBytesRef(value);
                        }
                    }
                }
                out.writeBoolean(collectedDocsTruncated);
            }
        }

        /** How many documents {@link #collectedValues} was read from. Identical for every field. */
        int getCollectedDocCount() {
            return collectedValues == null || collectedValues.length == 0 ? 0 : collectedValues[0].length;
        }

        private static final BytesRef[][] NO_COLLECTED_VALUES = new BytesRef[0][];

        BytesRef[][] collectedValuesOf(int field) {
            return collectedValues == null ? NO_COLLECTED_VALUES : collectedValues[field];
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

        /**
         * Key the coordinator merges buckets on: the 64-bit hash of the shape <b>as stored</b>, not of
         * {@link #wkb}, in which two distinct shapes that simplified or quantized alike would collide.
         */
        private long getShapeHash() {
            return Long.parseLong(wkbHash);
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
        final int collectFieldCount = getCollectFieldCount();
        this.buckets = in.readCollectionAsList(streamInput -> new InternalBucket(streamInput, collectFieldCount));
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
        final int collectFieldCount = getCollectFieldCount();
        out.writeCollection(buckets, (streamOutput, bucket) -> bucket.writeTo(streamOutput, collectFieldCount));
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
            prototype.wkbHash,
            prototype.realType,
            prototype.perimeter,
            prototype.docCount,
            aggregations,
            prototype.collectedValues,
            prototype.collectedDocsTruncated
        );
    }

    /** Number of fields whose values every bucket carries; {@code 0} when the param is absent. */
    private int getCollectFieldCount() {
        return collectFields == null ? 0 : collectFields.getFields().size();
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
                    collectFields,
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
        if (collectFields != null && buckets.size() > 1) {
            // A single contribution was already bounded by the shard that produced it, values and
            // truncation flag alike, so there is nothing to redo for what is by far the common case:
            // most shapes live on one shard.
            mergeCollectedValues(reduced, buckets, getCollectFieldCount(), collectFields.getMaxDocsPerBucket());
        }
        return reduced;
    }

    /**
     * Concatenate the collected values of the buckets that describe the same shape, keeping the
     * documents of the first shards to answer and re-applying {@code max_docs_per_bucket}.
     *
     * <p>Documents are cut, not values: a document either contributes all of its values or none of
     * them, which is why the values are carried grouped by document.
     */
    static void mergeCollectedValues(InternalBucket reduced, List<InternalBucket> buckets, int fieldCount, int maxDocsPerBucket) {
        int availableDocs = 0;
        boolean truncated = false;
        for (InternalBucket bucket : buckets) {
            availableDocs += bucket.getCollectedDocCount();
            truncated |= bucket.collectedDocsTruncated;
        }
        final int keptDocs = Math.min(availableDocs, maxDocsPerBucket);

        BytesRef[][][] merged = new BytesRef[fieldCount][][];
        for (int field = 0; field < fieldCount; field++) {
            BytesRef[][] perDocument = new BytesRef[keptDocs][];
            int document = 0;
            for (InternalBucket bucket : buckets) {
                BytesRef[][] contribution = bucket.collectedValuesOf(field);
                for (int i = 0; i < contribution.length && document < keptDocs; i++) {
                    perDocument[document++] = contribution[i];
                }
            }
            merged[field] = perDocument;
        }

        reduced.collectedValues = merged;
        reduced.collectedDocsTruncated = truncated || availableDocs > keptDocs;
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
            collectedFieldsToXContent(builder, bucket);
            bucket.getAggregations().toXContentInternal(builder, params);
            builder.endObject();
        }
        builder.endArray();
        return builder;
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
            for (BytesRef[] values : bucket.collectedValuesOf(field)) {
                for (BytesRef value : values) {
                    builder.value(value.utf8ToString());
                }
            }
            builder.endArray();
        }
        builder.endObject();
        builder.field("collected_docs_truncated", bucket.collectedDocsTruncated);
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
