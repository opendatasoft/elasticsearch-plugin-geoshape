package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.opendatasoft.elasticsearch.plugin.GeoUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GeoShapeAggregator extends BucketsAggregator {
    private final ValuesSource valuesSource;
    private final BytesRefHash bucketOrds;
    private final BucketCountThresholds bucketCountThresholds;
    private GeoUtils.OutputFormat output_format;
    private boolean must_simplify;
    private int zoom;
    private GeoShape.Algorithm algorithm;
    private final TileParams tile;
    private final GeoShapeTransform transform;

    private WKBReader wkbReader;
    private final WKBWriter wkbWriter;

    public GeoShapeAggregator(
        String name,
        AggregatorFactories factories,
        AggregationContext context,
        ValuesSource valuesSource,
        GeoUtils.OutputFormat output_format,
        boolean must_simplify,
        int zoom,
        GeoShape.Algorithm algorithm,
        TileParams tile,
        BucketCountThresholds bucketCountThresholds,
        Aggregator parent,
        CardinalityUpperBound cardinalityUpperBound,
        Map<String, Object> metaData
    ) throws IOException {
        super(name, factories, context, parent, cardinalityUpperBound, metaData);
        this.valuesSource = valuesSource;
        this.output_format = output_format;
        this.must_simplify = must_simplify;
        this.zoom = zoom;
        this.algorithm = algorithm;
        this.tile = tile;
        bucketOrds = new BytesRefHash(1, context.bigArrays());
        this.bucketCountThresholds = bucketCountThresholds;

        this.transform = new GeoShapeTransform(must_simplify, zoom, algorithm, tile);

        this.wkbReader = new WKBReader();
        this.wkbWriter = new WKBWriter();
    }

    /**
     * The collector collects the docs, including or not some score (depending of the including of a Scorer) in the
     * collect() process.
     *
     * The LeafBucketCollector is a "Per-leaf bucket collector". It collects docs for the account of buckets.
     */
    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final SortedBinaryDocValues values = valuesSource.bytesValues(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, values) {
            final BytesRefBuilder previous = new BytesRefBuilder();

            /**
             * Collect the given doc in the given bucket.
             * Called once for every document matching a query, with the unbased document number.
             */
            @Override
            public void collect(int doc, long owningBucketOrdinal) throws IOException {
                assert owningBucketOrdinal == 0;
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    previous.clear();

                    for (int i = 0; i < valuesCount; ++i) {
                        final BytesRef bytesValue = values.nextValue();
                        if (previous.get().equals(bytesValue)) {
                            continue;
                        }
                        long bucketOrdinal = bucketOrds.add(bytesValue);
                        if (bucketOrdinal < 0) { // already seen
                            bucketOrdinal = -1 - bucketOrdinal;
                            collectExistingBucket(sub, doc, bucketOrdinal);
                        } else {
                            collectBucket(sub, doc, bucketOrdinal);
                        }
                        previous.copyBytes(bytesValue);
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(LongArray owningBucketOrdinals) throws IOException {
        // TODO: replace by calling buildAggregationsForVariableBuckets or buildAggregationsForFixedBucketCount??
        try (ObjectArray<InternalGeoShape.InternalBucket[]> topBucketsPerOrd = bigArrays().newObjectArray(owningBucketOrdinals.size())) {
            // InternalGeoShape[] results = new InternalGeoShape[owningBucketOrdinals.size()];
            InternalGeoShape[] results = new InternalGeoShape[Math.toIntExact(owningBucketOrdinals.size())];

            for (long ordIdx = 0; ordIdx < owningBucketOrdinals.size(); ordIdx++) {
                assert owningBucketOrdinals.get(ordIdx) == 0;

                final int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());
                // We will insert buckets in a priority queue with a capacity of up to N=size elements
                InternalGeoShape.BucketPriorityQueue ordered = new InternalGeoShape.BucketPriorityQueue(size);

                InternalGeoShape.InternalBucket spare = null;
                // Total doc count of every distinct shape collected on this shard, before `shard_size` truncation.
                long totalDocCount = 0;
                for (int i = 0; i < bucketOrds.size(); i++) {
                    totalDocCount += bucketDocCount(i);
                    if (spare == null) {
                        spare = new InternalGeoShape.InternalBucket(new BytesRef(), null, null, 0, 0, null);
                    }
                    bucketOrds.get(i, spare.wkb);

                    // FIXME: why do we need a deepCopy here ?
                    spare.wkb = BytesRef.deepCopyOf(spare.wkb);
                    spare.wkbHash = String.valueOf(GeoUtils.getHashFromWKB(spare.wkb));

                    if (GeoUtils.wkbIsPoint(spare.wkb.bytes)) {
                        spare.perimeter = 0;
                        spare.realType = "Point";
                    } else {
                        Geometry geom;

                        try {
                            geom = wkbReader.read(spare.wkb.bytes);
                        } catch (ParseException e) {
                            continue;
                        }

                        if (transform.intersectsWindow(geom) == false) {
                            // The clip would empty this shape, so it must not take a slot in the
                            // ranking: doing so would hide a shape that is actually visible in the
                            // window. Free to check, the geometry is already parsed here. Its docs
                            // still count towards sum_other_doc_count, exactly as they did when the
                            // shape was ranked and then dropped further down.
                            continue;
                        }

                        spare.perimeter = geom.getLength();
                        spare.realType = geom.getGeometryType();

                    }

                    spare.docCount = bucketDocCount(i);
                    spare.bucketOrd = i;
                    spare = ordered.insertWithOverflow(spare);
                }

                // Pop the queue first: it hands buckets back smallest-perimeter first, and the result
                // has to stay ordered largest first.
                final InternalGeoShape.InternalBucket[] popped = new InternalGeoShape.InternalBucket[ordered.size()];
                for (int i = popped.length - 1; i >= 0; --i) {
                    popped[i] = ordered.pop();
                }

                // A shape can drop out here (unreadable or unprocessable WKB, or nothing of it left
                // inside the tile window). Collect the survivors rather than leaving holes in the
                // array: every bucket is later dereferenced by buildSubAggsForAllBuckets.
                final List<InternalGeoShape.InternalBucket> keptBuckets = new ArrayList<>(popped.length);
                for (InternalGeoShape.InternalBucket bucket : popped) {
                    if (transform.isNoop()) {
                        // Nothing to compute, so do not pay for parsing the WKB back.
                        keptBuckets.add(bucket);
                        continue;
                    }

                    try {
                        Geometry geom = transform.apply(wkbReader.read(bucket.wkb.bytes));
                        if (geom.isEmpty()) {
                            // Nothing of this shape falls inside the window. Dropping it is safe: the
                            // neighbouring tiles that do cover it still return it, and its doc count is
                            // reported through sum_other_doc_count below.
                            continue;
                        }
                        bucket.wkb = new BytesRef(wkbWriter.write(geom));
                        if (transform.rescalesGeometry() == false) {
                            // perimeter is the ranking key, and the coordinator re-ranks buckets with it
                            // across shards. Once a shape is clipped or rescaled its length no longer says
                            // anything about the shape itself, only about how much of it this window shows,
                            // so keep the whole-shape WGS84 length assigned above instead.
                            bucket.perimeter = geom.getLength();
                        }
                        keptBuckets.add(bucket);
                    } catch (ParseException | IllegalArgumentException | TopologyException e) {
                        // One malformed shape must not fail the shard, and with it the whole search.
                        // Clipping runs an overlay on raw stored geometry, and JTS throws
                        // TopologyException (or IllegalArgumentException on a malformed ring) when it
                        // cannot process one. Drop the bucket like an unreadable one; its docs are
                        // reported through sum_other_doc_count.
                    }
                }
                topBucketsPerOrd.set(ordIdx, keptBuckets.toArray(new InternalGeoShape.InternalBucket[0]));

                // Docs carried by the shapes this shard actually returns; the rest is reported as "other".
                long returnedDocCount = 0;
                for (InternalGeoShape.InternalBucket bucket : keptBuckets) {
                    returnedDocCount += bucket.docCount;
                }

                results[Math.toIntExact(ordIdx)] = new InternalGeoShape(
                    name,
                    Arrays.asList(topBucketsPerOrd.get(ordIdx)),
                    output_format,
                    bucketCountThresholds.getRequiredSize(),
                    bucketCountThresholds.getShardSize(),
                    totalDocCount - returnedDocCount,
                    metadata()
                );
            }

            // Build sub-aggregations
            buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, aggregations) -> b.subAggregations = aggregations);
            return results;
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalGeoShape(
            name,
            null,
            output_format,
            bucketCountThresholds.getRequiredSize(),
            bucketCountThresholds.getShardSize(),
            0,
            metadata()
        );
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrds);
    }

    public static class BucketCountThresholds implements Writeable, ToXContentFragment {
        private int requiredSize;
        private int shardSize;

        public BucketCountThresholds(int requiredSize, int shardSize) {
            this.requiredSize = requiredSize;
            this.shardSize = shardSize;
        }

        /**
         * Read from a stream.
         */
        public BucketCountThresholds(StreamInput in) throws IOException {
            requiredSize = in.readInt();
            shardSize = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(requiredSize);
            out.writeInt(shardSize);
        }

        public BucketCountThresholds(GeoShapeAggregator.BucketCountThresholds bucketCountThresholds) {
            this(bucketCountThresholds.requiredSize, bucketCountThresholds.shardSize);
        }

        public void ensureValidity() {
            // shard_size cannot be smaller than size as we need to at least fetch size entries from every shards in order to return size
            if (shardSize < requiredSize) {
                setShardSize(requiredSize);
            }

            if (requiredSize <= 0 || shardSize <= 0) {
                throw new ElasticsearchException("parameters [required_size] and [shard_size] must be >0 in geoshape aggregation.");
            }
        }

        public int getRequiredSize() {
            return requiredSize;
        }

        public void setRequiredSize(int requiredSize) {
            this.requiredSize = requiredSize;
        }

        public int getShardSize() {
            return shardSize;
        }

        public void setShardSize(int shardSize) {
            this.shardSize = shardSize;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(GeoShapeBuilder.SIZE_FIELD.getPreferredName(), requiredSize);
            if (shardSize != -1) {
                builder.field(GeoShapeBuilder.SHARD_SIZE_FIELD.getPreferredName(), shardSize);
            }
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(requiredSize, shardSize);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            GeoShapeAggregator.BucketCountThresholds other = (GeoShapeAggregator.BucketCountThresholds) obj;
            return Objects.equals(requiredSize, other.requiredSize) && Objects.equals(shardSize, other.shardSize);
        }
    }

}
