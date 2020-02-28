package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;
import org.opendatasoft.elasticsearch.plugin.GeoUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public class GeoShapeAggregator extends BucketsAggregator {
    private final ValuesSource valuesSource;
    private final BytesRefHash bucketOrds;
    private final BucketCountThresholds bucketCountThresholds;
    private InternalGeoShape.OutputFormat output_format;
    private boolean must_simplify;
    private int zoom;
    private GeoShape.Algorithm algorithm;

    private int pixelTolerance;
    private WKBReader wkbReader;
    private final GeometryFactory geometryFactory;


    public GeoShapeAggregator(
            String name,
            AggregatorFactories factories,
            SearchContext context,
            ValuesSource valuesSource,
            InternalGeoShape.OutputFormat output_format,
            boolean must_simplify,
            int zoom,
            GeoShape.Algorithm algorithm,
            BucketCountThresholds bucketCountThresholds,
            Aggregator parent,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData
    ) throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.output_format = output_format;
        this.must_simplify = must_simplify;
        this.zoom = zoom;
        this.algorithm = algorithm;
        bucketOrds = new BytesRefHash(1, context.bigArrays());
        this.bucketCountThresholds = bucketCountThresholds;

        pixelTolerance = 1;
        this.wkbReader = new WKBReader();
        this.geometryFactory = new GeometryFactory();
    }

    /**
     * The collector collects the docs, including or not some score (depending of the including of a Scorer) in the
     * collect() process.
     *
     * The LeafBucketCollector is a "Per-leaf bucket collector". It collects docs for the account of buckets.
     */
    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
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
                            bucketOrdinal = - 1 - bucketOrdinal;
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
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;

        final int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());
        InternalGeoShape.BucketPriorityQueue ordered = new InternalGeoShape.BucketPriorityQueue(size);

        InternalGeoShape.InternalBucket spare = null;
        for (int i = 0; i < bucketOrds.size(); i++) {
            if (spare == null) {
                spare = new InternalGeoShape.InternalBucket(new BytesRef(), null, null, 0, 0, null);
            }
            bucketOrds.get(i, spare.wkb);

            spare.wkb = BytesRef.deepCopyOf(spare.wkb);
            spare.wkbHash = String.valueOf(GeoUtils.getHashFromWKB(spare.wkb));

            if (GeoUtils.wkbIsPoint(spare.wkb.bytes)) {
                spare.area = 0;
                spare.realType = "Point";
            } else {
                Geometry geom;

                try {
                    geom = wkbReader.read(spare.wkb.bytes);
                } catch (ParseException e) {
                    continue;
                }

                spare.area = geom.getLength();
                spare.realType = geom.getGeometryType();

            }

            spare.docCount = bucketDocCount(i);
            spare.bucketOrd = i;
            spare = ordered.insertWithOverflow(spare);
        }

        final InternalGeoShape.InternalBucket[] list = new InternalGeoShape.InternalBucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; --i) {
            final InternalGeoShape.InternalBucket bucket = ordered.pop();

            Geometry geom;
            try {
                geom = wkbReader.read(bucket.wkb.bytes);
            } catch (ParseException e) {
                continue;
            }
            if (must_simplify) {
                geom = simplifyGeoShape(geom);
                bucket.wkb = new BytesRef(new WKBWriter().write(geom));
                bucket.area = geom.getLength();

            }

            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
            list[i] = bucket;
        }

        return new InternalGeoShape(name, Arrays.asList(list), output_format, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getShardSize(), pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalGeoShape(name, null, output_format, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getShardSize(), pipelineAggregators(), metaData());
    }

    private Geometry simplifyGeoShape(Geometry geom) {
        Geometry polygonSimplified = getSimplifiedShape(geom);
        if (polygonSimplified.isEmpty()) {
            polygonSimplified = this.geometryFactory.createPoint(geom.getCoordinate());
        }
        return polygonSimplified;
    }

    private Geometry getSimplifiedShape(Geometry geometry) {
        double tol = pixelTolerance * GeoUtils.getToleranceFromZoom(zoom);

        switch (algorithm) {
            case TOPOLOGY_PRESERVING:
                return TopologyPreservingSimplifier.simplify(geometry, tol);
            default:
                return DouglasPeuckerSimplifier.simplify(geometry, tol);
        }
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
            return Objects.equals(requiredSize, other.requiredSize)
                    && Objects.equals(shardSize, other.shardSize);
        }
    }

}
