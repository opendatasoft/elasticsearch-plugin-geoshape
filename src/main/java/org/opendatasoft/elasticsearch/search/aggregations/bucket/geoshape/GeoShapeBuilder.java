package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.opendatasoft.elasticsearch.plugin.GeoUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;


/**
 * The builder of the aggregatorFactory. Also implements the parsing of the request.
 */
public class GeoShapeBuilder extends ValuesSourceAggregationBuilder</*ValuesSource, */GeoShapeBuilder>
        /*implements MultiBucketAggregationBuilder*/ {
    public static final String NAME = "geoshape";

    public static final ValuesSourceRegistry.RegistryKey<GeoShapeAggregatorSupplier> REGISTRY_KEY =
            new ValuesSourceRegistry.RegistryKey<>(NAME, GeoShapeAggregatorSupplier.class);

    private static final ParseField OUTPUT_FORMAT_FIELD = new ParseField("output_format");
    public static final ParseField SIMPLIFY_FIELD = new ParseField("simplify");
    public static final ParseField SIZE_FIELD = new ParseField("size");
    public static final ParseField SHARD_SIZE_FIELD = new ParseField("shard_size");

    public static final GeoShapeAggregator.BucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS = new
            GeoShapeAggregator.BucketCountThresholds(10, -1);
    private static final ObjectParser<GeoShapeBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(GeoShapeBuilder.NAME);
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
        PARSER.declareString(GeoShapeBuilder::output_format, OUTPUT_FORMAT_FIELD);
        PARSER.declareObjectArray(GeoShapeBuilder::simplify_keys,
                (p, c) -> SimplifyKeysParser.Parser.parseSimplifyParam(p), SIMPLIFY_FIELD);
        PARSER.declareInt(GeoShapeBuilder::size, SIZE_FIELD);
        PARSER.declareInt(GeoShapeBuilder::shardSize, SHARD_SIZE_FIELD);
    }

    public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new GeoShapeBuilder(aggregationName), null);
    }

    static class SimplifyKeysParser {
        static class Parser {
            static List<Object> parseSimplifyParam(XContentParser parser) throws IOException {
                XContentParser.Token token;
                int zoom = -1;
                String algorithm = null;

                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.VALUE_NUMBER) {
                        if ("zoom".equals(currentFieldName)) {
                            zoom = parser.intValue();
                        }
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        if ("algorithm".equals(currentFieldName)) {
                            algorithm = parser.text();
                        }
                    }
                }
                if ((zoom != -1) && (algorithm != null))
                    return Arrays.asList(zoom, algorithm);
                else
                    return Collections.emptyList();
            }
        }
    }

    public static final GeoUtils.OutputFormat DEFAULT_OUTPUT_FORMAT = GeoUtils.OutputFormat.GEOJSON;
    private boolean must_simplify = false;
    public static final int DEFAULT_ZOOM = 0;
    public static final GeoShape.Algorithm DEFAULT_ALGORITHM = GeoShape.Algorithm.DOUGLAS_PEUCKER;
    private GeoUtils.OutputFormat output_format = DEFAULT_OUTPUT_FORMAT;
    private int simplify_zoom = DEFAULT_ZOOM;
    private GeoShape.Algorithm simplify_algorithm = DEFAULT_ALGORITHM;
    private GeoShapeAggregator.BucketCountThresholds bucketCountThresholds = new GeoShapeAggregator.BucketCountThresholds(
            DEFAULT_BUCKET_COUNT_THRESHOLDS);


    private GeoShapeBuilder(String name) {
        super(name);
    }

    /**
     * Read from a stream
     *
     */
    public GeoShapeBuilder(StreamInput in) throws IOException {
        super(in);
        bucketCountThresholds = new GeoShapeAggregator.BucketCountThresholds(in);
        must_simplify = in.readBoolean();
        output_format = GeoUtils.OutputFormat.valueOf(in.readString());
        simplify_zoom = in.readInt();
        simplify_algorithm = GeoShape.Algorithm.valueOf(in.readString());
    }

    /**
     * Write to a stream
     */
    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        bucketCountThresholds.writeTo(out);
        out.writeBoolean(must_simplify);
        out.writeString(output_format.name());
        out.writeInt(simplify_zoom);
        out.writeString(simplify_algorithm.name());
    }

    private GeoShapeBuilder(GeoShapeBuilder clone, Builder factoriesBuilder,
                                            Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        output_format = clone.output_format;
        must_simplify = clone.must_simplify;
        simplify_zoom = clone.simplify_zoom;
        simplify_algorithm = clone.simplify_algorithm;
        this.bucketCountThresholds = new GeoShapeAggregator.BucketCountThresholds(clone.bucketCountThresholds);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        return new GeoShapeBuilder(this, factoriesBuilder, metaData);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return null;
    }

    private GeoShapeBuilder output_format(String output_format) {
        this.output_format = GeoUtils.OutputFormat.valueOf(output_format.toUpperCase(Locale.getDefault()));
        return this;
    }

    @SuppressWarnings("unchecked")
    private GeoShapeBuilder simplify_keys(List<Object> simplify) {
        List<Object> simplify_keys = (List<Object>) simplify.get(0);
        if (!simplify_keys.isEmpty()) {
            this.must_simplify = true;
            this.simplify_zoom = (int) simplify_keys.get(0);
            this.simplify_algorithm = GeoShape.Algorithm.valueOf(((String) simplify_keys.get(1)).toUpperCase(Locale.getDefault()));
        }
        return this;
    }

    @Override
    protected boolean serializeTargetValueType(Version version) {
        return true;
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return REGISTRY_KEY;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    /**
     * Sets the size - indicating how many term buckets should be returned
     * (defaults to 10)
     */
    public GeoShapeBuilder size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("[size] must be greater than 0. Found [" + size + "] in [" + name + "]");
        }
        bucketCountThresholds.setRequiredSize(size);
        return this;
    }

    /**
     * Sets the shard_size - indicating the number of term buckets each shard
     * will return to the coordinating node (the node that coordinates the
     * search execution). The higher the shard size is, the more accurate the
     * results are.
     */
    public GeoShapeBuilder shardSize(int shardSize) {
        if (shardSize <= 0) {
            throw new IllegalArgumentException(
                    "[shardSize] must be greater than 0. Found [" + shardSize + "] in [" + name + "]");
        }
        bucketCountThresholds.setShardSize(shardSize);
        return this;
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(
            QueryShardContext queryShardContext,
            ValuesSourceConfig config,
            AggregatorFactory parent,
            AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        return new GeoShapeAggregatorFactory(
                name, config, output_format, must_simplify, simplify_zoom, simplify_algorithm,
                bucketCountThresholds, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (!output_format.equals(DEFAULT_OUTPUT_FORMAT)) {
            builder.field(OUTPUT_FORMAT_FIELD.getPreferredName(), output_format);
        }

        return builder.endObject();
    }

    /**
     * Used for caching requests, amongst other things.
     */
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), output_format, must_simplify, simplify_zoom, simplify_algorithm, bucketCountThresholds);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (!super.equals(obj)) return false;

        GeoShapeBuilder other = (GeoShapeBuilder) obj;
        return Objects.equals(output_format, other.output_format)
                && Objects.equals(must_simplify, other.must_simplify)
                && Objects.equals(simplify_zoom, other.simplify_zoom)
                && Objects.equals(simplify_algorithm, other.simplify_algorithm)
                && Objects.equals(bucketCountThresholds, other.bucketCountThresholds);
    }

    @Override
    public String getType() {
        return NAME;
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
                GeoShapeBuilder.REGISTRY_KEY,
                CoreValuesSourceType.BYTES,
                GeoShapeAggregator::new,
                true);
    }
}

