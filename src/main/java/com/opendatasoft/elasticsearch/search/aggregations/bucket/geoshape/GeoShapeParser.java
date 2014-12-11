package com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoHashGrid;
import org.elasticsearch.search.aggregations.support.*;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;

public class GeoShapeParser implements Aggregator.Parser{

    @Override
    public String type() {
        return InternalGeoShape.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceParser vsParser = ValuesSourceParser.any(aggregationName, InternalGeoShape.TYPE, context).build();

        int requiredSize = 10000;
        int shardSize = -1;
        InternalGeoShape.OutputFormat outputFormat = InternalGeoShape.OutputFormat.GEOJSON;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("size".equals(currentFieldName)) {
                    requiredSize = parser.intValue();
                } else if ("shard_size".equals(currentFieldName) || "shardSize".equals(currentFieldName)) {
                    shardSize = parser.intValue();
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("outputFormat".equals(currentFieldName) || "output_format".equals(currentFieldName)) {
                    try {
                        outputFormat = InternalGeoShape.OutputFormat.valueOf(parser.text().toUpperCase());
                    } catch (IllegalArgumentException e) {
                        // Do nothing : les GeoJson as default format
                    }
                }
            }
        }

        if (shardSize == 0) {
            shardSize = Integer.MAX_VALUE;
        }

        if (requiredSize == 0) {
            requiredSize = Integer.MAX_VALUE;
        }

        if (shardSize < 0) {
            //Use default heuristic to avoid any wrong-ranking caused by distributed counting
            shardSize = BucketUtils.suggestShardSideQueueSize(requiredSize, context.numberOfShards());
        }

        if (shardSize < requiredSize) {
            shardSize = requiredSize;
        }

        return new GeoShapeFactory(aggregationName, vsParser.config(), requiredSize, shardSize, outputFormat);

    }


    private static class GeoShapeFactory extends ValuesSourceAggregatorFactory {

        private int requiredSize;
        private int shardSize;
        private InternalGeoShape.OutputFormat outputFormat;

        public GeoShapeFactory(String name, ValuesSourceConfig config, int requiredSize, int shardSize, InternalGeoShape.OutputFormat outputFormat) {
            super(name, InternalGeoHashGrid.TYPE.name(), config);
            this.requiredSize = requiredSize;
            this.shardSize = shardSize;
            this.outputFormat = outputFormat;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            final InternalAggregation aggregation = new InternalGeoShape(name, requiredSize, outputFormat, Collections.<InternalGeoShape.Bucket>emptyList());
            return new NonCollectingAggregator(name, aggregationContext, parent) {
                @Override
                public InternalAggregation buildEmptyAggregation() {
                    return aggregation;
                }
            };
        }

        @Override
        protected Aggregator create(ValuesSource valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new GeoShapeAggregator(name, factories, valuesSource, requiredSize, shardSize, outputFormat, aggregationContext, parent);
        }
    }
}
