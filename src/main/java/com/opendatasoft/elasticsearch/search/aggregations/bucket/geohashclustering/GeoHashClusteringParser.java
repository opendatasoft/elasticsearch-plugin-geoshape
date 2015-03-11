package com.opendatasoft.elasticsearch.search.aggregations.bucket.geohashclustering;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.support.*;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;

public class GeoHashClusteringParser implements Aggregator.Parser {

    @Override
    public String type() {
        return InternalGeoHashClustering.TYPE.name();
    }

    public static final int DEFAULT_ZOOM = 0;
    public static final int DEFAULT_DISTANCE = 100;

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceParser vsParser = ValuesSourceParser.geoPoint(aggregationName, InternalGeoHashClustering.TYPE, context).build();

        int zoom = DEFAULT_ZOOM;
        int distance = DEFAULT_DISTANCE;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("zoom".equals(currentFieldName)) {
                    zoom = parser.intValue();
                } else if ("distance".equals(currentFieldName)) {
                    distance = parser.intValue();
                }
            }
        }

        return new GeoGridFactory(aggregationName, vsParser.config(), zoom, distance);
    }


    private static class GeoGridFactory extends ValuesSourceAggregatorFactory<ValuesSource.GeoPoint> {

        private int zoom;
        private int distance;

        public GeoGridFactory(String name, ValuesSourceConfig<ValuesSource.GeoPoint> config,
                              int zoom, int distance) {
            super(name, InternalGeoHashClustering.TYPE.name(), config);
            this.zoom = zoom;
            this.distance = distance;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            final InternalAggregation aggregation = new InternalGeoHashClustering(name, Collections.<InternalGeoHashClustering.Bucket>emptyList(), distance, zoom);
            return new NonCollectingAggregator(name, aggregationContext, parent) {
                public InternalAggregation buildEmptyAggregation() {
                    return aggregation;
                }
            };
        }

        @Override
        protected Aggregator create(final ValuesSource.GeoPoint valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new GeoHashClusteringAggregator(name, factories, valuesSource,
              aggregationContext, parent, zoom, distance);

        }
    }

}