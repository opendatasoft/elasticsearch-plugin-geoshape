package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.opendatasoft.elasticsearch.plugin.GeoUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


class GeoShapeAggregatorFactory extends ValuesSourceAggregatorFactory {

    private GeoUtils.OutputFormat output_format;
    private boolean must_simplify;
    private int zoom;
    private GeoShape.Algorithm algorithm;
    private final GeoShapeAggregator.BucketCountThresholds bucketCountThresholds;

    GeoShapeAggregatorFactory(String name,
                                   ValuesSourceConfig config,
                                   GeoUtils.OutputFormat output_format,
                                   boolean must_simplify,
                                   int zoom,
                                   GeoShape.Algorithm algorithm,
                                   GeoShapeAggregator.BucketCountThresholds bucketCountThresholds,
                                   QueryShardContext context,
                                   AggregatorFactory parent,
                                   AggregatorFactories.Builder subFactoriesBuilder,
                                   Map<String, Object> metaData
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metaData);
        this.output_format = output_format;
        this.must_simplify = must_simplify;
        this.zoom = zoom;
        this.algorithm = algorithm;
        this.bucketCountThresholds = bucketCountThresholds;
    }

    @Override
    protected Aggregator createUnmapped(
            SearchContext searchContext,
            Aggregator parent,
            Map<String,
                    Object> metadata) throws IOException {
        final InternalAggregation aggregation = new InternalGeoShape(name, new ArrayList<>(), output_format,
                bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getShardSize(),
                metadata);
        return new NonCollectingAggregator(name, searchContext, parent, factories, metadata) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(SearchContext searchContext, Aggregator parent, CardinalityUpperBound cardinality,
                                          Map<String, Object> metadata) throws IOException {
        GeoShapeAggregator.BucketCountThresholds bucketCountThresholds = new
                GeoShapeAggregator.BucketCountThresholds(this.bucketCountThresholds);
        bucketCountThresholds.ensureValidity();
        ValuesSource valuesSourceBytes = config.getValuesSource();
        return new GeoShapeAggregator(
                name,
                factories,
                searchContext,
                valuesSourceBytes,
                output_format,
                must_simplify,
                zoom,
                algorithm,
                bucketCountThresholds,
                parent,
                cardinality,
                metadata);
    }
}

