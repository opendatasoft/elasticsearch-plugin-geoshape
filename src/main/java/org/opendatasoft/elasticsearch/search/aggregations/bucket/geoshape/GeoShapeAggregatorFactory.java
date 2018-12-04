package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


class GeoShapeAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource, GeoShapeAggregatorFactory> {

    private InternalGeoShape.OutputFormat output_format;
    private boolean must_simplify;
    private int zoom;
    private GeoShape.Algorithm algorithm;
    private final GeoShapeAggregator.BucketCountThresholds bucketCountThresholds;

    GeoShapeAggregatorFactory(String name,
                                   ValuesSourceConfig<ValuesSource> config,
                                   InternalGeoShape.OutputFormat output_format,
                                   boolean must_simplify,
                                   int zoom,
                                   GeoShape.Algorithm algorithm,
                                   GeoShapeAggregator.BucketCountThresholds bucketCountThresholds,
                                   SearchContext context,
                                   AggregatorFactory<?> parent,
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
            Aggregator parent,
            List<PipelineAggregator> pipelineAggregators,
            Map<String,
                    Object> metaData) throws IOException {
        final InternalAggregation aggregation = new InternalGeoShape(name, new ArrayList<>(), output_format,
                bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getShardSize(),
                pipelineAggregators, metaData);
        return new NonCollectingAggregator(name, context, parent, factories, pipelineAggregators, metaData) {
            @Override
            public InternalAggregation buildEmptyAggregation() { return aggregation; }
        };
    }

    @Override
    protected Aggregator doCreateInternal(
            ValuesSource valuesSource, Aggregator parent,
            boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        GeoShapeAggregator.BucketCountThresholds bucketCountThresholds = new
                GeoShapeAggregator.BucketCountThresholds(this.bucketCountThresholds);
        bucketCountThresholds.ensureValidity();
        return new GeoShapeAggregator(
                name, factories, context, valuesSource, output_format, must_simplify, zoom, algorithm,
                bucketCountThresholds, parent, pipelineAggregators, metaData);
    }
}

