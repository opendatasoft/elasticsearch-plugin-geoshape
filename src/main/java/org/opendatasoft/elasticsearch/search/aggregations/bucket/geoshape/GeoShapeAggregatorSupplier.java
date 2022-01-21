package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;
import org.opendatasoft.elasticsearch.plugin.GeoUtils;

import java.io.IOException;
import java.util.Map;

@FunctionalInterface
public interface GeoShapeAggregatorSupplier {
    Aggregator build(
            String name,
            AggregatorFactories factories,
            SearchContext context,
            ValuesSource valuesSource,
            GeoUtils.OutputFormat output_format,
            boolean must_simplify,
            int zoom,
            GeoShape.Algorithm algorithm,
            GeoShapeAggregator.BucketCountThresholds bucketCountThresholds,
            Aggregator parent,
            CardinalityUpperBound cardinalityUpperBound,
            Map<String, Object> metadata) throws IOException;
}
