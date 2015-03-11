package com.opendatasoft.elasticsearch.search.aggregations.bucket.geohashclustering;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.io.IOException;

public class GeoHashClusteringBuilder extends AggregationBuilder<GeoHashClusteringBuilder> {

    private String field;
    private int zoom = GeoHashClusteringParser.DEFAULT_ZOOM;
    private int distance = GeoHashClusteringParser.DEFAULT_DISTANCE;

    public GeoHashClusteringBuilder(String name) {
        super(name, InternalGeoHashClustering.TYPE.name());
    }


    public GeoHashClusteringBuilder field(String field) {
        this.field = field;
        return this;
    }

    public GeoHashClusteringBuilder zoom(int zoom) {
        this.zoom = zoom;
        return this;
    }

    public GeoHashClusteringBuilder distance(int distance) {
        this.distance = distance;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field("field", field);
        }
        if (zoom != GeoHashClusteringParser.DEFAULT_ZOOM) {
            builder.field("zoom", zoom);
        }

        if (distance != GeoHashClusteringParser.DEFAULT_DISTANCE) {
            builder.field("distance", distance);
        }
        return builder.endObject();
    }
}
