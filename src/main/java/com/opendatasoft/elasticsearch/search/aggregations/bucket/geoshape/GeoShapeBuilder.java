package com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: clement
 * Date: 11/12/14
 * Time: 15:00
 * To change this template use File | Settings | File Templates.
 */
public class GeoShapeBuilder extends AggregationBuilder<GeoShapeBuilder>{

    private String field;
    private int zoom = GeoShapeParser.DEFAULT_ZOOM;
    private int requiredSize = GeoShapeParser.DEFAULT_MAX_NUM_CELLS;
    private int shardSize = 0;
    private boolean simplifyShape = false;
    private GeoShape.Algorithm algorithm;
    InternalGeoShape.OutputFormat outputFormat = InternalGeoShape.OutputFormat.GEOJSON;


    public GeoShapeBuilder(String name) {
        super(name, InternalGeoShape.TYPE.name());
    }

    public GeoShapeBuilder field(String field) {
        this.field = field;
        return this;
    }

    public GeoShapeBuilder simplifyShape(boolean simplifyShape) {
        this.simplifyShape = simplifyShape;
        return this;
    }

    public GeoShapeBuilder zoom(int zoom) {
        if (zoom < 0) {
            zoom = GeoShapeParser.DEFAULT_ZOOM;
        }
        this.zoom = zoom;
        return this;
    }

    public GeoShapeBuilder outputFormat(InternalGeoShape.OutputFormat outputFormat) {
        this.outputFormat = outputFormat;
        return this;
    }

    public GeoShapeBuilder algorithm(GeoShape.Algorithm algorithm) {
        this.algorithm = algorithm;
        return this;
    }

    public GeoShapeBuilder size(int requiredSize) {
        this.requiredSize = requiredSize;
        return this;
    }

    public GeoShapeBuilder shardSize(int shardSize) {
        this.shardSize = shardSize;
        return this;
    }


    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject();

        if (field != null) {
            builder.field("field", field);
        }
        if (simplifyShape) {
            builder.startObject("simplify");
            builder.field("zoom", zoom);
            if (algorithm != GeoShapeParser.DEFAULT_ALGORITHM) {
                builder.field("algorithm", algorithm);
            }
            builder.endObject();
        }
        if (outputFormat != GeoShapeParser.DEFAULT_OUTPUT_FORMAT)
        {
            builder.field("output_format", outputFormat.toString());
        }

        if (requiredSize != GeoShapeParser.DEFAULT_MAX_NUM_CELLS) {
            builder.field("size", requiredSize);
        }
        if (shardSize != 0) {
            builder.field("shard_size", shardSize);
        }

        return builder.endObject();
    }
}
