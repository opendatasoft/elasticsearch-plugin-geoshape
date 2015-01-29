package com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortingBinaryDocValues;
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

    public static final int DEFAULT_ZOOM = 0;
    public static final int DEFAULT_MAX_NUM_CELLS = 0;
    public static final InternalGeoShape.OutputFormat DEFAULT_OUTPUT_FORMAT = InternalGeoShape.OutputFormat.GEOJSON;
    public static final GeoShape.Algorithm DEFAULT_ALGORITHM = GeoShape.Algorithm.DOUGLAS_PEUCKER;


    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceParser vsParser = ValuesSourceParser.any(aggregationName, InternalGeoShape.TYPE, context).scriptable(false).build();

        int requiredSize = DEFAULT_MAX_NUM_CELLS;
        int shardSize = -1;
        int zoom = DEFAULT_ZOOM;
        int clippedBuffer = 0;
        boolean simplifyShape = false;
        boolean smallPolygon = false;
        InternalGeoShape.OutputFormat outputFormat = DEFAULT_OUTPUT_FORMAT;
        GeoShape.Algorithm algorithm = DEFAULT_ALGORITHM;
        ShapeBuilder shapeBuilder = null;

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
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("simplify".equals(currentFieldName)) {
                    simplifyShape = true;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            if ("zoom".equals(currentFieldName)) {
                                zoom = parser.intValue();
                            }
                            else if ("algorithm".equals(currentFieldName)) {
                                algorithm = GeoShape.Algorithm.valueOf(currentFieldName.toUpperCase());
                            }
                        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                            if ("small_polygon".equals(currentFieldName)) {
                                smallPolygon = parser.booleanValue();
                            }
                        }
                    }
                } else if ("clipped".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            if ("buffer".equals(currentFieldName)) {
                                clippedBuffer = parser.intValue();
                            }
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            if ("envelope".equals(currentFieldName)) {
                                shapeBuilder = ShapeBuilder.parse(parser);
                            }
                        }
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

        if (zoom < 0) {
            zoom = DEFAULT_ZOOM;
        }

        Envelope clippedEnvelope = null;
        if (shapeBuilder != null && shapeBuilder.type() == ShapeBuilder.GeoShapeType.ENVELOPE) {
            Rectangle env = (Rectangle) shapeBuilder.build();

            clippedEnvelope = new Envelope(env.getMinX(), env.getMaxX(), env.getMinY(), env.getMaxY());
        }


        return new GeoShapeFactory(aggregationName, vsParser.config(), requiredSize, shardSize, outputFormat, simplifyShape, zoom, algorithm, smallPolygon, clippedEnvelope, clippedBuffer);

    }


    private static class GeoShapeFactory extends ValuesSourceAggregatorFactory<ValuesSource.Bytes> {

        private int requiredSize;
        private int shardSize;
        private InternalGeoShape.OutputFormat outputFormat;
        boolean simplifyShape;
        boolean smallPolygon;
        int zoom;
        int clippedBuffer;
        GeoShape.Algorithm algorithm;
        private Envelope clippedEnvelope;

        public GeoShapeFactory(String name, ValuesSourceConfig config, int requiredSize, int shardSize,
                               InternalGeoShape.OutputFormat outputFormat, boolean simplifyShape, int zoom,
                               GeoShape.Algorithm algorithm, boolean smallPolygon, Envelope clippedEnvelope,
                               int clippedBuffer) {
            super(name, InternalGeoHashGrid.TYPE.name(), config);
            this.requiredSize = requiredSize;
            this.shardSize = shardSize;
            this.outputFormat = outputFormat;
            this.simplifyShape = simplifyShape;
            this.smallPolygon = smallPolygon;
            this.zoom = zoom;
            this.clippedBuffer = clippedBuffer;
            this.algorithm = algorithm;
            this.clippedEnvelope = clippedEnvelope;
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
        protected Aggregator create(ValuesSource.Bytes valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new GeoShapeAggregator(name, factories, valuesSource, requiredSize, shardSize, outputFormat, simplifyShape, smallPolygon, zoom, algorithm, clippedEnvelope, clippedBuffer, aggregationContext, parent);
        }

    }
}
