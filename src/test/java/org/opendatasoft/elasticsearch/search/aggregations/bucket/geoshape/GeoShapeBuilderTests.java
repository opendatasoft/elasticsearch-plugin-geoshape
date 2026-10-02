package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

/**
 * Covers the rendering of the aggregation request, which elasticsearch uses in the slow log and the task
 * descriptions.
 */
public class GeoShapeBuilderTests extends ESTestCase {

    private GeoShapeBuilder parse(String json) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            return GeoShapeBuilder.parse(parser, "g");
        }
    }

    /** Parse the {@code {"g": {"geoshape": {...}}}} that {@code toXContent} renders. */
    private GeoShapeBuilder parseRendered(String json) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            for (int token = 0; token < 5; token++) {
                parser.nextToken();
            }
            return GeoShapeBuilder.parse(parser, "g");
        }
    }

    private static String render(GeoShapeBuilder geoShape) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        geoShape.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return Strings.toString(builder);
    }

    public void testEveryParamSurvivesARenderAndAParse() throws IOException {
        GeoShapeBuilder geoShape = parse("""
            {
              "field": "geo_shape_0.wkb",
              "output_format": "mvt",
              "simplify": {"zoom": 5, "algorithm": "topology_preserving"},
              "tile": {"z": 4, "x": 8, "y": 5, "extent": 512, "buffer": 0.125},
              "collect_fields": {"fields": ["id"], "max_docs_per_bucket": 3},
              "size": 20,
              "shard_size": 30
            }""");

        assertEquals(geoShape, parseRendered(render(geoShape)));
    }

    public void testDefaultsSurviveARenderAndAParse() throws IOException {
        GeoShapeBuilder geoShape = parse("{\"field\": \"geo_shape_0.wkb\"}");

        assertEquals(geoShape, parseRendered(render(geoShape)));
    }
}
