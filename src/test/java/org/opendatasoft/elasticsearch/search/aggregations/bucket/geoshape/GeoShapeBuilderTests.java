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

    public void testSimplifyRunsAtTheTileZoomByDefault() throws IOException {
        GeoShapeBuilder geoShape = parse("""
            {"field": "f", "simplify": {"algorithm": "topology_preserving"}, "tile": {"z": 6, "x": 31, "y": 22}}""");

        assertEquals(6, geoShape.resolveSimplifyZoom());
        assertEquals(geoShape, parseRendered(render(geoShape)));
    }

    /** z + 1 suits tiles displayed at 512 pixels, whose pixel is half the one the zoom tolerance assumes. */
    public void testAnExplicitZoomOverridesTheTileZoom() throws IOException {
        GeoShapeBuilder geoShape = parse("""
            {"field": "f", "simplify": {"zoom": 7, "algorithm": "douglas_peucker"}, "tile": {"z": 6, "x": 31, "y": 22}}""");

        assertEquals(7, geoShape.resolveSimplifyZoom());
    }

    /** It used to be ignored, so the request silently ran without simplification. */
    public void testSimplifyWithoutZoomNeedsATile() throws IOException {
        GeoShapeBuilder geoShape = parse("{\"field\": \"f\", \"simplify\": {\"algorithm\": \"douglas_peucker\"}}");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, geoShape::resolveSimplifyZoom);
        assertEquals("[simplify] requires a [zoom] when no [tile] is given, in geoshape aggregation.", e.getMessage());
    }

    /** The documented default, which the request used to ignore along with the whole simplify param. */
    public void testAlgorithmDefaultsToDouglasPeucker() throws IOException {
        String rendered = render(parse("{\"field\": \"f\", \"simplify\": {\"zoom\": 5}}"));

        assertTrue(rendered, rendered.contains("\"simplify\":{\"zoom\":5,\"algorithm\":\"DOUGLAS_PEUCKER\"}"));
    }
}
