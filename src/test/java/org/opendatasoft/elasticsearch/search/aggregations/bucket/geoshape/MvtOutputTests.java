package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.opendatasoft.elasticsearch.plugin.GeoUtils;
import org.opendatasoft.elasticsearch.plugin.GeoUtils.OutputFormat;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Covers what the {@code mvt} output format changes outside the encoder: the bucket payload on the
 * wire between a shard and the coordinator, and the rendering of {@code key}.
 */
public class MvtOutputTests extends ESTestCase {

    private static InternalGeoShape.InternalBucket bucket(long shapeHash, byte[] wkb, int[] mvt) {
        return new InternalGeoShape.InternalBucket(new BytesRef(wkb), mvt, shapeHash, "Polygon", 42.0, 3, InternalAggregations.EMPTY, null);
    }

    private static InternalGeoShape shape(OutputFormat outputFormat, InternalGeoShape.InternalBucket... buckets) {
        return new InternalGeoShape("g", List.of(buckets), outputFormat, null, 10, 10, 0, Map.of());
    }

    private static InternalGeoShape roundTrip(InternalGeoShape shape) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            shape.writeTo(out);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), new NamedWriteableRegistry(List.of()))) {
                return new InternalGeoShape(in);
            }
        }
    }

    private static String render(InternalGeoShape shape) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        shape.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return Strings.toString(builder);
    }

    /** A square of side 4 at the grid origin, as the encoder writes it. */
    private static final int[] SQUARE = new int[] { 9, 0, 0, 26, 8, 0, 0, 8, 7, 0, 15 };

    public void testCommandStreamSurvivesTheWire() throws IOException {
        InternalGeoShape read = roundTrip(
            shape(OutputFormat.MVT, bucket(1, new byte[0], SQUARE), bucket(2, new byte[0], new int[] { 9, 2, 4 }))
        );

        assertEquals(2, read.getBuckets().size());
        assertArrayEquals(SQUARE, read.getBuckets().get(0).mvt);
        assertArrayEquals(new int[] { 9, 2, 4 }, read.getBuckets().get(1).mvt);
    }

    public void testNegativeCommandHeaderSurvivesTheWire() throws IOException {
        // A repeat count close to the 29-bit limit makes the packed header negative as a signed int.
        // Nothing real produces one, but the payload must not depend on that.
        int[] extreme = new int[] { (1 << 29) - 1 << 3 | 2, 0, 0 };

        InternalGeoShape read = roundTrip(shape(OutputFormat.MVT, bucket(1, new byte[0], extreme)));

        assertArrayEquals(extreme, read.getBuckets().get(0).mvt);
    }

    public void testKeyIsRenderedAsAnArrayOfIntegers() throws IOException {
        String json = render(shape(OutputFormat.MVT, bucket(1, new byte[0], SQUARE)));

        assertTrue(json, json.contains("\"key\":[9,0,0,26,8,0,0,8,7,0,15]"));
        assertTrue(json, json.contains("\"digest\":\"1\""));
        assertTrue(json, json.contains("\"type\":\"Polygon\""));
    }

    public void testKeyIsStillAStringForTheOtherFormats() throws IOException {
        // A one-point WKB, little endian, so the geometry is readable by the writers.
        byte[] point = new byte[] { 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

        String json = render(shape(OutputFormat.WKB, bucket(1, point, null)));

        assertTrue(json, json.contains("\"key\":\"0101000000"));
    }

    public void testTextualWritersRejectTheCommandStreamFormat() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> GeoUtils.exportWkbTo(new BytesRef(new byte[0]), OutputFormat.MVT, GeoUtils.createGeoJsonWriter())
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[mvt]"));
    }
}
