package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.locationtech.jts.geom.Envelope;

import java.io.IOException;

/**
 * Covers the {@code tile} param: parsing, every rejection, the wire, and the equivalence with the WGS84 box
 * the platform sent before z/x/y.
 */
public class TileParamsTests extends ESTestCase {

    private static final int EXTENT = 4096;

    /**
     * How far a derived edge may sit from mercantile's, in degrees. Java and glibc compute sinh and atan
     * differently and the measured gap reaches 2 ulps, while a TMS y or swapped north and south edges are off by
     * whole degrees. 1e-12 is some 70 ulps at 85 degrees and still under a tenth of a grid unit at z29.
     */
    private static final double TOLERANCE = 1e-12;

    /**
     * A tile and the box mercantile gives for it, as (west, south, east, north). Printed by
     * {@code docker compose exec platform python -c "import mercantile; print(mercantile.bounds(x, y, z))"}
     * from the platform repo: mercantile 1.2.1, Python 3.11.15, glibc 2.31, aarch64.
     */
    private record Bounds(int z, int x, int y, double west, double south, double east, double north) {}

    private static final Bounds[] TILES = {
        new Bounds(0, 0, 0, -180.0, -85.0511287798066, 180.0, 85.0511287798066),
        new Bounds(9, 259, 176, 2.109375, 48.45835188280866, 2.8125, 48.92249926375824),
        // The last index of z22: the south-east corner of the world.
        new Bounds(22, 4194303, 4194303, 179.99991416931152, -85.0511287798066, 180.0, -85.05112137546753),
        // The north edge row (y = 0) and the south edge row (y = 2^z - 1).
        new Bounds(9, 0, 0, -180.0, 84.9901001802348, -179.296875, 85.0511287798066),
        new Bounds(9, 511, 511, 179.296875, -85.0511287798066, 180.0, -84.9901001802348),
        new Bounds(22, 0, 0, -180.0, 85.05112137546753, -179.99991416931152, 85.0511287798066),
        // One whose south edge Java puts an ulp away from mercantile's: 47.5172006978394 here.
        new Bounds(9, 259, 178, 2.109375, 47.51720069783939, 2.8125, 47.98992166741417) };

    private TileParams parse(String json) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            return TileParams.parse(parser);
        }
    }

    private void assertRejected(String json, String expectedMessagePart) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parse(json));
        assertTrue(e.getMessage(), e.getMessage().contains(expectedMessagePart));
    }

    public void testExtentAndBufferKeepTheirDefaults() throws IOException {
        assertEquals(new TileParams(9, 259, 176, 4096, 0.0625), parse("{\"z\": 9, \"x\": 259, \"y\": 176}"));
    }

    public void testEachTileIndexIsMandatory() {
        assertRejected("{\"x\": 259, \"y\": 176}", "Required one of fields [z]");
        assertRejected("{\"z\": 9, \"y\": 176}", "Required one of fields [x]");
        assertRejected("{\"z\": 9, \"x\": 259}", "Required one of fields [y]");
    }

    public void testNegativeIndexIsRejected() {
        assertRejected("{\"z\": -1, \"x\": 0, \"y\": 0}", "[z] must be within [0, 29], got [-1]");
        assertRejected("{\"z\": 9, \"x\": -1, \"y\": 176}", "[x] must be within [0, 511] at [z] [9], got [-1]");
        assertRejected("{\"z\": 9, \"x\": 259, \"y\": -1}", "[y] must be within [0, 511] at [z] [9], got [-1]");
    }

    public void testIndexPastItsZoomIsRejected() throws IOException {
        assertRejected("{\"z\": 9, \"x\": 512, \"y\": 176}", "[x] must be within [0, 511] at [z] [9], got [512]");
        assertRejected("{\"z\": 9, \"x\": 259, \"y\": 512}", "[y] must be within [0, 511] at [z] [9], got [512]");
        assertRejected("{\"z\": 0, \"x\": 1, \"y\": 0}", "[x] must be within [0, 0] at [z] [0], got [1]");
        // The last index of a zoom is still a tile.
        assertEquals(new TileParams(9, 511, 511, EXTENT, TileParams.DEFAULT_BUFFER), parse("{\"z\": 9, \"x\": 511, \"y\": 511}"));
    }

    /**
     * z stops where elasticsearch's geotile_grid does. The last index of z29 must neither overflow 1 << z nor
     * 2 * (y + 1), which would turn its box into garbage rather than fail.
     */
    public void testZoomPastTwentyNineIsRejected() {
        assertRejected("{\"z\": 30, \"x\": 0, \"y\": 0}", "[z] must be within [0, 29], got [30]");

        int last = (1 << 29) - 1;
        Envelope box = new TileParams(29, last, last, EXTENT, 0).clipEnvelope();
        assertEquals(180.0, box.getMaxX(), 0.0);
        assertEquals(-85.0511287798066, box.getMinY(), TOLERANCE);
        assertTrue("the box must not be degenerate", box.getWidth() > 0 && box.getHeight() > 0);
    }

    /** Web mercator stops at +/-85.0511: the buffer widens an edge tile's window everywhere but past that limit. */
    public void testBufferStopsAtTheMercatorLatitudeLimit() {
        Envelope world = new TileParams(0, 0, 0, EXTENT, TileParams.DEFAULT_BUFFER).clipEnvelope();
        assertEquals(-85.0511287798066, world.getMinY(), TOLERANCE);
        assertEquals(85.0511287798066, world.getMaxY(), TOLERANCE);
        assertEquals(-202.5, world.getMinX(), 0.0);
        assertEquals(202.5, world.getMaxX(), 0.0);
    }

    /** bbox was the window before z/x/y: it must fail like any unknown field, never be ignored next to a tile. */
    public void testABboxIsRejectedAsAnUnknownField() {
        String bbox = "\"bbox\": [2.109375, 48.45835188280866, 2.8125, 48.92249926375824]";
        for (String json : new String[] { "{\"z\": 9, \"x\": 259, \"y\": 176, " + bbox + "}", "{" + bbox + "}" }) {
            XContentParseException e = expectThrows(XContentParseException.class, () -> parse(json));
            assertTrue(e.getMessage(), e.getMessage().contains("unknown field [bbox]"));
        }
    }

    public void testTheCodeConstructorValidatesToo() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new TileParams(9, 512, 0, EXTENT, 0));
        assertTrue(e.getMessage(), e.getMessage().contains("[x]"));
    }

    public void testWireRoundTrip() throws IOException {
        TileParams[] tiles = { new TileParams(9, 259, 176, EXTENT, TileParams.DEFAULT_BUFFER), new TileParams(22, 4194303, 0, 512, 0) };
        for (TileParams tile : tiles) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                tile.writeTo(out);
                TileParams read = new TileParams(out.bytes().streamInput());
                assertEquals(tile, read);
            }
        }
    }

    public void testXContentRoundTrip() throws IOException {
        TileParams tile = new TileParams(9, 259, 176, EXTENT, TileParams.DEFAULT_BUFFER);
        assertEquals("{\"z\":9,\"x\":259,\"y\":176,\"extent\":4096,\"buffer\":0.0625}", Strings.toString(tile));
        assertEquals(tile, parse(Strings.toString(tile)));
    }

    public void testTilesOneIndexApartAreDifferentTiles() {
        TileParams tile = new TileParams(9, 259, 176, EXTENT, TileParams.DEFAULT_BUFFER);
        TileParams same = new TileParams(9, 259, 176, EXTENT, TileParams.DEFAULT_BUFFER);
        assertEquals(tile, same);
        assertEquals(tile.hashCode(), same.hashCode());
        assertNotEquals(tile, new TileParams(9, 260, 176, EXTENT, TileParams.DEFAULT_BUFFER));
        assertNotEquals(tile, new TileParams(9, 259, 177, EXTENT, TileParams.DEFAULT_BUFFER));
        assertNotEquals(tile, new TileParams(10, 259, 176, EXTENT, TileParams.DEFAULT_BUFFER));
    }

    /**
     * The box derived from z/x/y is the one mercantile gives, i.e. the box the platform sent before z/x/y.
     * Each edge is compared on its own, so a north edge read from y + 1 fails even though the box stays valid.
     * The buffer is 0, so clipEnvelope() is the box itself; mercatorEnvelope() projects those same corners.
     */
    public void testBoxIsTheOneMercantileGives() {
        for (Bounds b : TILES) {
            Envelope box = new TileParams(b.z(), b.x(), b.y(), EXTENT, 0).clipEnvelope();
            String name = b.z() + "/" + b.x() + "/" + b.y();
            assertEquals(name + " west", b.west(), box.getMinX(), TOLERANCE);
            assertEquals(name + " south", b.south(), box.getMinY(), TOLERANCE);
            assertEquals(name + " east", b.east(), box.getMaxX(), TOLERANCE);
            assertEquals(name + " north", b.north(), box.getMaxY(), TOLERANCE);
        }
    }
}
