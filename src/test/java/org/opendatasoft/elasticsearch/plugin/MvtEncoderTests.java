package org.opendatasoft.elasticsearch.plugin;

import org.elasticsearch.test.ESTestCase;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.List;

/**
 * Covers {@link MvtEncoder}, the pure function from a geometry already quantized onto a tile grid to
 * the MVT command stream that draws it.
 *
 * <p>Two kinds of assertion are used deliberately. Short streams are compared integer by integer,
 * because the bit packing and the zigzag encoding are exactly what has to be pinned down. Larger
 * geometry goes through {@link #decode}, an implementation written from the spec rather than from the
 * encoder, so that a shared misreading of the spec cannot make a test pass.
 */
public class MvtEncoderTests extends ESTestCase {

    private final GeometryFactory factory = new GeometryFactory();

    private static final int MOVE_TO_1 = 1 << 3 | 1;
    private static final int CLOSE_PATH = 1 << 3 | 7;

    private static int lineTo(int count) {
        return count << 3 | 2;
    }

    private static int moveTo(int count) {
        return count << 3 | 1;
    }

    private Coordinate[] coords(double... xy) {
        Coordinate[] coordinates = new Coordinate[xy.length / 2];
        for (int i = 0; i < coordinates.length; i++) {
            coordinates[i] = new Coordinate(xy[2 * i], xy[2 * i + 1]);
        }
        return coordinates;
    }

    private LinearRing ring(double... xy) {
        return factory.createLinearRing(coords(xy));
    }

    /** The unit square of the plan's worked example: a closed JTS ring of five coordinates. */
    private LinearRing square() {
        return ring(0, 0, 4, 0, 4, 4, 0, 4, 0, 0);
    }

    // ---------------------------------------------------------------------------------------------
    // The encoding itself
    // ---------------------------------------------------------------------------------------------

    public void testPolygonRingIsMoveToLineToClosePath() {
        int[] stream = MvtEncoder.encode(factory.createPolygon(square()));

        // MoveTo 1 (0,0); LineTo 3 (+4,0) (0,+4) (-4,0); ClosePath. Deltas are zigzag encoded, so
        // +4 is 8 and -4 is 7.
        assertArrayEquals(new int[] { MOVE_TO_1, 0, 0, lineTo(3), 8, 0, 0, 8, 7, 0, CLOSE_PATH }, stream);
    }

    public void testRingDoesNotRepeatItsFirstPoint() {
        // JTS closes a ring by repeating the first coordinate, MVT closes it with ClosePath: the
        // repeated coordinate must not reach the stream.
        int[] stream = MvtEncoder.encode(factory.createPolygon(square()));

        assertEquals("one header, one pair, one header, three pairs, one header", 11, stream.length);
        assertEquals(1, countCommands(stream, 7));
    }

    public void testTrailingPointEqualToTheFirstIsDropped() {
        // Quantization can land the last vertex on the first one; ClosePath already draws that segment.
        int[] withTrailingDuplicate = MvtEncoder.encode(factory.createPolygon(ring(0, 0, 4, 0, 4, 4, 0, 4, 0, 0, 0, 0)));

        assertArrayEquals(MvtEncoder.encode(factory.createPolygon(square())), withTrailingDuplicate);
    }

    public void testConsecutiveIdenticalPointsAreSkipped() {
        // (4,0) appears twice in a row, and 4.4 and 3.6 both round onto 4.
        int[] stream = MvtEncoder.encode(factory.createPolygon(ring(0, 0, 4, 0, 4.4, 0, 3.6, 0, 4, 4, 0, 4, 0, 0)));

        assertArrayEquals(MvtEncoder.encode(factory.createPolygon(square())), stream);
    }

    public void testCursorIsSharedAcrossTheRingsOfAPolygon() {
        Polygon polygon = factory.createPolygon(square(), new LinearRing[] { ring(1, 1, 1, 3, 3, 3, 3, 1, 1, 1) });

        int[] stream = MvtEncoder.encode(polygon);

        // The shell leaves the cursor on its last point (0,4), so the hole's first pair is the delta
        // from there: (+1,-3), i.e. zigzag 2 and 5.
        assertArrayEquals(
            new int[] {
                MOVE_TO_1,
                0,
                0,
                lineTo(3),
                8,
                0,
                0,
                8,
                7,
                0,
                CLOSE_PATH,
                MOVE_TO_1,
                2,
                5,
                lineTo(3),
                0,
                4,
                4,
                0,
                0,
                3,
                CLOSE_PATH },
            stream
        );
    }

    public void testCursorIsSharedAcrossThePartsOfAMultiPolygon() {
        Geometry multi = factory.createMultiPolygon(
            new Polygon[] { factory.createPolygon(square()), factory.createPolygon(ring(10, 0, 14, 0, 14, 4, 10, 4, 10, 0)) }
        );

        int[] stream = MvtEncoder.encode(multi);

        // Decoding is what proves the point: the second part must start at (10,0) in absolute
        // coordinates, which only holds if its delta was taken from where the first part left the pen.
        List<List<int[]>> parts = decode(stream);
        assertEquals(2, parts.size());
        assertArrayEquals(new int[] { 0, 0 }, parts.get(0).get(0));
        assertArrayEquals(new int[] { 10, 0 }, parts.get(1).get(0));
    }

    public void testPoint() {
        assertArrayEquals(new int[] { MOVE_TO_1, 6, 8 }, MvtEncoder.encode(factory.createPoint(new Coordinate(3, 4))));
    }

    public void testMultiPointIsASingleMoveTo() {
        Geometry multiPoint = factory.createMultiPointFromCoords(coords(1, 1, 2, 3));

        // One MoveTo carrying both pairs, not two MoveTo commands.
        assertArrayEquals(new int[] { moveTo(2), 2, 2, 2, 4 }, MvtEncoder.encode(multiPoint));
    }

    public void testLineStringHasNoClosePath() {
        LineString line = factory.createLineString(coords(0, 0, 2, 0, 2, 2));

        int[] stream = MvtEncoder.encode(line);

        assertArrayEquals(new int[] { MOVE_TO_1, 0, 0, lineTo(2), 4, 0, 0, 4 }, stream);
        assertEquals(0, countCommands(stream, 7));
    }

    public void testMultiLineString() {
        Geometry multiLine = factory.createMultiLineString(
            new LineString[] { factory.createLineString(coords(0, 0, 2, 0)), factory.createLineString(coords(5, 5, 7, 5)) }
        );

        List<List<int[]>> parts = decode(MvtEncoder.encode(multiLine));

        assertEquals(2, parts.size());
        assertEquals(List.of("0,0", "2,0"), flatten(parts.get(0)));
        assertEquals(List.of("5,5", "7,5"), flatten(parts.get(1)));
    }

    // ---------------------------------------------------------------------------------------------
    // What quantization destroys
    // ---------------------------------------------------------------------------------------------

    public void testRingBelowTwoLineToPairsIsDroppedWithItsHeader() {
        // Every vertex rounds onto (5,5): nothing is left to draw, and an empty MoveTo/ClosePath pair
        // would still cost four integers and produce a degenerate feature.
        Polygon collapsed = factory.createPolygon(ring(5, 5, 5.2, 5, 5, 5.2, 5, 5));

        assertArrayEquals(new int[0], MvtEncoder.encode(collapsed));
    }

    public void testPolygonWhoseShellCollapsedIsDroppedWithItsHoles() {
        Polygon polygon = factory.createPolygon(ring(5, 5, 5.2, 5, 5, 5.2, 5, 5), new LinearRing[] { square() });

        assertArrayEquals(new int[0], MvtEncoder.encode(polygon));
    }

    public void testCollapsedHoleDoesNotMoveTheCursor() {
        Polygon polygon = factory.createPolygon(
            square(),
            new LinearRing[] { ring(2, 2, 2.1, 2, 2, 2.1, 2, 2), ring(1, 1, 1, 3, 3, 3, 3, 1, 1, 1) }
        );

        // The surviving hole must be encoded exactly as if the collapsed one had never been there.
        assertArrayEquals(
            MvtEncoder.encode(factory.createPolygon(square(), new LinearRing[] { ring(1, 1, 1, 3, 3, 3, 3, 1, 1, 1) })),
            MvtEncoder.encode(polygon)
        );
    }

    public void testGeometryLeftWithNoRingProducesAnEmptyStream() {
        Geometry multi = factory.createMultiPolygon(
            new Polygon[] {
                factory.createPolygon(ring(5, 5, 5.2, 5, 5, 5.2, 5, 5)),
                factory.createPolygon(ring(9, 9, 9.1, 9, 9, 9.1, 9, 9)) }
        );

        assertArrayEquals(new int[0], MvtEncoder.encode(multi));
    }

    public void testLineStringLeftWithASinglePointIsDropped() {
        assertArrayEquals(new int[0], MvtEncoder.encode(factory.createLineString(coords(3, 3, 3.2, 3))));
    }

    public void testEmptyGeometryProducesAnEmptyStream() {
        assertArrayEquals(new int[0], MvtEncoder.encode(factory.createPolygon()));
    }

    // ---------------------------------------------------------------------------------------------
    // Guards
    // ---------------------------------------------------------------------------------------------

    public void testCommandCountWiderThan29BitsIsRejected() {
        // A ring this long cannot occur at a tile extent of 4096, but a silently truncated header
        // would corrupt the whole stream rather than fail.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> MvtEncoder.command(2, 1 << 29));
        assertTrue(e.getMessage(), e.getMessage().contains("29 bits"));

        assertEquals(MvtEncoder.MAX_COMMAND_COUNT << 3 | 2, MvtEncoder.command(2, MvtEncoder.MAX_COMMAND_COUNT));
    }

    public void testUnsupportedGeometryIsRejected() {
        Geometry collection = factory.createGeometryCollection(
            new Geometry[] { factory.createPoint(new Coordinate(1, 1)), factory.createLineString(coords(0, 0, 2, 0)) }
        );

        // A GeometryCollection has no MVT geometry type: MVT features are points, lines or polygons.
        expectThrows(IllegalArgumentException.class, () -> MvtEncoder.encode(collection));
    }

    // ---------------------------------------------------------------------------------------------
    // Round-trip against an independent decoder
    // ---------------------------------------------------------------------------------------------

    public void testRoundTripOfAMultiPolygonWithHoles() {
        Polygon first = factory.createPolygon(
            ring(0, 0, 100, 0, 100, 100, 0, 100, 0, 0),
            new LinearRing[] { ring(10, 10, 10, 20, 20, 20, 20, 10, 10, 10), ring(40, 40, 40, 60, 60, 60, 60, 40, 40, 40) }
        );
        Polygon second = factory.createPolygon(ring(200, 200, 260, 200, 260, 260, 200, 260, 200, 200));
        Geometry multi = factory.createMultiPolygon(new Polygon[] { first, second });

        List<List<int[]>> parts = decode(MvtEncoder.encode(multi));

        assertEquals("one part per ring", 4, parts.size());
        assertRingMatches(first.getExteriorRing(), parts.get(0));
        assertRingMatches(first.getInteriorRingN(0), parts.get(1));
        assertRingMatches(first.getInteriorRingN(1), parts.get(2));
        assertRingMatches(second.getExteriorRing(), parts.get(3));
    }

    public void testRoundTripOfARandomPolygon() {
        // A convex ring built on a circle: distinct grid points, no self-intersection, arbitrary sizes.
        int points = randomIntBetween(4, 60);
        int radius = randomIntBetween(200, 2000);
        List<Coordinate> coordinates = new ArrayList<>();
        for (int i = 0; i < points; i++) {
            double angle = 2 * Math.PI * i / points;
            coordinates.add(new Coordinate(Math.round(2048 + radius * Math.cos(angle)), Math.round(2048 + radius * Math.sin(angle))));
        }
        coordinates.add(coordinates.get(0));
        LinearRing shell = factory.createLinearRing(coordinates.toArray(new Coordinate[0]));

        List<List<int[]>> parts = decode(MvtEncoder.encode(factory.createPolygon(shell)));

        assertEquals(1, parts.size());
        assertRingMatches(shell, parts.get(0));
    }

    private void assertRingMatches(LinearRing expected, List<int[]> decoded) {
        assertEquals("ring " + expected + " decoded as " + flatten(decoded), expected.getNumPoints(), decoded.size());
        for (int i = 0; i < decoded.size(); i++) {
            Coordinate coordinate = expected.getCoordinateN(i);
            assertArrayEquals("point " + i, new int[] { (int) coordinate.x, (int) coordinate.y }, decoded.get(i));
        }
    }

    // ---------------------------------------------------------------------------------------------
    // An MVT reader, written from the spec
    // ---------------------------------------------------------------------------------------------

    /**
     * Walk a command stream and rebuild the absolute grid coordinates the pen draws, one list per
     * MoveTo. A ClosePath appends the first point of the current part, so a decoded ring reads like
     * the closed JTS ring it came from.
     */
    private static List<List<int[]>> decode(int[] stream) {
        List<List<int[]>> parts = new ArrayList<>();
        List<int[]> current = null;
        int x = 0;
        int y = 0;
        int i = 0;

        while (i < stream.length) {
            int header = stream[i++];
            int id = header & 0x7;
            int count = header >>> 3;
            switch (id) {
                case 1 -> {
                    assertTrue("MoveTo with a zero count", count > 0);
                    for (int n = 0; n < count; n++) {
                        x += unzigzag(stream[i++]);
                        y += unzigzag(stream[i++]);
                        current = new ArrayList<>();
                        current.add(new int[] { x, y });
                        parts.add(current);
                    }
                }
                case 2 -> {
                    assertNotNull("LineTo before any MoveTo", current);
                    assertTrue("LineTo with a zero count", count > 0);
                    for (int n = 0; n < count; n++) {
                        x += unzigzag(stream[i++]);
                        y += unzigzag(stream[i++]);
                        current.add(new int[] { x, y });
                    }
                }
                case 7 -> {
                    assertNotNull("ClosePath before any MoveTo", current);
                    assertEquals("ClosePath takes no parameter, so its count is 1", 1, count);
                    current.add(current.get(0));
                }
                default -> throw new AssertionError("unknown command id [" + id + "]");
            }
        }
        assertEquals("the stream ends mid-parameter", stream.length, i);
        return parts;
    }

    private static int unzigzag(int value) {
        return value >>> 1 ^ -(value & 1);
    }

    private static int countCommands(int[] stream, int id) {
        int found = 0;
        int i = 0;
        while (i < stream.length) {
            int header = stream[i++];
            if ((header & 0x7) == id) {
                found++;
            }
            i += (header & 0x7) == 7 ? 0 : 2 * (header >>> 3);
        }
        return found;
    }

    private static List<String> flatten(List<int[]> points) {
        return points.stream().map(point -> point[0] + "," + point[1]).toList();
    }
}
