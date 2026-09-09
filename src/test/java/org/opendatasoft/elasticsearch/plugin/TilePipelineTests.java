package org.opendatasoft.elasticsearch.plugin;

import org.elasticsearch.test.ESTestCase;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.TileParams;

/**
 * Covers the geometric primitives the tile pipeline is built from: projection, clipping, ring
 * orientation, and the sign convention they all have to agree on, plus the tile window they are
 * driven by.
 *
 * <p>The fixture tile is z=6/x=31/y=22, the one the perf investigation profiled.
 */
public class TilePipelineTests extends ESTestCase {

    private static final int EXTENT = 4096;

    // z=6/x=31/y=22 bounds, as mercantile computes them.
    private static final double TILE_MIN_LON = -5.625;
    private static final double TILE_MIN_LAT = 45.089035564831015;
    private static final double TILE_MAX_LON = 0.0;
    private static final double TILE_MAX_LAT = 48.92249926375824;

    private final GeometryFactory factory = new GeometryFactory();

    private TileParams tileParams() {
        return new TileParams(TILE_MIN_LON, TILE_MIN_LAT, TILE_MAX_LON, TILE_MAX_LAT, EXTENT, TileParams.DEFAULT_BUFFER);
    }

    private LinearRing ccwRing(double minLon, double minLat, double maxLon, double maxLat) {
        return factory.createLinearRing(
            new Coordinate[] {
                new Coordinate(minLon, minLat),
                new Coordinate(maxLon, minLat),
                new Coordinate(maxLon, maxLat),
                new Coordinate(minLon, maxLat),
                new Coordinate(minLon, minLat) }
        );
    }

    /**
     * The surveyor's formula, spelled out rather than delegated.
     *
     * <p>Deliberately not {@code org.locationtech.jts.algorithm.Area.ofRingSigned}, which returns the
     * opposite sign (positive for clockwise). The MVT spec is written in terms of this formula, and
     * this is the one place where getting the sign convention wrong is not recoverable by reasoning,
     * so it is written out in full.
     */
    private double signedArea(CoordinateSequence sequence) {
        double sum = 0;
        for (int i = 0; i < sequence.size() - 1; i++) {
            double x1 = sequence.getOrdinate(i, CoordinateSequence.X);
            double y1 = sequence.getOrdinate(i, CoordinateSequence.Y);
            double x2 = sequence.getOrdinate(i + 1, CoordinateSequence.X);
            double y2 = sequence.getOrdinate(i + 1, CoordinateSequence.Y);
            sum += x1 * y2 - x2 * y1;
        }
        return sum / 2;
    }

    /**
     * Pins the sign convention itself, against a ring whose vertex order can be checked by hand.
     * Everything built on these primitives leans on this being right.
     */
    public void testSignConventionInAYDownGrid() {
        // Top-left, top-right, bottom-right, bottom-left: clockwise as seen on screen when y grows
        // downwards.
        LinearRing visuallyClockwise = factory.createLinearRing(
            new Coordinate[] {
                new Coordinate(0, 0),
                new Coordinate(10, 0),
                new Coordinate(10, 10),
                new Coordinate(0, 10),
                new Coordinate(0, 0) }
        );

        assertEquals(
            "a visually clockwise y-down ring has a positive area",
            100.0,
            signedArea(visuallyClockwise.getCoordinateSequence()),
            0.0
        );
        assertTrue(
            "and JTS reports that same ring as CCW, because isCCW reads raw ordinates",
            Orientation.isCCW(visuallyClockwise.getCoordinateSequence())
        );
    }

    public void testMercatorMatchesMercantile() {
        // Reference values straight out of python's mercantile.xy.
        assertEquals(261848.1441407865, GeoUtils.lonToMercatorX(2.3522219), 1e-7);
        assertEquals(6250566.718238154, GeoUtils.latToMercatorY(48.856614), 1e-7);
        assertEquals(-376910.9864742041, GeoUtils.lonToMercatorX(-3.3858489990234375), 1e-7);
        assertEquals(6064418.277860527, GeoUtils.latToMercatorY(47.7442871774986), 1e-7);
        assertEquals(0.0, GeoUtils.lonToMercatorX(0), 1e-7);
        assertEquals(0.0, GeoUtils.latToMercatorY(0), 1e-7);
    }

    public void testToWebMercatorMutatesInPlace() {
        Point point = factory.createPoint(new Coordinate(2.3522219, 48.856614));
        GeoUtils.toWebMercator(point);
        assertEquals(261848.1441407865, point.getX(), 1e-7);
        assertEquals(6250566.718238154, point.getY(), 1e-7);
    }

    /**
     * At lat -90 the tangent is exactly zero, so an unguarded logarithm returns -Infinity, which
     * would make any envelope built from it infinitely tall.
     */
    public void testPolesDoNotProduceInfiniteMercator() {
        assertTrue("lat -90 must project to a finite value", Double.isFinite(GeoUtils.latToMercatorY(-90)));
        assertTrue("lat 90 must project to a finite value", Double.isFinite(GeoUtils.latToMercatorY(90)));
    }

    /**
     * An intersection does not preserve dimension: a polygon merely tangent to the window intersects
     * it along a line, or at a point. Both are valid and non-empty, so nothing downstream would catch
     * them; they must be discarded at the clip.
     */
    public void testTangentShapesClipToEmptyRatherThanToALine() {
        Envelope window = new Envelope(0, 10, 0, 10);

        // Edge flush against the eastern border of the window.
        Geometry edgeFlush = GeoUtils.clipToBbox(factory.createPolygon(ccwRing(10, 2, 20, 8)), window);
        assertTrue("an edge-tangent polygon must clip to empty, got " + edgeFlush, edgeFlush.isEmpty());
        assertEquals("and must keep the input dimension", 2, edgeFlush.getDimension());

        // Single corner touching the north-east corner of the window.
        Geometry cornerTouch = GeoUtils.clipToBbox(factory.createPolygon(ccwRing(10, 10, 20, 20)), window);
        assertTrue("a corner-tangent polygon must clip to empty, got " + cornerTouch, cornerTouch.isEmpty());
    }

    public void testOverlappingShapesAreStillClippedNormally() {
        Envelope window = new Envelope(0, 10, 0, 10);
        Geometry clipped = GeoUtils.clipToBbox(factory.createPolygon(ccwRing(5, 5, 15, 15)), window);

        assertFalse(clipped.isEmpty());
        assertEquals("a real overlap must stay areal", 2, clipped.getDimension());
        assertEquals("and keep its area", 25.0, clipped.getArea(), 1e-9);
    }

    /** A stored LineString is legitimately one-dimensional, so clipping must not empty it. */
    public void testLinearShapesKeepTheirOwnDimension() {
        Envelope window = new Envelope(0, 10, 0, 10);
        Geometry line = factory.createLineString(new Coordinate[] { new Coordinate(5, 5), new Coordinate(15, 5) });

        Geometry clipped = GeoUtils.clipToBbox(line, window);

        assertFalse("a line crossing the window must survive", clipped.isEmpty());
        assertEquals(1, clipped.getDimension());
        assertEquals("clipped at the window border", 5.0, clipped.getLength(), 1e-9);
    }

    public void testOrientRingsLeavesPointsAndLinesAlone() {
        Point point = factory.createPoint(new Coordinate(-3.0, 47.0));
        GeoUtils.orientRings(point);
        assertEquals(-3.0, point.getX(), 0.0);

        Geometry line = factory.createLineString(new Coordinate[] { new Coordinate(-4.0, 46.0), new Coordinate(-2.0, 48.0) });
        GeoUtils.orientRings(line);
        CoordinateSequence sequence = ((org.locationtech.jts.geom.LineString) line).getCoordinateSequence();
        assertEquals("line direction must be preserved", -4.0, sequence.getOrdinate(0, CoordinateSequence.X), 0.0);
    }

    private LinearRing cwRing(double minLon, double minLat, double maxLon, double maxLat) {
        return (LinearRing) ccwRing(minLon, minLat, maxLon, maxLat).reverse();
    }

    /** A polygon fully inside the tile, with a hole, wound per RFC 7946. */
    private Polygon insideTileWithHole() {
        return factory.createPolygon(ccwRing(-4.0, 46.0, -2.0, 48.0), new LinearRing[] { cwRing(-3.5, 46.5, -2.5, 47.5) });
    }

    public void testGridOriginIsTopLeftCorner() {
        TileParams tile = tileParams();

        Point topLeft = factory.createPoint(new Coordinate(TILE_MIN_LON, TILE_MAX_LAT));
        GeoUtils.toTileGrid(topLeft, tile.mercatorEnvelope(), EXTENT);
        assertEquals("top-left x", 0.0, topLeft.getX(), 0.0);
        assertEquals("top-left y", 0.0, topLeft.getY(), 0.0);

        Point bottomRight = factory.createPoint(new Coordinate(TILE_MAX_LON, TILE_MIN_LAT));
        GeoUtils.toTileGrid(bottomRight, tile.mercatorEnvelope(), EXTENT);
        assertEquals("bottom-right x", (double) EXTENT, bottomRight.getX(), 0.0);
        assertEquals("bottom-right y", (double) EXTENT, bottomRight.getY(), 0.0);
    }

    public void testBufferFallsOutsideTheExtent() {
        TileParams tile = tileParams();
        // Just west of the tile, inside the 6.25% buffer.
        Point point = factory.createPoint(new Coordinate(TILE_MIN_LON - 0.1, TILE_MAX_LAT - 0.1));
        assertTrue("point should survive the clip window", tile.clipEnvelope().contains(point.getCoordinate()));

        GeoUtils.toTileGrid(point, tile.mercatorEnvelope(), EXTENT);
        assertTrue("a point in the buffer must land outside [0, extent]", point.getX() < 0);
    }

    public void testWorldWideBboxStillQuantizesOntoTheGrid() {
        TileParams world = new TileParams(-180, -90, 180, 90, EXTENT, 0);
        Envelope mercator = world.mercatorEnvelope();
        assertTrue("envelope height must be finite", Double.isFinite(mercator.getHeight()));
        assertTrue("envelope height must not be zero", mercator.getHeight() > 0);

        Point point = factory.createPoint(new Coordinate(0, 0));
        GeoUtils.toTileGrid(point, mercator, EXTENT);
        assertTrue("x must land on the grid, got " + point.getX(), point.getX() >= 0 && point.getX() <= EXTENT);
        assertTrue("y must land on the grid, got " + point.getY(), point.getY() >= 0 && point.getY() <= EXTENT);
    }

    public void testClipKeepsAShapeAlreadyInsideUntouched() {
        Polygon polygon = insideTileWithHole();
        Geometry clipped = GeoUtils.clipToBbox(polygon, tileParams().clipEnvelope());
        assertSame("a shape fully inside must not go through an overlay", polygon, clipped);
    }

    public void testClipEmptiesAShapeOutsideTheWindow() {
        Polygon faraway = factory.createPolygon(ccwRing(20.0, 20.0, 21.0, 21.0));
        Geometry clipped = GeoUtils.clipToBbox(faraway, tileParams().clipEnvelope());
        assertTrue("a shape outside the window must clip to empty", clipped.isEmpty());
    }

    /**
     * Latitude is not cyclic, so both orderings describe the same band and both are accepted. This is
     * what lets a caller pass mercantile's (west, south, east, north) or elasticsearch's envelope
     * ordering (west, north, east, south) interchangeably.
     */
    public void testEitherLatitudeOrderIsAccepted() {
        TileParams southFirst = tileParams();
        TileParams northFirst = new TileParams(TILE_MIN_LON, TILE_MAX_LAT, TILE_MAX_LON, TILE_MIN_LAT, EXTENT, TileParams.DEFAULT_BUFFER);

        assertEquals(southFirst.mercatorEnvelope(), northFirst.mercatorEnvelope());
        assertEquals(southFirst.clipEnvelope(), northFirst.clipEnvelope());
    }

    /**
     * Longitude is cyclic, so a decreasing pair is ambiguous: [170, -170] could be the 20 degree
     * strip across the antimeridian or the 340 degree band the other way. Rejected rather than
     * guessed at.
     */
    public void testDecreasingLongitudeIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new TileParams(170, 40, -170, 50, EXTENT, TileParams.DEFAULT_BUFFER)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("antimeridian"));
    }

    public void testCoordinatesOutsideTheirRangeAreRejected() {
        expectThrows(IllegalArgumentException.class, () -> new TileParams(-181, 40, 10, 50, EXTENT, 0));
        expectThrows(IllegalArgumentException.class, () -> new TileParams(0, 40, 181, 50, EXTENT, 0));
        expectThrows(IllegalArgumentException.class, () -> new TileParams(0, -91, 10, 50, EXTENT, 0));
        expectThrows(IllegalArgumentException.class, () -> new TileParams(0, 40, 10, 91, EXTENT, 0));
    }

    /** validate() must guard every entry point, not just the JSON parser. */
    public void testDegenerateBboxIsRejectedByTheConstructor() {
        IllegalArgumentException sameLon = expectThrows(IllegalArgumentException.class, () -> new TileParams(5, 45, 5, 48, EXTENT, 0));
        assertTrue(sameLon.getMessage(), sameLon.getMessage().contains("degenerate"));

        expectThrows(IllegalArgumentException.class, () -> new TileParams(0, 45, 5, 45, EXTENT, 0));
    }
}
