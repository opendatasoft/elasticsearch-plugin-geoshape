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
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.GeoShape;
import org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.GeoShapeTransform;
import org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.TileParams;

/**
 * Covers the clip -> simplify -> reproject/quantize -> repair -> orient pipeline.
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

    /**
     * The production pipeline itself, not a copy of it. An earlier version of this file reimplemented
     * the step order by hand and silently omitted the repair step, which meant the ordering these
     * tests exist to pin was never actually exercised.
     */
    private Geometry runPipeline(Geometry geom, TileParams tile, boolean simplify) {
        return new GeoShapeTransform(simplify, 6, GeoShape.Algorithm.DOUGLAS_PEUCKER, tile).apply(geom);
    }

    /**
     * A ring wound counter-clockwise, i.e. what the ingest processor stores after normalizing to
     * Orientation.RIGHT.
     */
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

    private LinearRing cwRing(double minLon, double minLat, double maxLon, double maxLat) {
        return (LinearRing) ccwRing(minLon, minLat, maxLon, maxLat).reverse();
    }

    /** A polygon fully inside the tile, with a hole, wound per RFC 7946. */
    private Polygon insideTileWithHole() {
        return factory.createPolygon(ccwRing(-4.0, 46.0, -2.0, 48.0), new LinearRing[] { cwRing(-3.5, 46.5, -2.5, 47.5) });
    }

    /** The same shape, but stretched well past the tile's eastern and southern edges. */
    private Polygon straddlingTileWithHole() {
        return factory.createPolygon(ccwRing(-4.0, 40.0, 5.0, 48.0), new LinearRing[] { cwRing(-3.5, 46.5, -2.5, 47.5) });
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
     * MVT spec 4.3.3.3: exterior rings MUST have a <b>positive</b> area under the surveyor's formula
     * applied to tile coordinates, interior rings a negative one.
     *
     * <p>In the y-down tile grid a positive area is what reads as clockwise on screen. That double
     * inversion (the axis flips, and so does the relationship between the formula's sign and what
     * the eye sees) is what an earlier version of this pipeline got wrong, by orienting rings before
     * the flip instead of after it.
     */
    private void assertMvtWinding(Geometry geom, String what) {
        assertFalse(what + ": expected a non-empty geometry", geom.isEmpty());
        int polygons = 0;
        for (int i = 0; i < geom.getNumGeometries(); i++) {
            if (geom.getGeometryN(i) instanceof Polygon polygon) {
                polygons++;
                double exteriorArea = signedArea(polygon.getExteriorRing().getCoordinateSequence());
                assertTrue(what + ": exterior ring must have a positive area in the tile grid, got " + exteriorArea, exteriorArea > 0);
                for (int h = 0; h < polygon.getNumInteriorRing(); h++) {
                    double holeArea = signedArea(polygon.getInteriorRingN(h).getCoordinateSequence());
                    assertTrue(what + ": hole must have a negative area in the tile grid, got " + holeArea, holeArea < 0);
                }
            }
        }
        assertTrue(what + ": expected at least one polygon", polygons > 0);
    }

    /**
     * Pins the sign convention itself, against a ring whose vertex order can be checked by hand.
     * Everything else in this file leans on this being right.
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

    /**
     * The clip path is the one that reverses orientation, so it has to be covered together with
     * holes. Asserting the hole is still there first: without that, this test would keep passing if
     * the clip ever swallowed it, and would prove nothing about hole winding.
     */
    public void testClippedPolygonKeepsItsHoleAndItsWinding() {
        Geometry geom = runPipeline(straddlingTileWithHole(), tileParams(), false);

        int holes = 0;
        for (int i = 0; i < geom.getNumGeometries(); i++) {
            if (geom.getGeometryN(i) instanceof Polygon polygon) {
                holes += polygon.getNumInteriorRing();
            }
        }
        assertTrue("fixture must still carry a hole after clipping for this test to mean anything", holes > 0);

        assertMvtWinding(geom, "clipped polygon with a hole");
    }

    /**
     * At lat -90 the tangent is exactly zero, so an unguarded logarithm returns -Infinity, which
     * makes the tile envelope infinitely tall and silently flattens every y coordinate onto 0.
     */
    public void testPolesDoNotProduceInfiniteMercator() {
        assertTrue("lat -90 must project to a finite value", Double.isFinite(GeoUtils.latToMercatorY(-90)));
        assertTrue("lat 90 must project to a finite value", Double.isFinite(GeoUtils.latToMercatorY(90)));
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

    /** validate() must guard every entry point, not just the JSON parser. */
    public void testDegenerateBboxIsRejectedByTheConstructor() {
        IllegalArgumentException sameLon = expectThrows(IllegalArgumentException.class, () -> new TileParams(5, 45, 5, 48, EXTENT, 0));
        assertTrue(sameLon.getMessage(), sameLon.getMessage().contains("degenerate"));

        expectThrows(IllegalArgumentException.class, () -> new TileParams(0, 45, 5, 45, EXTENT, 0));
    }

    /**
     * The envelope pre-filter that keeps invisible shapes out of the top-N ranking.
     */
    public void testIntersectsWindow() {
        GeoShapeTransform transform = new GeoShapeTransform(false, 6, GeoShape.Algorithm.DOUGLAS_PEUCKER, tileParams());

        assertTrue("a shape inside the tile competes", transform.intersectsWindow(insideTileWithHole()));
        assertTrue("a shape straddling the tile competes", transform.intersectsWindow(straddlingTileWithHole()));
        assertFalse(
            "a shape nowhere near the tile must not take a slot",
            transform.intersectsWindow(factory.createPolygon(ccwRing(20.0, 20.0, 21.0, 21.0)))
        );

        // The buffer is part of the window, so a shape that only reaches the margin still competes.
        double bufferLon = TileParams.DEFAULT_BUFFER * (TILE_MAX_LON - TILE_MIN_LON);
        Geometry inBufferOnly = factory.createPolygon(ccwRing(TILE_MIN_LON - bufferLon / 2, 46.0, TILE_MIN_LON - bufferLon / 4, 47.0));
        assertTrue("a shape in the buffer competes", transform.intersectsWindow(inBufferOnly));
    }

    public void testEverythingCompetesWhenNoTileIsRequested() {
        GeoShapeTransform transform = new GeoShapeTransform(true, 6, GeoShape.Algorithm.DOUGLAS_PEUCKER, null);
        assertTrue(transform.intersectsWindow(factory.createPolygon(ccwRing(20.0, 20.0, 21.0, 21.0))));
    }

    /**
     * An intersection does not preserve dimension: a polygon merely tangent to the window intersects
     * it along a line, or at a point. Both are valid and non-empty, so they slip past the validity
     * repair and the empty check; they must be discarded at the clip.
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

    public void testGridCoordinatesAreIntegers() {
        Geometry geom = runPipeline(insideTileWithHole(), tileParams(), true);
        for (Coordinate coordinate : geom.getCoordinates()) {
            assertEquals("x must be quantized", Math.rint(coordinate.x), coordinate.x, 0.0);
            assertEquals("y must be quantized", Math.rint(coordinate.y), coordinate.y, 0.0);
        }
    }

    public void testBufferFallsOutsideTheExtent() {
        TileParams tile = tileParams();
        // Just west of the tile, inside the 6.25% buffer.
        Point point = factory.createPoint(new Coordinate(TILE_MIN_LON - 0.1, TILE_MAX_LAT - 0.1));
        assertTrue("point should survive the clip window", tile.clipEnvelope().contains(point.getCoordinate()));

        GeoUtils.toTileGrid(point, tile.mercatorEnvelope(), EXTENT);
        assertTrue("a point in the buffer must land outside [0, extent]", point.getX() < 0);
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

    public void testClipCutsAStraddlingShapeDownToTheWindow() {
        TileParams tile = tileParams();
        Envelope window = tile.clipEnvelope();
        Geometry clipped = GeoUtils.clipToBbox(straddlingTileWithHole(), window);

        assertFalse(clipped.isEmpty());
        assertTrue("the clipped shape must not extend past the window", window.contains(clipped.getEnvelopeInternal()));
        assertTrue("clipping must shrink the shape", clipped.getArea() < straddlingTileWithHole().getArea());
    }

    /**
     * The reason {@link GeoUtils#orientRings} exists: intersection() emits clockwise shells, so
     * without it a clipped shape and an untouched one would come out wound opposite ways.
     */
    public void testWindingIsIndependentOfWhetherTheShapeWasClipped() {
        assertMvtWinding(runPipeline(insideTileWithHole(), tileParams(), true), "shape inside the tile");
        assertMvtWinding(runPipeline(straddlingTileWithHole(), tileParams(), true), "shape straddling the tile");
    }

    public void testWindingHoldsWithoutSimplification() {
        assertMvtWinding(runPipeline(insideTileWithHole(), tileParams(), false), "unsimplified shape inside the tile");
        assertMvtWinding(runPipeline(straddlingTileWithHole(), tileParams(), false), "unsimplified straddling shape");
    }

    /** Input wound the wrong way must still come out in the MVT convention. */
    public void testWindingIsFixedForMisorientedInput() {
        Polygon reversed = factory.createPolygon(cwRing(-4.0, 46.0, -2.0, 48.0), new LinearRing[] { ccwRing(-3.5, 46.5, -2.5, 47.5) });
        assertMvtWinding(runPipeline(reversed, tileParams(), false), "shape wound the wrong way");
    }

    public void testMultiPolygonWindingIsFixed() {
        Geometry multiPolygon = factory.createMultiPolygon(
            new Polygon[] { factory.createPolygon(ccwRing(-4.0, 46.0, -3.0, 47.0)), factory.createPolygon(cwRing(-2.5, 47.5, -1.5, 48.5)) }
        );
        assertMvtWinding(runPipeline(multiPolygon, tileParams(), false), "multipolygon");
    }

    public void testWithoutExtentShapesStayInMercatorMeters() {
        TileParams tile = new TileParams(
            TILE_MIN_LON,
            TILE_MIN_LAT,
            TILE_MAX_LON,
            TILE_MAX_LAT,
            TileParams.NO_EXTENT,
            TileParams.DEFAULT_BUFFER
        );
        assertFalse("extent must be optional", tile.hasExtent());

        Geometry geom = runPipeline(insideTileWithHole(), tile, false);
        assertFalse(geom.isEmpty());
        // Mercator meters, not a 0-4096 grid.
        assertTrue("coordinates should be mercator meters", geom.getEnvelopeInternal().getMinX() < -100000);
        // Without the y flip the right-hand rule reads as counter-clockwise exteriors.
        Polygon polygon = (Polygon) geom;
        assertTrue(
            "without quantization the exterior stays counter-clockwise",
            Orientation.isCCW(polygon.getExteriorRing().getCoordinateSequence())
        );
    }

    public void testTileGridIsWithinExtentForAShapeInsideTheTile() {
        Geometry geom = runPipeline(insideTileWithHole(), tileParams(), false);
        for (Coordinate coordinate : geom.getCoordinates()) {
            assertTrue("x within grid: " + coordinate.x, coordinate.x >= 0 && coordinate.x <= EXTENT);
            assertTrue("y within grid: " + coordinate.y, coordinate.y >= 0 && coordinate.y <= EXTENT);
        }
    }

    /**
     * The pipeline in production never sees hand-built geometries: shapes come off the doc values
     * through WKBReader, which yields CoordinateXY sequences. Those reject an ordinate index of 2, so
     * reversing a ring through JTS' own CoordinateSequences.reverse throws on them. This exercises
     * the real path.
     */
    public void testPipelineOnGeometryReadBackFromWkb() throws ParseException {
        byte[] wkb = new WKBWriter().write(straddlingTileWithHole());
        Geometry fromWkb = new WKBReader().read(wkb);

        Geometry geom = runPipeline(fromWkb, tileParams(), true);

        assertMvtWinding(geom, "shape read back from WKB");
    }

    public void testOrientRingsOnGeometryReadBackFromWkb() throws ParseException {
        // A clockwise shell, so orientRings has to actually reverse it.
        byte[] wkb = new WKBWriter().write(factory.createPolygon(cwRing(-4.0, 46.0, -2.0, 48.0)));
        Geometry fromWkb = new WKBReader().read(wkb);

        GeoUtils.orientRings(fromWkb);

        Polygon polygon = (Polygon) fromWkb;
        assertTrue("shell must have been reversed to CCW", Orientation.isCCW(polygon.getExteriorRing().getCoordinateSequence()));
        // The ring must still be a ring: closed, and geometrically unchanged.
        assertTrue("reversing must keep the ring closed", polygon.getExteriorRing().isClosed());
        assertEquals("reversing must not change the area", 4.0, polygon.getArea(), 1e-9);
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

    /**
     * Latitude is not cyclic, so both orderings describe the same band and both are accepted. This is
     * what lets a caller pass mercantile's (west, south, east, north) or elasticsearch's envelope
     * ordering (west, north, east, south) interchangeably.
     */
    public void testEitherLatitudeOrderIsAccepted() {
        TileParams southFirst = new TileParams(TILE_MIN_LON, TILE_MIN_LAT, TILE_MAX_LON, TILE_MAX_LAT, EXTENT, TileParams.DEFAULT_BUFFER);
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

        // The poles themselves stay valid: the projection clamps rather than diverging.
        TileParams world = new TileParams(-180, -90, 180, 90, EXTENT, 0);
        assertTrue(Double.isFinite(world.mercatorEnvelope().getHeight()));
    }
}
