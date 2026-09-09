package org.opendatasoft.elasticsearch.plugin;

import org.elasticsearch.test.ESTestCase;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.GeoShape;
import org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.GeoShapeTransform;
import org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.TileParams;

/**
 * Rounding onto the integer grid is destructive: sub-pixel holes and parts land on a single point,
 * slivers flatten onto a line, and a notch narrower than one grid unit closes into a zero-width
 * spike. These assert that what comes out is always valid geometry, with the collapsed pieces gone
 * and the visible area kept.
 *
 * <p>Fixture tile z=6/x=31/y=22, where one grid unit is ~0.00137 degrees of longitude at
 * {@code extent=4096}. Every "sub-pixel" fixture below is deliberately smaller than that.
 */
public class QuantizationCollapseTests extends ESTestCase {

    private static final int EXTENT = 4096;
    private static final double TILE_MIN_LON = -5.625;
    private static final double TILE_MIN_LAT = 45.089035564831015;
    private static final double TILE_MAX_LON = 0.0;
    private static final double TILE_MAX_LAT = 48.92249926375824;

    /** Area of the main 1x1 degree fixture ring once quantized, in grid units squared. */
    private static final double MAIN_RING_AREA = 770224.0;

    private final GeometryFactory factory = new GeometryFactory();

    private TileParams tileParams() {
        return new TileParams(TILE_MIN_LON, TILE_MIN_LAT, TILE_MAX_LON, TILE_MAX_LAT, EXTENT, TileParams.DEFAULT_BUFFER);
    }

    /** The production pipeline itself, so the step order under test is the shipped one. */
    private Geometry runPipeline(Geometry geom) {
        return new GeoShapeTransform(false, 6, GeoShape.Algorithm.DOUGLAS_PEUCKER, tileParams()).apply(geom);
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

    private LinearRing cwRing(double minLon, double minLat, double maxLon, double maxLat) {
        return (LinearRing) ccwRing(minLon, minLat, maxLon, maxLat).reverse();
    }

    private double signedArea(org.locationtech.jts.geom.CoordinateSequence sequence) {
        double sum = 0;
        for (int i = 0; i < sequence.size() - 1; i++) {
            sum += sequence.getOrdinate(i, 0) * sequence.getOrdinate(i + 1, 1) - sequence.getOrdinate(i + 1, 0) * sequence.getOrdinate(
                i,
                1
            );
        }
        return sum / 2;
    }

    private int countHoles(Geometry geom) {
        int holes = 0;
        for (int i = 0; i < geom.getNumGeometries(); i++) {
            if (geom.getGeometryN(i) instanceof Polygon polygon) {
                holes += polygon.getNumInteriorRing();
            }
        }
        return holes;
    }

    public void testOneGridUnitIsSmallerThanEverySubPixelFixture() {
        double lonPerUnit = (TILE_MAX_LON - TILE_MIN_LON) / EXTENT;
        assertEquals("one grid unit, in degrees of longitude", 0.001373291015625, lonPerUnit, 1e-12);
        // The fixtures below use 0.0005 and 0.0004 degree features, both under one unit.
        assertTrue("fixtures must be sub-pixel for these tests to mean anything", 0.0005 < lonPerUnit);
    }

    public void testSubPixelHoleIsDropped() {
        Geometry geom = runPipeline(
            factory.createPolygon(ccwRing(-4.0, 46.0, -3.0, 47.0), new LinearRing[] { cwRing(-3.5, 46.5, -3.4995, 46.5005) })
        );

        assertTrue("output must be valid", geom.isValid());
        assertEquals("the collapsed hole must be gone, not kept with zero area", 0, countHoles(geom));
        assertEquals("the visible area must be untouched", MAIN_RING_AREA, geom.getArea(), 0.0);
    }

    public void testSubPixelPartIsDropped() {
        Geometry geom = runPipeline(
            factory.createMultiPolygon(
                new Polygon[] {
                    factory.createPolygon(ccwRing(-4.0, 46.0, -3.0, 47.0)),
                    factory.createPolygon(ccwRing(-2.5, 46.5, -2.4995, 46.5005)) }
            )
        );

        assertTrue("output must be valid", geom.isValid());
        assertEquals("the collapsed part must be gone", 1, geom.getNumGeometries());
        assertEquals("the visible area must be untouched", MAIN_RING_AREA, geom.getArea(), 0.0);
    }

    /** A shape carrying both a sub-pixel hole and a sub-pixel satellite part. */
    public void testSubPixelHoleAndPartAreBothDropped() {
        Geometry geom = runPipeline(
            factory.createMultiPolygon(
                new Polygon[] {
                    factory.createPolygon(ccwRing(-4.0, 46.0, -3.0, 47.0), new LinearRing[] { cwRing(-3.5, 46.5, -3.4995, 46.5005) }),
                    factory.createPolygon(ccwRing(-2.5, 46.5, -2.4995, 46.5005)) }
            )
        );

        assertTrue("output must be valid", geom.isValid());
        assertEquals("the collapsed part must be gone", 1, geom.getNumGeometries());
        assertEquals("the collapsed hole must be gone", 0, countHoles(geom));
        assertEquals("the visible area must be untouched", MAIN_RING_AREA, geom.getArea(), 0.0);
        assertFalse("a shape with something left must not come back empty", geom.isEmpty());
    }

    /**
     * The case a "drop degenerate rings" rule would miss: the ring keeps a large area and six
     * distinct points, but the two sides of a sub-pixel notch round onto each other and it touches
     * itself. Repairing keeps the area; dropping it would have lost the whole commune.
     */
    public void testSubPixelNotchIsRepairedNotDropped() {
        Geometry geom = runPipeline(
            factory.createPolygon(
                factory.createLinearRing(
                    new Coordinate[] {
                        new Coordinate(-4.0, 46.0),
                        new Coordinate(-3.0, 46.0),
                        new Coordinate(-3.0, 47.0),
                        new Coordinate(-3.5, 47.0),
                        new Coordinate(-3.5, 46.5),
                        new Coordinate(-3.5004, 46.5),
                        new Coordinate(-3.5004, 47.0),
                        new Coordinate(-4.0, 47.0),
                        new Coordinate(-4.0, 46.0) }
                )
            )
        );

        assertTrue("output must be valid", geom.isValid());
        assertFalse("the shape must survive, it is fully visible", geom.isEmpty());
        assertEquals("repair must not change the visible area", MAIN_RING_AREA, geom.getArea(), 0.0);
    }

    /** A shape that is entirely sub-pixel has nothing left to return. */
    public void testFullyCollapsedShapeComesBackEmpty() {
        Geometry geom = runPipeline(
            factory.createPolygon(
                factory.createLinearRing(
                    new Coordinate[] {
                        new Coordinate(-4.0, 46.0),
                        new Coordinate(-3.0, 46.0),
                        new Coordinate(-3.0, 46.00005),
                        new Coordinate(-3.5, 46.00003),
                        new Coordinate(-4.0, 46.00005),
                        new Coordinate(-4.0, 46.0) }
                )
            )
        );

        assertTrue("a fully collapsed shape must come back empty so the aggregator drops it", geom.isEmpty());
    }

    /**
     * The repair rebuilds rings and chooses its own winding, so the MVT convention has to still hold
     * afterwards. This is why orientRings runs after the repair, not before.
     */
    public void testWindingStillHoldsAfterRepair() {
        Geometry geom = runPipeline(
            factory.createMultiPolygon(
                new Polygon[] {
                    factory.createPolygon(ccwRing(-4.0, 46.0, -3.0, 47.0), new LinearRing[] { cwRing(-3.5, 46.5, -3.4995, 46.5005) }),
                    factory.createPolygon(ccwRing(-2.5, 46.5, -2.4995, 46.5005)) }
            )
        );

        assertTrue(geom.isValid());
        for (int i = 0; i < geom.getNumGeometries(); i++) {
            if (geom.getGeometryN(i) instanceof Polygon polygon) {
                assertTrue(
                    "exterior must keep a positive area after repair",
                    signedArea(polygon.getExteriorRing().getCoordinateSequence()) > 0
                );
                assertTrue(
                    "and JTS still reports it as CCW on raw ordinates",
                    Orientation.isCCW(polygon.getExteriorRing().getCoordinateSequence())
                );
            }
        }
    }

    /** Valid geometry must not be rebuilt: the repair only runs on what is actually broken. */
    public void testValidGeometryIsReturnedUntouched() {
        Geometry quantized = factory.createPolygon(ccwRing(-4.0, 46.0, -3.0, 47.0));
        GeoUtils.toTileGrid(quantized, tileParams().mercatorEnvelope(), EXTENT);
        assertTrue("fixture must already be valid", quantized.isValid());

        assertSame("a valid geometry must be handed back as-is", quantized, GeoUtils.fixCollapsedGeometry(quantized));
    }
}
