package org.opendatasoft.elasticsearch.plugin;

import org.elasticsearch.test.ESTestCase;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;

/**
 * Establishes, empirically, the two assumptions the tile pipeline relies on for ring orientation:
 * <ol>
 *     <li>the simplifiers never reverse a ring's vertex traversal direction;</li>
 *     <li>{@code intersection()} (the clip step) emits a known, stable orientation.</li>
 * </ol>
 * Ingest normalizes every stored shape to {@code Orientation.RIGHT} (exterior CCW, RFC 7946), so
 * knowing what these two steps do to that orientation is what tells us whether an explicit orient
 * step is needed after quantization.
 */
public class RingOrientationTests extends ESTestCase {

    private final GeometryFactory factory = new GeometryFactory();

    /** A CCW exterior ring (right-hand rule, i.e. what ingest stores), with enough points to be simplifiable. */
    private Polygon ccwPolygonWithHole() {
        LinearRing shell = factory.createLinearRing(
            new Coordinate[] {
                new Coordinate(0, 0),
                new Coordinate(5, 0.01),
                new Coordinate(10, 0),
                new Coordinate(10, 10),
                new Coordinate(5, 9.99),
                new Coordinate(0, 10),
                new Coordinate(0, 0) }
        );
        // hole wound the other way (CW), as RFC 7946 requires
        LinearRing hole = factory.createLinearRing(
            new Coordinate[] {
                new Coordinate(4, 4),
                new Coordinate(4, 6),
                new Coordinate(6, 6),
                new Coordinate(6, 4),
                new Coordinate(4, 4) }
        );
        return factory.createPolygon(shell, new LinearRing[] { hole });
    }

    public void testFixtureMatchesIngestOrientation() {
        Polygon p = ccwPolygonWithHole();
        assertTrue("fixture shell must be CCW", Orientation.isCCW(p.getExteriorRing().getCoordinateSequence()));
        assertFalse("fixture hole must be CW", Orientation.isCCW(p.getInteriorRingN(0).getCoordinateSequence()));
    }

    public void testDouglasPeuckerPreservesOrientation() {
        Geometry simplified = DouglasPeuckerSimplifier.simplify(ccwPolygonWithHole(), 0.5);
        assertFalse("simplification dropped the shape entirely", simplified.isEmpty());
        Polygon p = (Polygon) simplified;
        assertTrue("DouglasPeucker must not reverse the shell", Orientation.isCCW(p.getExteriorRing().getCoordinateSequence()));
        if (p.getNumInteriorRing() > 0) {
            assertFalse("DouglasPeucker must not reverse holes", Orientation.isCCW(p.getInteriorRingN(0).getCoordinateSequence()));
        }
    }

    public void testTopologyPreservingPreservesOrientation() {
        Geometry simplified = TopologyPreservingSimplifier.simplify(ccwPolygonWithHole(), 0.5);
        assertFalse("simplification dropped the shape entirely", simplified.isEmpty());
        Polygon p = (Polygon) simplified;
        assertTrue("TopologyPreserving must not reverse the shell", Orientation.isCCW(p.getExteriorRing().getCoordinateSequence()));
        if (p.getNumInteriorRing() > 0) {
            assertFalse("TopologyPreserving must not reverse holes", Orientation.isCCW(p.getInteriorRingN(0).getCoordinateSequence()));
        }
    }

    /**
     * The one that actually matters, and the reason {@link GeoUtils#orientRings} is not optional.
     *
     * <p>JTS overlay operations rebuild their output rings from scratch and emit them in the JTS
     * convention: clockwise shells, counter-clockwise holes. That is the <b>opposite</b> of what went
     * in. So a shape big enough to be clipped comes out wound the other way from a shape small enough
     * that {@link GeoUtils#clipToBbox} hands it back untouched, and no fixed flip can reconcile the
     * two. Re-establishing the convention explicitly is what makes the output orientation independent
     * of the path a shape took.
     *
     * <p>This is a characterization test: it pins the JTS behaviour we observed rather than a
     * behaviour we want. If a JTS upgrade flips it, this test says so.
     */
    public void testIntersectionReversesTheInputOrientation() {
        Geometry clipWindow = factory.toGeometry(new org.locationtech.jts.geom.Envelope(-1, 7, -1, 7));
        Geometry clipped = ccwPolygonWithHole().intersection(clipWindow);
        assertFalse("clip emptied the shape", clipped.isEmpty());

        for (int i = 0; i < clipped.getNumGeometries(); i++) {
            Polygon p = (Polygon) clipped.getGeometryN(i);
            assertFalse(
                "intersection() is expected to emit clockwise shells",
                Orientation.isCCW(p.getExteriorRing().getCoordinateSequence())
            );
            for (int h = 0; h < p.getNumInteriorRing(); h++) {
                assertTrue(
                    "intersection() is expected to emit counter-clockwise holes",
                    Orientation.isCCW(p.getInteriorRingN(h).getCoordinateSequence())
                );
            }
        }
    }

    /**
     * And once oriented, a clipped shape is back in the ingest convention regardless.
     */
    public void testOrientRingsRestoresTheRightHandRuleAfterClipping() {
        Geometry clipWindow = factory.toGeometry(new org.locationtech.jts.geom.Envelope(-1, 7, -1, 7));
        Geometry clipped = ccwPolygonWithHole().intersection(clipWindow);

        GeoUtils.orientRings(clipped);

        for (int i = 0; i < clipped.getNumGeometries(); i++) {
            Polygon p = (Polygon) clipped.getGeometryN(i);
            assertTrue("shell must be CCW again", Orientation.isCCW(p.getExteriorRing().getCoordinateSequence()));
            for (int h = 0; h < p.getNumInteriorRing(); h++) {
                assertFalse("holes must be CW again", Orientation.isCCW(p.getInteriorRingN(h).getCoordinateSequence()));
            }
        }
    }
}
