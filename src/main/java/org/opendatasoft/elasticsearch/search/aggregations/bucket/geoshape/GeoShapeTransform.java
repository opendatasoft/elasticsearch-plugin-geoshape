package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;
import org.opendatasoft.elasticsearch.plugin.GeoUtils;

/**
 * The geometric pipeline applied to each of the shapes an aggregation returns: clip, simplify,
 * reproject, repair, orient.
 *
 * <p>This lives in its own class so there is exactly one implementation of the step order. It used to
 * be a private method on the aggregator with hand-written copies in the tests, and those copies
 * drifted: the ordering the pipeline is most sensitive to was asserted against a copy rather than
 * against the real thing.
 *
 * <p>Built once per aggregator, so the derived envelopes are computed once per request rather than
 * once per shape.
 */
public class GeoShapeTransform {

    private final boolean mustSimplify;
    private final int zoom;
    private final GeoShape.Algorithm algorithm;
    private final TileParams tile;
    private final Envelope clipEnvelope;
    private final Envelope mercatorEnvelope;
    private final GeometryFactory geometryFactory = new GeometryFactory();

    public GeoShapeTransform(boolean mustSimplify, int zoom, GeoShape.Algorithm algorithm, TileParams tile) {
        this.mustSimplify = mustSimplify;
        this.zoom = zoom;
        this.algorithm = algorithm;
        this.tile = tile;
        this.clipEnvelope = tile == null ? null : tile.clipEnvelope();
        this.mercatorEnvelope = tile == null ? null : tile.mercatorEnvelope();
    }

    /**
     * True when there is nothing to compute and the stored WKB is what gets returned. Lets the caller
     * skip parsing the WKB altogether on the default path.
     */
    public boolean isNoop() {
        return mustSimplify == false && tile == null;
    }

    /**
     * Whether any part of this shape can survive the clip.
     *
     * <p>An envelope comparison, so callers holding a parsed geometry can ask this for free. Used to
     * keep shapes that would be clipped away entirely out of the top-N ranking: they would otherwise
     * take a {@code size} slot and then be dropped, hiding shapes that are actually visible.
     *
     * <p>Envelopes overlapping is necessary but not sufficient, so this can still let through a shape
     * the clip later empties. That is fine: it only decides who competes for a slot.
     */
    public boolean intersectsWindow(Geometry geom) {
        return clipEnvelope == null || clipEnvelope.intersects(geom.getEnvelopeInternal());
    }

    /**
     * True when the returned geometry no longer measures the whole shape, so its length must not be
     * used as a ranking key.
     */
    public boolean rescalesGeometry() {
        return tile != null;
    }

    /**
     * Run the pipeline.
     *
     * <p>Clipping comes first so Douglas-Peucker only ever sees the part of the shape that can end up
     * visible, which is what makes a border spanning many tiles cheap to serve.
     *
     * <p>Orienting comes <b>last</b>, on the coordinates that are actually emitted. Ring orientation
     * is a property of the coordinate values, so quantizing to the y-down grid flips it; deciding it
     * beforehand and reasoning about how the flip would land is exactly how this shipped inverted the
     * first time. The repair sits just before it, because rebuilding rings picks a fresh winding.
     *
     * @return the transformed geometry, empty when nothing of the shape survived
     */
    public Geometry apply(Geometry geom) {
        if (clipEnvelope != null) {
            geom = GeoUtils.clipToBbox(geom, clipEnvelope);
            if (geom.isEmpty()) {
                return geom;
            }
        }

        if (mustSimplify) {
            geom = simplify(geom);
        }

        if (tile != null) {
            if (tile.hasExtent()) {
                GeoUtils.toTileGrid(geom, mercatorEnvelope, tile.getExtent());
            } else {
                GeoUtils.toWebMercator(geom);
            }
            geom = GeoUtils.fixCollapsedGeometry(geom);
            GeoUtils.orientRings(geom);
        }

        return geom;
    }

    private Geometry simplify(Geometry geom) {
        Geometry simplified = simplifyWith(geom);
        if (simplified.isEmpty()) {
            simplified = geometryFactory.createPoint(geom.getCoordinate());
        }
        return simplified;
    }

    private Geometry simplifyWith(Geometry geometry) {
        double tol = GeoUtils.getToleranceFromZoom(zoom);

        switch (algorithm) {
            case TOPOLOGY_PRESERVING:
                return TopologyPreservingSimplifier.simplify(geometry, tol);
            default:
                return DouglasPeuckerSimplifier.simplify(geometry, tol);
        }
    }
}
