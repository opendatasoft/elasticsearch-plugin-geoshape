package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;
import org.opendatasoft.elasticsearch.plugin.GeoUtils;

/**
 * The geometric pipeline applied to each of the shapes an aggregation returns: clip, simplify,
 * reproject, repair, orient. Built once per aggregator, so the envelopes are derived once per request.
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

    /** True when the stored WKB is returned as it is, so the caller can skip parsing it. */
    public boolean isNoop() {
        return mustSimplify == false && tile == null;
    }

    /**
     * Whether any part of this shape can survive the clip. An envelope test only: it can let through
     * a shape the clip later empties, which is fine since it only decides who competes for a slot.
     */
    public boolean intersectsWindow(Geometry geom) {
        return clipEnvelope == null || clipEnvelope.intersects(geom.getEnvelopeInternal());
    }

    /**
     * Run the pipeline.
     *
     * <p>Orienting comes <b>last</b>, on the coordinates that are actually emitted. Ring orientation
     * is a property of the coordinate values, so quantizing to the y-down grid flips it. The repair
     * sits just before it, because rebuilding rings picks a fresh winding.
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
            geom = GeoUtils.makeValid(geom, tile.hasExtent());
            GeoUtils.orientRings(geom);
        }

        return geom;
    }

    private Geometry simplify(Geometry geom) {
        Geometry simplified = simplifyWith(geom);
        // Under a tile, an emptied shape is dropped like one the clip emptied: a point standing in for
        // a polygon would break the guarantee that the returned geometry keeps the stored dimension.
        if (simplified.isEmpty() && tile == null) {
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
