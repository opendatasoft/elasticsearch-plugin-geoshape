package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.locationtech.jts.geom.Envelope;
import org.opendatasoft.elasticsearch.plugin.GeoUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * The window a shape is cut down to, and the coordinate space it is delivered in.
 *
 * <p>Carries a WGS84 bounding box, a {@code buffer} widening it, and an optional {@code extent}. It
 * describes a purely geometric transform: clip to the buffered box, reproject to web mercator and,
 * when an extent is given, rescale to the integer grid {@code [0, extent]} local to the box.
 *
 * <p>The grid has its origin at the <b>top-left</b> of the box, y growing downwards, which is the
 * usual convention for tile-local pixel space.
 */
public class TileParams implements Writeable, ToXContentObject {

    static final ParseField BBOX_FIELD = new ParseField("bbox");
    static final ParseField EXTENT_FIELD = new ParseField("extent");
    static final ParseField BUFFER_FIELD = new ParseField("buffer");

    /** Fraction of the bounding box size added on each side. Matches PostGIS' 256/4096 default. */
    public static final double DEFAULT_BUFFER = 0.0625;

    /** Sentinel for "no extent given": clip and reproject, but do not quantize. */
    public static final int NO_EXTENT = 0;

    private double minLon = Double.NaN;
    private double minLat = Double.NaN;
    private double maxLon = Double.NaN;
    private double maxLat = Double.NaN;
    private int extent = NO_EXTENT;
    private double buffer = DEFAULT_BUFFER;

    private static final ObjectParser<TileParams, Void> PARSER = new ObjectParser<>("tile", TileParams::new);
    static {
        PARSER.declareDoubleArray(TileParams::setBbox, BBOX_FIELD);
        PARSER.declareInt(TileParams::setExtent, EXTENT_FIELD);
        PARSER.declareDouble(TileParams::setBuffer, BUFFER_FIELD);
    }

    private TileParams() {}

    public TileParams(double lon1, double lat1, double lon2, double lat2, int extent, double buffer) {
        setBbox(List.of(lon1, lat1, lon2, lat2));
        setExtent(extent);
        setBuffer(buffer);
        // Same guard as the parser: a degenerate bbox gives an infinite scale factor and coordinates
        // of Long.MAX_VALUE, so no entry point may skip it.
        validate();
    }

    public TileParams(StreamInput in) throws IOException {
        minLon = in.readDouble();
        minLat = in.readDouble();
        maxLon = in.readDouble();
        maxLat = in.readDouble();
        extent = in.readInt();
        buffer = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(minLon);
        out.writeDouble(minLat);
        out.writeDouble(maxLon);
        out.writeDouble(maxLat);
        out.writeInt(extent);
        out.writeDouble(buffer);
    }

    static TileParams parse(XContentParser parser) throws IOException {
        TileParams tileParams = PARSER.parse(parser, null);
        tileParams.validate();
        return tileParams;
    }

    /**
     * Read the bounding box, rejecting what cannot be interpreted.
     *
     * <p>The two axes are treated differently, because they do not have the same topology:
     * <ul>
     *   <li><b>longitude is cyclic</b>, so {@code [170, -170]} could mean either the 20 degree strip
     *       across the antimeridian or the 340 degree band the other way round, and normalizing to
     *       min/max silently picks one. There is nothing to infer here, so a decreasing longitude is
     *       rejected instead of guessed at. A tile never crosses the antimeridian, so this costs
     *       callers nothing;</li>
     *   <li><b>latitude is not cyclic</b>, so {@code [45, 40]} and {@code [40, 45]} describe the same
     *       band with no ambiguity. Both orders are accepted, which lets a caller pass either
     *       mercantile's {@code (west, south, east, north)} or elasticsearch's envelope ordering
     *       {@code (west, north, east, south)}.</li>
     * </ul>
     */
    private void setBbox(List<Double> bbox) {
        if (bbox.size() != 4) {
            throw new IllegalArgumentException(
                "[" + BBOX_FIELD.getPreferredName() + "] must hold exactly 4 values [lon1, lat1, lon2, lat2] in geoshape aggregation."
            );
        }

        double lon1 = bbox.get(0);
        double lat1 = bbox.get(1);
        double lon2 = bbox.get(2);
        double lat2 = bbox.get(3);

        requireInRange(lon1, 180, "longitude");
        requireInRange(lon2, 180, "longitude");
        requireInRange(lat1, 90, "latitude");
        requireInRange(lat2, 90, "latitude");

        if (lon1 > lon2) {
            throw new IllegalArgumentException(
                "["
                    + BBOX_FIELD.getPreferredName()
                    + "] must have an increasing longitude, got ["
                    + lon1
                    + "] then ["
                    + lon2
                    + "] in geoshape aggregation. A box crossing the antimeridian cannot be expressed: "
                    + "split it into two requests."
            );
        }

        minLon = lon1;
        maxLon = lon2;
        // Latitude carries no such ambiguity, so either order is fine.
        minLat = Math.min(lat1, lat2);
        maxLat = Math.max(lat1, lat2);
    }

    private static void requireInRange(double value, double limit, String what) {
        if (Double.isNaN(value) || Math.abs(value) > limit) {
            throw new IllegalArgumentException(
                "["
                    + BBOX_FIELD.getPreferredName()
                    + "] "
                    + what
                    + " must be within +/-"
                    + (long) limit
                    + ", got ["
                    + value
                    + "] in geoshape aggregation."
            );
        }
    }

    private void setExtent(int extent) {
        if (extent < 0) {
            throw new IllegalArgumentException("[" + EXTENT_FIELD.getPreferredName() + "] must be >= 0 in geoshape aggregation.");
        }
        this.extent = extent;
    }

    private void setBuffer(double buffer) {
        if (buffer < 0) {
            throw new IllegalArgumentException("[" + BUFFER_FIELD.getPreferredName() + "] must be >= 0 in geoshape aggregation.");
        }
        this.buffer = buffer;
    }

    private void validate() {
        if (Double.isNaN(minLon)) {
            throw new IllegalArgumentException(
                "["
                    + BBOX_FIELD.getPreferredName()
                    + "] is mandatory in the ["
                    + GeoShapeBuilder.TILE_FIELD.getPreferredName()
                    + "] "
                    + "parameter of geoshape aggregation."
            );
        }
        if (minLon == maxLon || minLat == maxLat) {
            throw new IllegalArgumentException("[" + BBOX_FIELD.getPreferredName() + "] must not be degenerate in geoshape aggregation.");
        }
    }

    public boolean hasExtent() {
        return extent != NO_EXTENT;
    }

    public int getExtent() {
        return extent;
    }

    /**
     * The WGS84 window shapes are clipped against: the bounding box widened by {@code buffer} on
     * every side.
     *
     * <p>The widening is computed in degrees rather than in mercator meters. Mercator's latitude
     * scale is not linear, so the buffer is marginally wider at the poleward edge of the box than at
     * the equatorward one. The buffer only exists to keep adjacent tiles from showing a seam, so
     * erring on the generous side is harmless and this saves an inverse projection.
     */
    public Envelope clipEnvelope() {
        double bufferLon = buffer * (maxLon - minLon);
        double bufferLat = buffer * (maxLat - minLat);
        return new Envelope(minLon - bufferLon, maxLon + bufferLon, minLat - bufferLat, maxLat + bufferLat);
    }

    /**
     * The bounding box in web mercator meters. Quantization rescales against this, <b>not</b> against
     * {@link #clipEnvelope()}: the buffered margin is meant to fall outside {@code [0, extent]}.
     */
    public Envelope mercatorEnvelope() {
        return new Envelope(
            GeoUtils.lonToMercatorX(minLon),
            GeoUtils.lonToMercatorX(maxLon),
            GeoUtils.latToMercatorY(minLat),
            GeoUtils.latToMercatorY(maxLat)
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.array(BBOX_FIELD.getPreferredName(), minLon, minLat, maxLon, maxLat);
        if (hasExtent()) {
            builder.field(EXTENT_FIELD.getPreferredName(), extent);
        }
        builder.field(BUFFER_FIELD.getPreferredName(), buffer);
        return builder.endObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(minLon, minLat, maxLon, maxLat, extent, buffer);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        TileParams other = (TileParams) obj;
        return Double.compare(minLon, other.minLon) == 0
            && Double.compare(minLat, other.minLat) == 0
            && Double.compare(maxLon, other.maxLon) == 0
            && Double.compare(maxLat, other.maxLat) == 0
            && extent == other.extent
            && Double.compare(buffer, other.buffer) == 0;
    }
}
