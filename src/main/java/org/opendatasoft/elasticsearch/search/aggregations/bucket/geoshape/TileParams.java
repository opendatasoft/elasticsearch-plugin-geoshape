package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.locationtech.jts.geom.Envelope;
import org.opendatasoft.elasticsearch.plugin.GeoUtils;

import java.io.IOException;
import java.util.Objects;

/**
 * The {@code tile} param: an XYZ slippy-map tile, the {@code buffer} widening it, and an optional
 * {@code extent} for the integer grid local to the tile.
 */
public class TileParams implements Writeable, ToXContentObject {

    static final ParseField Z_FIELD = new ParseField("z");
    static final ParseField X_FIELD = new ParseField("x");
    static final ParseField Y_FIELD = new ParseField("y");
    static final ParseField EXTENT_FIELD = new ParseField("extent");
    static final ParseField BUFFER_FIELD = new ParseField("buffer");

    /** Fraction of the tile size added on each side. Matches PostGIS' 256/4096 default. */
    public static final double DEFAULT_BUFFER = 0.0625;

    /** Sentinel for "no extent given": clip and reproject, but do not quantize. */
    public static final int NO_EXTENT = 0;

    private int z;
    private int x;
    private int y;
    private int extent = NO_EXTENT;
    private double buffer = DEFAULT_BUFFER;

    private static final ObjectParser<TileParams, Void> PARSER = new ObjectParser<>("tile", TileParams::new);
    static {
        PARSER.declareInt((tile, value) -> tile.z = value, Z_FIELD);
        PARSER.declareInt((tile, value) -> tile.x = value, X_FIELD);
        PARSER.declareInt((tile, value) -> tile.y = value, Y_FIELD);
        PARSER.declareRequiredFieldSet(Z_FIELD.getPreferredName());
        PARSER.declareRequiredFieldSet(X_FIELD.getPreferredName());
        PARSER.declareRequiredFieldSet(Y_FIELD.getPreferredName());
        PARSER.declareInt(TileParams::setExtent, EXTENT_FIELD);
        PARSER.declareDouble(TileParams::setBuffer, BUFFER_FIELD);
    }

    private TileParams() {}

    public TileParams(int z, int x, int y, int extent, double buffer) {
        this.z = z;
        this.x = x;
        this.y = y;
        // NO_EXTENT is how code asks for no quantization; a request asks for it by leaving extent out.
        if (extent != NO_EXTENT) {
            setExtent(extent);
        }
        setBuffer(buffer);
        // Same guard as the parser: an index outside its zoom names no tile, so no entry point may skip it.
        validate();
    }

    public TileParams(StreamInput in) throws IOException {
        z = in.readInt();
        x = in.readInt();
        y = in.readInt();
        extent = in.readInt();
        buffer = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(z);
        out.writeInt(x);
        out.writeInt(y);
        out.writeInt(extent);
        out.writeDouble(buffer);
    }

    static TileParams parse(XContentParser parser) throws IOException {
        TileParams tileParams = PARSER.parse(parser, null);
        tileParams.validate();
        return tileParams;
    }

    private void setExtent(int extent) {
        // 0 is not a size: it is the NO_EXTENT sentinel, which a request must not be able to send.
        if (extent < 1) {
            throw new IllegalArgumentException("[" + EXTENT_FIELD.getPreferredName() + "] must be >= 1 in geoshape aggregation.");
        }
        this.extent = extent;
    }

    private void setBuffer(double buffer) {
        // NaN compares false to everything, so it must be named: it would empty every tile silently.
        if (Double.isNaN(buffer) || buffer < 0) {
            throw new IllegalArgumentException("[" + BUFFER_FIELD.getPreferredName() + "] must be >= 0 in geoshape aggregation.");
        }
        this.buffer = buffer;
    }

    /**
     * Checked once all fields are read, since x and y are only bounded once z is known. z stops where
     * elasticsearch's own geotile_grid does, which also keeps {@code 1 << z} and {@code 2 * y} inside an int.
     */
    private void validate() {
        if (z < 0 || z > GeoTileUtils.MAX_ZOOM) {
            throw new IllegalArgumentException(
                "["
                    + Z_FIELD.getPreferredName()
                    + "] must be within [0, "
                    + GeoTileUtils.MAX_ZOOM
                    + "], got ["
                    + z
                    + "] in geoshape aggregation."
            );
        }
        requireTileIndex(x, X_FIELD);
        requireTileIndex(y, Y_FIELD);
    }

    private void requireTileIndex(int index, ParseField field) {
        int maxIndex = (1 << z) - 1;
        if (index < 0 || index > maxIndex) {
            throw new IllegalArgumentException(
                "["
                    + field.getPreferredName()
                    + "] must be within [0, "
                    + maxIndex
                    + "] at ["
                    + Z_FIELD.getPreferredName()
                    + "] ["
                    + z
                    + "], got ["
                    + index
                    + "] in geoshape aggregation."
            );
        }
    }

    public boolean hasExtent() {
        return extent != NO_EXTENT;
    }

    public int getExtent() {
        return extent;
    }

    /**
     * The tile's WGS84 box, with the expressions of {@code mercantile.bounds(x, y, z)}, operation for operation,
     * so that it is the box a caller holding the tile computes. Longitude is plain arithmetic and matches it
     * exactly. Latitude goes through sinh and atan, where Java and the caller's libm can differ by an ulp or two,
     * far below a grid unit.
     */
    private Envelope bounds() {
        double tiles = 1 << z;
        return new Envelope(tileLon(x, tiles), tileLon(x + 1, tiles), tileLat(y + 1, tiles), tileLat(y, tiles));
    }

    private static double tileLon(int x, double tiles) {
        return x / tiles * 360.0 - 180.0;
    }

    private static double tileLat(int y, double tiles) {
        return Math.toDegrees(Math.atan(Math.sinh(Math.PI * (1 - 2 * y / tiles))));
    }

    /**
     * The WGS84 window shapes are clipped against: the tile widened by {@code buffer} on every side. Widened
     * in degrees, so marginally more on the poleward edge than in mercator meters, which is harmless for a
     * margin that only hides seams and saves an inverse projection.
     */
    public Envelope clipEnvelope() {
        Envelope bounds = bounds();
        double bufferLon = buffer * bounds.getWidth();
        double bufferLat = buffer * bounds.getHeight();
        return new Envelope(
            bounds.getMinX() - bufferLon,
            bounds.getMaxX() + bufferLon,
            bounds.getMinY() - bufferLat,
            bounds.getMaxY() + bufferLat
        );
    }

    /**
     * The tile in web mercator meters, projected from its WGS84 corners like any other coordinate. Quantization
     * rescales against this, <b>not</b> against {@link #clipEnvelope()}: the buffered margin is meant to fall
     * outside {@code [0, extent]}.
     */
    public Envelope mercatorEnvelope() {
        Envelope bounds = bounds();
        return new Envelope(
            GeoUtils.lonToMercatorX(bounds.getMinX()),
            GeoUtils.lonToMercatorX(bounds.getMaxX()),
            GeoUtils.latToMercatorY(bounds.getMinY()),
            GeoUtils.latToMercatorY(bounds.getMaxY())
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Z_FIELD.getPreferredName(), z);
        builder.field(X_FIELD.getPreferredName(), x);
        builder.field(Y_FIELD.getPreferredName(), y);
        if (hasExtent()) {
            builder.field(EXTENT_FIELD.getPreferredName(), extent);
        }
        builder.field(BUFFER_FIELD.getPreferredName(), buffer);
        return builder.endObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(z, x, y, extent, buffer);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        TileParams other = (TileParams) obj;
        return z == other.z && x == other.x && y == other.y && extent == other.extent && Double.compare(buffer, other.buffer) == 0;
    }
}
