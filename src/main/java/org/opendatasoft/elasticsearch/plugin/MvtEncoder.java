package org.opendatasoft.elasticsearch.plugin;

import org.apache.lucene.util.ArrayUtil;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * Encodes a geometry already quantized onto a tile grid into the command stream of the Mapbox Vector
 * Tile spec (version 2.1, section 4.3): command integers packing {@code count << 3 | id}, and points as
 * zigzag-encoded deltas from the pen's current position.
 *
 * <p>The cursor is a property of the geometry, not of the ring: deltas carry on across the rings of a
 * polygon and across the parts of a multi-geometry. A ring that is dropped must therefore not move
 * it, which is why each path is materialized before any of it is written.
 */
public final class MvtEncoder {

    /** Command ids, from the spec's section 4.3.3.1. */
    private static final int MOVE_TO = 1;
    private static final int LINE_TO = 2;
    private static final int CLOSE_PATH = 7;

    /** A command integer keeps 3 bits for the id, so the repeat count is 29 bits wide. */
    static final int MAX_COMMAND_COUNT = (1 << 29) - 1;

    private static final int[] EMPTY_STREAM = new int[0];

    private MvtEncoder() {}

    /**
     * Encode {@code geom} into its command stream.
     *
     * <p>Returns an <b>empty</b> stream when the geometry draws nothing, which a non-empty geometry
     * can do once every one of its rings has collapsed onto a single grid cell.
     *
     * @throws IllegalArgumentException if the geometry has no MVT equivalent, or if a single path is
     *                                  too long for one command header
     */
    public static int[] encode(Geometry geom) {
        CommandStream stream = new CommandStream();
        write(geom, stream);
        return stream.toArray();
    }

    private static void write(Geometry geom, CommandStream stream) {
        if (geom.isEmpty()) {
            return;
        }

        if (geom instanceof Point point) {
            stream.command(MOVE_TO, 1);
            stream.point(round(point.getX()), round(point.getY()));
        } else if (geom instanceof MultiPoint multiPoint) {
            writeMultiPoint(multiPoint, stream);
        } else if (geom instanceof LinearRing ring) {
            // A standalone ring is closed, so it is written as one, ClosePath included.
            writeRing(ring.getCoordinateSequence(), stream);
        } else if (geom instanceof LineString line) {
            writeLine(line.getCoordinateSequence(), stream);
        } else if (geom instanceof Polygon polygon) {
            writePolygon(polygon, stream);
        } else if (geom instanceof MultiPolygon || geom instanceof MultiLineString) {
            // One part after another, sharing the cursor.
            for (int i = 0; i < geom.getNumGeometries(); i++) {
                write(geom.getGeometryN(i), stream);
            }
        } else {
            // A plain GeometryCollection has no MVT counterpart: a feature carries one geometry type.
            // The aggregation leaves stored collections out before encoding, so this guards other callers.
            throw new IllegalArgumentException("cannot encode a [" + geom.getGeometryType() + "] as an MVT command stream");
        }
    }

    /**
     * A multipoint is a single {@code MoveTo} carrying every point. Points landing on the same grid
     * cell are kept: unlike two vertices of a ring, they are two distinct members of the geometry.
     */
    private static void writeMultiPoint(MultiPoint multiPoint, CommandStream stream) {
        int count = 0;
        for (int i = 0; i < multiPoint.getNumGeometries(); i++) {
            if (multiPoint.getGeometryN(i).isEmpty() == false) {
                count++;
            }
        }
        if (count == 0) {
            return;
        }

        stream.command(MOVE_TO, count);
        for (int i = 0; i < multiPoint.getNumGeometries(); i++) {
            Point point = (Point) multiPoint.getGeometryN(i);
            if (point.isEmpty() == false) {
                stream.point(round(point.getX()), round(point.getY()));
            }
        }
    }

    /**
     * Write a polygon as its exterior ring followed by its holes.
     *
     * <p>A polygon whose exterior collapsed is dropped whole: its holes describe the absence of area
     * inside a shell that no longer exists, and emitting them alone would draw them as shells.
     */
    private static void writePolygon(Polygon polygon, CommandStream stream) {
        if (writeRing(polygon.getExteriorRing().getCoordinateSequence(), stream) == false) {
            return;
        }
        for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            writeRing(polygon.getInteriorRingN(i).getCoordinateSequence(), stream);
        }
    }

    /**
     * Write one ring as {@code MoveTo 1}, {@code LineTo n-1}, {@code ClosePath}.
     *
     * @return false when the ring drew nothing and no command was written
     */
    private static boolean writeRing(CoordinateSequence sequence, CommandStream stream) {
        int[] points = gridPoints(sequence, true);
        // Fewer than three distinct points enclose no area. Such a ring is dropped entirely, header
        // included: an empty outline costs four integers and draws a degenerate feature.
        if (points.length < 6) {
            return false;
        }
        writePath(points, stream);
        stream.command(CLOSE_PATH, 1);
        return true;
    }

    /** Write one open path as {@code MoveTo 1} then {@code LineTo n-1}. */
    private static void writeLine(CoordinateSequence sequence, CommandStream stream) {
        int[] points = gridPoints(sequence, false);
        // A single point draws no segment.
        if (points.length < 4) {
            return;
        }
        writePath(points, stream);
    }

    private static void writePath(int[] points, CommandStream stream) {
        stream.command(MOVE_TO, 1);
        stream.point(points[0], points[1]);
        stream.command(LINE_TO, points.length / 2 - 1);
        for (int i = 2; i < points.length; i += 2) {
            stream.point(points[i], points[i + 1]);
        }
    }

    /**
     * The grid points a path actually draws, flat as {@code x0, y0, x1, y1, ...}: vertices that
     * rounded onto the same cell as their predecessor are skipped.
     *
     * @param ring true to read a closed JTS ring, whose repeated last coordinate {@code ClosePath}
     *             replaces
     */
    private static int[] gridPoints(CoordinateSequence sequence, boolean ring) {
        int size = sequence.size();
        if (ring && size > 0) {
            size--;
        }

        int[] points = new int[2 * size];
        int count = 0;
        for (int i = 0; i < size; i++) {
            int x = round(sequence.getOrdinate(i, CoordinateSequence.X));
            int y = round(sequence.getOrdinate(i, CoordinateSequence.Y));
            if (count > 0 && points[2 * count - 2] == x && points[2 * count - 1] == y) {
                continue;
            }
            points[2 * count] = x;
            points[2 * count + 1] = y;
            count++;
        }

        // ClosePath already draws the segment back to the first point, so a vertex that rounded onto
        // it is redundant.
        while (ring && count > 1 && points[2 * count - 2] == points[0] && points[2 * count - 1] == points[1]) {
            count--;
        }

        return 2 * count == points.length ? points : ArrayUtil.copyOfSubArray(points, 0, 2 * count);
    }

    /** Snap an ordinate onto the grid. It is already integral; rounding guards against a value a hair off. */
    private static int round(double ordinate) {
        return (int) Math.round(ordinate);
    }

    /** Pack a command id and its repeat count into one integer. */
    static int command(int id, int count) {
        if (count < 0 || count > MAX_COMMAND_COUNT) {
            throw new IllegalArgumentException(
                "a path of [" + count + "] points does not fit in the 29 bits an MVT command header keeps for its count"
            );
        }
        return count << 3 | id;
    }

    /**
     * Map a signed delta onto an unsigned integer of the same magnitude, so that {@code -1} costs one
     * byte as a varint rather than five.
     */
    static int zigzag(int value) {
        return value << 1 ^ value >> 31;
    }

    /** The growable integer buffer being written, and the pen position the deltas are taken from. */
    private static final class CommandStream {
        private int[] values = EMPTY_STREAM;
        private int size;
        private int cursorX;
        private int cursorY;

        void command(int id, int count) {
            add(MvtEncoder.command(id, count));
        }

        void point(int x, int y) {
            add(zigzag(x - cursorX));
            add(zigzag(y - cursorY));
            cursorX = x;
            cursorY = y;
        }

        private void add(int value) {
            values = ArrayUtil.grow(values, size + 1);
            values[size++] = value;
        }

        int[] toArray() {
            return size == values.length ? values : ArrayUtil.copyOfSubArray(values, 0, size);
        }
    }
}
