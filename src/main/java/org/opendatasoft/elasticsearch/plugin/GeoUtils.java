package org.opendatasoft.elasticsearch.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.util.GeometryFixer;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.io.geojson.GeoJsonWriter;
import org.locationtech.jts.operation.overlayng.OverlayNG;
import org.locationtech.jts.operation.overlayng.OverlayNGRobust;
import org.locationtech.jts.precision.GeometryPrecisionReducer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

public class GeoUtils {
    public enum OutputFormat {
        WKT,
        WKB,
        GEOJSON,
        /** The MVT command stream, an array of integers built by {@link MvtEncoder}; needs a {@code tile.extent}. */
        MVT
    }

    public enum SimplifyAlgorithm {
        DOUGLAS_PEUCKER,
        TOPOLOGY_PRESERVING
    }

    public static long getHashFromWKB(BytesRef wkb) {
        return MurmurHash3.hash128(wkb.bytes, wkb.offset, wkb.length, 0, new MurmurHash3.Hash128()).h1;
    }

    public static List<GeoPoint> getBboxFromCoords(Coordinate[] coords) {
        GeoPoint topLeft = new GeoPoint(
            org.elasticsearch.common.geo.GeoUtils.normalizeLat(coords[0].y),
            org.elasticsearch.common.geo.GeoUtils.normalizeLon(coords[0].x)
        );
        GeoPoint bottomRight = new GeoPoint(
            org.elasticsearch.common.geo.GeoUtils.normalizeLat(coords[2].y),
            org.elasticsearch.common.geo.GeoUtils.normalizeLon(coords[2].x)
        );
        return Arrays.asList(topLeft, bottomRight);
    }

    public static GeoPoint getCentroidFromGeom(Geometry geom) {
        Geometry geom_centroid = geom.getCentroid();
        return new GeoPoint(geom_centroid.getCoordinate().y, geom_centroid.getCoordinate().x);
    }

    public static double getArea(org.elasticsearch.geometry.Geometry geom) {
        Geometry jtsGeom = convertToJTS(geom);
        return jtsGeom.getArea();
    }

    public static GeoPoint getCentroidFromGeom(org.elasticsearch.geometry.Geometry geom) {
        Geometry jtsGeom = convertToJTS(geom);
        org.locationtech.jts.geom.Point point = jtsGeom.getCentroid();
        return new GeoPoint(point.getY(), point.getX());
    }

    public static Geometry getEnvelope(org.elasticsearch.geometry.Geometry geom) {
        Geometry jtsGeom = convertToJTS(geom);
        return jtsGeom.getEnvelope();
    }

    /**
     * Convert Elasticsearch Geometry → JTS Geometry
     */
    public static org.locationtech.jts.geom.Geometry convertToJTS(org.elasticsearch.geometry.Geometry esGeometry) {
        org.locationtech.jts.geom.GeometryFactory factory = new org.locationtech.jts.geom.GeometryFactory();

        return switch (esGeometry.type()) {
            case POINT -> {
                Point esPoint = (Point) esGeometry;
                yield factory.createPoint(new Coordinate(esPoint.getLon(), esPoint.getLat()));
            }
            case LINESTRING -> {
                Line esLine = (Line) esGeometry;
                Coordinate[] coords = extractCoordinates(esLine.getLons(), esLine.getLats());
                yield factory.createLineString(coords);
            }
            case POLYGON -> {
                Polygon esPolygon = (Polygon) esGeometry;
                yield convertPolygonToJTS(esPolygon, factory);
            }
            case MULTIPOINT -> {
                MultiPoint esMultiPoint = (MultiPoint) esGeometry;
                Coordinate[] coordinates = new Coordinate[esMultiPoint.size()];
                int index = 0;

                for (org.elasticsearch.geometry.Geometry geom : esMultiPoint) {
                    Point point = (Point) geom;
                    coordinates[index++] = new Coordinate(point.getX(), point.getY());
                }
                yield factory.createMultiPointFromCoords(coordinates);
            }
            case MULTILINESTRING -> {
                MultiLine esMultiLine = (MultiLine) esGeometry;
                LineString[] lineStrings = new LineString[esMultiLine.size()];

                int index = 0;
                for (org.elasticsearch.geometry.Geometry geom : esMultiLine) {
                    Line line = (Line) geom;
                    Coordinate[] coords = extractCoordinates(line.getLons(), line.getLats());
                    lineStrings[index++] = factory.createLineString(coords);
                }
                yield factory.createMultiLineString(lineStrings);
            }
            case MULTIPOLYGON -> {
                MultiPolygon esMultiPolygon = (MultiPolygon) esGeometry;
                yield convertMultiPolygonToJTS(esMultiPolygon, factory);
            }
            case GEOMETRYCOLLECTION -> {
                @SuppressWarnings("unchecked")
                GeometryCollection<org.elasticsearch.geometry.Geometry> esCollection = (GeometryCollection<
                    org.elasticsearch.geometry.Geometry>) esGeometry;
                yield convertGeometryCollectionToJTS(esCollection, factory);
            }
            default -> throw new IllegalArgumentException("Unsupported geometry type: " + esGeometry.type());
        };
    }

    private static Coordinate[] extractCoordinates(double[] longs, double[] lats) {
        Coordinate[] coords = new Coordinate[lats.length];
        for (int i = 0; i < lats.length; i++) {
            coords[i] = new Coordinate(longs[i], lats[i]);
        }
        return coords;
    }

    private static org.locationtech.jts.geom.Polygon convertPolygonToJTS(
        Polygon esPolygon,
        org.locationtech.jts.geom.GeometryFactory factory
    ) {
        // Anneau extérieur
        LinearRing exterior = esPolygon.getPolygon();
        Coordinate[] exteriorCoords = extractCoordinates(exterior.getLons(), exterior.getLats());
        org.locationtech.jts.geom.LinearRing jtsExterior = factory.createLinearRing(exteriorCoords);

        // Trous
        org.locationtech.jts.geom.LinearRing[] jtsHoles = new org.locationtech.jts.geom.LinearRing[esPolygon.getNumberOfHoles()];
        for (int i = 0; i < esPolygon.getNumberOfHoles(); i++) {
            LinearRing hole = esPolygon.getHole(i);
            Coordinate[] holeCoords = extractCoordinates(hole.getLons(), hole.getLats());
            jtsHoles[i] = factory.createLinearRing(holeCoords);
        }

        return factory.createPolygon(jtsExterior, jtsHoles);
    }

    private static org.locationtech.jts.geom.MultiPolygon convertMultiPolygonToJTS(
        MultiPolygon esMultiPolygon,
        org.locationtech.jts.geom.GeometryFactory factory
    ) {
        org.locationtech.jts.geom.Polygon[] jtsPolygons = new org.locationtech.jts.geom.Polygon[esMultiPolygon.size()];
        for (int i = 0; i < esMultiPolygon.size(); i++) {
            jtsPolygons[i] = convertPolygonToJTS(esMultiPolygon.get(i), factory);
        }
        return factory.createMultiPolygon(jtsPolygons);
    }

    private static org.locationtech.jts.geom.GeometryCollection convertGeometryCollectionToJTS(
        GeometryCollection<org.elasticsearch.geometry.Geometry> esCollection,
        org.locationtech.jts.geom.GeometryFactory factory
    ) {

        org.locationtech.jts.geom.Geometry[] jtsGeometries = new org.locationtech.jts.geom.Geometry[esCollection.size()];
        int i = 0;
        for (org.elasticsearch.geometry.Geometry geometry : esCollection) {
            jtsGeometries[i++] = convertToJTS(geometry);
        }
        return factory.createGeometryCollection(jtsGeometries);
    }

    // Return true if wkb is a point
    // http://en.wikipedia.org/wiki/Well-known_text#Well-known_binary
    public static boolean wkbIsPoint(byte[] wkb) {
        // A 2D point is exactly 21 bytes: byte order, type, x, y. Anything else with a point header is
        // malformed and must go through the parser, which rejects it, rather than be taken for a point.
        if (wkb.length != 21) {
            return false;
        }

        // Big endian or little endian shape representation
        if (wkb[0] == 0) {
            return wkb[1] == 0 && wkb[2] == 0 && wkb[3] == 0 && wkb[4] == 1;
        } else {
            return wkb[1] == 1 && wkb[2] == 0 && wkb[3] == 0 && wkb[4] == 0;
        }
    }

    public static double getMeterByPixel(int zoom, double lat) {
        return (org.elasticsearch.common.geo.GeoUtils.EARTH_EQUATOR / 256) * (Math.cos(Math.toRadians(lat)) / Math.pow(2, zoom));
    }

    public static double getDecimalDegreeFromMeter(double meter) {
        return meter * 360 / org.elasticsearch.common.geo.GeoUtils.EARTH_EQUATOR;
    }

    public static double getDecimalDegreeFromMeter(double meter, double latitude) {
        return meter * 360 / (org.elasticsearch.common.geo.GeoUtils.EARTH_EQUATOR * Math.cos(Math.toRadians(latitude)));
    }

    public static double getToleranceFromZoom(int zoom) {
        /*
        This is a simplified formula for
        double meterByPixel = GeoUtils.getMeterByPixel(zoom, lat);
        double tol = GeoUtils.getDecimalDegreeFromMeter(meterByPixel, lat);
         */
        return 360 / (256 * Math.pow(2, zoom));
    }

    /**
     * Earth radius used by the web mercator projection (EPSG:3857). Same value as the one used by
     * python's {@code mercantile.xy}, so both produce identical coordinates.
     */
    public static final double MERCATOR_EARTH_RADIUS = 6378137.0;

    public static double lonToMercatorX(double lon) {
        return MERCATOR_EARTH_RADIUS * Math.toRadians(lon);
    }

    /** Diverges toward the poles: the tile clip window keeps every coordinate within +/-85.0511. */
    public static double latToMercatorY(double lat) {
        return MERCATOR_EARTH_RADIUS * Math.log(Math.tan(Math.PI / 4 + Math.toRadians(lat) / 2));
    }

    /**
     * Cut a WGS84 geometry down to a WGS84 window, which is exact since web mercator is axis-separable.
     *
     * <p>The overlay is {@code OverlayNGRobust}, called directly: {@code Geometry.intersection} runs JTS'
     * legacy overlay by default, which throws a {@code TopologyException} on invalid shapes common in
     * real data (a MultiPolygon whose parts share an edge, a zero-width spike, a ring touching itself)
     * that {@code OverlayNGRobust} clips fine. A GeometryCollection still goes through
     * {@code Geometry.intersection}, which clips it member by member: OverlayNG rejects mixed-dimension
     * input.
     *
     * @return the clipped geometry, empty when nothing falls inside, or {@code geom} itself when it is fully inside
     */
    public static Geometry clipToBbox(Geometry geom, Envelope clipEnvelope) {
        Envelope geomEnvelope = geom.getEnvelopeInternal();

        // Both shortcuts spare us a full overlay computation, which is by far the costly part here.
        if (clipEnvelope.contains(geomEnvelope)) {
            return geom;
        }
        if (clipEnvelope.intersects(geomEnvelope) == false) {
            return geom.getFactory().createEmpty(geom.getDimension());
        }

        Geometry window = geom.getFactory().toGeometry(clipEnvelope);
        Geometry clipped = isHeterogeneous(geom)
            ? geom.intersection(window)
            : OverlayNGRobust.overlay(geom, window, OverlayNG.INTERSECTION);
        return keepDimensionOf(clipped, geom.getDimension());
    }

    /**
     * Discard clip output of a lower dimension than the input, as {@code ST_AsMVTGeom} does: a polygon
     * merely tangent to the window intersects it along a line or at a point, a valid, non-empty result
     * that neither the repair nor the empty check would catch. For a GeometryCollection, the input
     * dimension is that of its highest-dimension member.
     *
     * <p>Done here rather than through {@code OverlayNG.setStrictMode(true)}, so the clip keeps going
     * through {@code OverlayNGRobust} and its snapping fallbacks.
     */
    private static Geometry keepDimensionOf(Geometry clipped, int dimension) {
        if (clipped.isEmpty() || clipped.getDimension() == dimension && isHeterogeneous(clipped) == false) {
            return clipped;
        }

        if (clipped.getDimension() < dimension) {
            // Nothing of the right dimension survived: the shape only touched the window.
            return clipped.getFactory().createEmpty(dimension);
        }

        List<Geometry> kept = new ArrayList<>();
        for (int i = 0; i < clipped.getNumGeometries(); i++) {
            if (clipped.getGeometryN(i).getDimension() == dimension) {
                kept.add(clipped.getGeometryN(i));
            }
        }
        return clipped.getFactory().buildGeometry(kept);
    }

    /** A mixed collection, as opposed to a MultiPolygon or MultiLineString, whose parts all match. */
    private static boolean isHeterogeneous(Geometry geom) {
        return "GeometryCollection".equals(geom.getGeometryType());
    }

    /**
     * Force every ring of {@code geom}, in place, to a positive signed area for exteriors and a negative
     * one for holes, over the coordinates <b>as they currently stand</b>: the MVT winding on the y-down
     * grid, the RFC 7946 right-hand rule in mercator meters.
     *
     * <p>Apply this <b>after</b> any reprojection: flipping an axis flips the signed area. It cannot rely
     * on the input orientation either, since the overlay rebuilds the rings of a clipped shape with
     * clockwise shells while a shape inside the window passes through untouched.
     *
     * <p>{@code Orientation.isCCW} reads raw ordinates, so it calls a ring with a positive area
     * counter-clockwise whichever way the y axis points. That is the sense used below.
     */
    public static void orientRings(Geometry geom) {
        orientRingsWithoutInvalidating(geom);
        // Once, at the top: geometryChanged() walks every component, so calling it inside the
        // recursion would cost one full traversal per part.
        geom.geometryChanged();
    }

    private static void orientRingsWithoutInvalidating(Geometry geom) {
        if (geom instanceof org.locationtech.jts.geom.Polygon polygon) {
            orientRing(polygon.getExteriorRing(), true);
            for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
                orientRing(polygon.getInteriorRingN(i), false);
            }
        } else if (geom instanceof org.locationtech.jts.geom.GeometryCollection collection) {
            // Covers MultiPolygon and GeometryCollection alike.
            for (int i = 0; i < collection.getNumGeometries(); i++) {
                orientRingsWithoutInvalidating(collection.getGeometryN(i));
            }
        }
        // Points and lines carry no orientation.
    }

    private static void orientRing(org.locationtech.jts.geom.LinearRing ring, boolean counterClockwise) {
        CoordinateSequence sequence = ring.getCoordinateSequence();
        if (Orientation.isCCW(sequence) != counterClockwise) {
            reverseXY(sequence);
        }
    }

    /** The tile grid as a precision model: every coordinate an integer. */
    private static final PrecisionModel INTEGER_GRID = new PrecisionModel(1.0);

    /**
     * Make the transformed geometry valid again, dropping whatever collapsed: Douglas-Peucker can make a
     * ring cross itself, and rounding onto the grid collapses sub-pixel parts and pinches narrow notches.
     * {@link GeometryFixer} keeps the visible area of a pinched ring, which dropping invalid parts would
     * not, and a fully collapsed shape comes back empty. Only invalid geometry is rebuilt.
     *
     * <p>On the integer grid, the repair is snapped back onto it: {@link GeometryFixer} nodes crossing
     * edges at their true intersection, and rounding such a vertex later, as the mvt encoder has to,
     * could make the geometry invalid again or flip a ring.
     */
    public static Geometry makeValid(Geometry geom, boolean onIntegerGrid) {
        if (geom.isValid()) {
            return geom;
        }
        Geometry fixed = GeometryFixer.fix(geom);
        return onIntegerGrid ? GeometryPrecisionReducer.reduce(fixed, INTEGER_GRID) : fixed;
    }

    /**
     * Reverse a coordinate sequence in place, swapping x and y only. Not {@code CoordinateSequences.reverse}:
     * it swaps every ordinate up to {@code getDimension()}, and the {@code CoordinateXY} instances
     * {@code WKBReader} produces reject an ordinate index of 2.
     */
    private static void reverseXY(CoordinateSequence sequence) {
        for (int i = 0, j = sequence.size() - 1; i < j; i++, j--) {
            double x = sequence.getOrdinate(i, CoordinateSequence.X);
            double y = sequence.getOrdinate(i, CoordinateSequence.Y);
            sequence.setOrdinate(i, CoordinateSequence.X, sequence.getOrdinate(j, CoordinateSequence.X));
            sequence.setOrdinate(i, CoordinateSequence.Y, sequence.getOrdinate(j, CoordinateSequence.Y));
            sequence.setOrdinate(j, CoordinateSequence.X, x);
            sequence.setOrdinate(j, CoordinateSequence.Y, y);
        }
    }

    /**
     * Reproject a WGS84 geometry to web mercator meters, in place.
     */
    public static void toWebMercator(Geometry geom) {
        geom.apply(new MercatorFilter(null, 0));
    }

    /**
     * Reproject a WGS84 geometry, in place, to the integer grid {@code [0, extent]} local to
     * {@code mercatorBbox}, origin top-left, y downwards. The flip reverses the signed area of every
     * ring: call {@link #orientRings} afterwards, not before.
     */
    public static void toTileGrid(Geometry geom, Envelope mercatorBbox, int extent) {
        geom.apply(new MercatorFilter(mercatorBbox, extent));
    }

    /**
     * Projects WGS84 to web mercator and, when given a box and an extent, rescales to a tile-local
     * integer grid. One pass over every coordinate, no intermediate geometry.
     */
    private static final class MercatorFilter implements CoordinateSequenceFilter {
        private final Envelope mercatorBbox;
        private final double scaleX;
        private final double scaleY;

        MercatorFilter(Envelope mercatorBbox, int extent) {
            this.mercatorBbox = mercatorBbox;
            if (mercatorBbox != null) {
                this.scaleX = extent / mercatorBbox.getWidth();
                this.scaleY = extent / mercatorBbox.getHeight();
            } else {
                this.scaleX = 0;
                this.scaleY = 0;
            }
        }

        @Override
        public void filter(CoordinateSequence sequence, int i) {
            double x = lonToMercatorX(sequence.getOrdinate(i, CoordinateSequence.X));
            double y = latToMercatorY(sequence.getOrdinate(i, CoordinateSequence.Y));

            if (mercatorBbox != null) {
                x = Math.round((x - mercatorBbox.getMinX()) * scaleX);
                // Flip: the grid origin is the top-left corner, y grows downwards.
                y = Math.round((mercatorBbox.getMaxY() - y) * scaleY);
            }

            sequence.setOrdinate(i, CoordinateSequence.X, x);
            sequence.setOrdinate(i, CoordinateSequence.Y, y);
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public boolean isGeometryChanged() {
            return true;
        }
    }

    /**
     * The single place geojson writers are built. No {@code crs} member: RFC 7946 removed it, and JTS
     * would write the geometry's SRID, which nothing sets, as {@code EPSG:0}.
     */
    public static GeoJsonWriter createGeoJsonWriter() {
        GeoJsonWriter writer = new GeoJsonWriter();
        writer.setEncodeCRS(false);
        return writer;
    }

    public static String exportWkbTo(BytesRef wkb, OutputFormat output_format, GeoJsonWriter geoJsonWriter) throws ParseException {
        return switch (output_format) {
            case WKT -> new WKTWriter().write(new WKBReader().read(wkb.bytes));
            case WKB -> WKBWriter.toHex(wkb.bytes);
            case GEOJSON -> geoJsonWriter.write(new WKBReader().read(wkb.bytes));
            case MVT -> throw notTextual();
        };
    }

    public static String exportGeoTo(Geometry geom, OutputFormat outputFormat, GeoJsonWriter geoJsonWriter) {
        return switch (outputFormat) {
            case WKT -> new WKTWriter().write(geom);
            case WKB -> WKBWriter.toHex(new WKBWriter().write(geom));
            case GEOJSON -> geoJsonWriter.write(geom);
            case MVT -> throw notTextual();
        };
    }

    /** The geoshape aggregation renders {@link OutputFormat#MVT} itself, as an array of integers. */
    private static IllegalArgumentException notTextual() {
        return new IllegalArgumentException("[mvt] is a command stream, not a text format");
    }

    /**
     * Remove the duplicated coordinates from a Line
     */
    public static Line removeDuplicateCoordinates(Line line) {
        List<Double> newX = new ArrayList<>();
        List<Double> newY = new ArrayList<>();

        Point previous = null;
        for (int i = 0; i < line.length(); i++) {
            Point current = new Point(line.getX(i), line.getY(i));
            if ((previous != null) && (previous.equals(current))) {
                continue;
            }
            newX.add(current.getX());
            newY.add(current.getY());
            previous = current;
        }
        return new Line(newX.stream().mapToDouble(Double::doubleValue).toArray(), newY.stream().mapToDouble(Double::doubleValue).toArray());
    }

    /**
     * Remove the duplicated coordinates from a linear ring.
     */
    public static LinearRing removeDuplicateCoordinates(LinearRing ring) {
        Line line = removeDuplicateCoordinates((Line) ring);
        return new LinearRing(line.getX(), line.getY());
    }

    /**
     * Remove the duplicated coordinates from a Polygon
     */
    public static Polygon removeDuplicateCoordinates(Polygon polygon) {
        // Process the exterior ring
        LinearRing exteriorRing = removeDuplicateCoordinates(polygon.getPolygon());

        // Process each hole if necessary
        List<LinearRing> processedHoles = new ArrayList<>();
        for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
            LinearRing hole = polygon.getHole(i);
            LinearRing processedHole = removeDuplicateCoordinates(hole);
            processedHoles.add(processedHole);
        }

        return new Polygon(exteriorRing, processedHoles);
    }

    /**
    * Remove duplicated coordinates for each Polygon in a MultiPolygon
    */
    public static MultiPolygon removeDuplicateCoordinates(MultiPolygon multiPolygon) {
        List<Polygon> polygons = new ArrayList<>();

        for (int i = 0; i < multiPolygon.size(); i++) {
            Polygon polygon = multiPolygon.get(i);
            Polygon cleaned = removeDuplicateCoordinates(polygon);
            polygons.add(cleaned);
        }

        return new MultiPolygon(polygons);
    }

    /**
     * Process and clean-up a GeometryCollection
     */
    public static GeometryCollection<org.elasticsearch.geometry.Geometry> removeDuplicateCoordinates(
        GeometryCollection<org.elasticsearch.geometry.Geometry> collection
    ) {
        List<org.elasticsearch.geometry.Geometry> cleanedGeometries = new ArrayList<>();

        for (org.elasticsearch.geometry.Geometry geometry : collection) {
            org.elasticsearch.geometry.Geometry cleaned = removeDuplicateCoordinates(geometry);
            if (cleaned != null) {
                cleanedGeometries.add(cleaned);
            }
        }

        return new GeometryCollection<>(cleanedGeometries);
    }

    public static org.elasticsearch.geometry.Geometry removeDuplicateCoordinates(org.elasticsearch.geometry.Geometry geometry) {
        return switch (geometry.type()) {
            case POINT -> geometry; // Point does not have duplicated coordinates
            case LINESTRING -> removeDuplicateCoordinates((Line) geometry);
            case POLYGON -> removeDuplicateCoordinates((Polygon) geometry);
            case MULTIPOLYGON -> removeDuplicateCoordinates((MultiPolygon) geometry);
            case GEOMETRYCOLLECTION -> {
                // Safe cast
                @SuppressWarnings("unchecked")
                GeometryCollection<org.elasticsearch.geometry.Geometry> collection = (GeometryCollection<
                    org.elasticsearch.geometry.Geometry>) geometry;
                yield removeDuplicateCoordinates(collection);
            }
            default -> geometry;
        };
    }
}
