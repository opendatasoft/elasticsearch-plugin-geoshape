package org.opendatasoft.elasticsearch.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.io.geojson.GeoJsonWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

public class GeoUtils {
    public enum OutputFormat {
        WKT,
        WKB,
        GEOJSON
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
        if (wkb.length < 5) {
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

    public static String exportWkbTo(BytesRef wkb, OutputFormat output_format, GeoJsonWriter geoJsonWriter) throws ParseException {
        switch (output_format) {
            case WKT:
                Geometry geom = new WKBReader().read(wkb.bytes);
                return new WKTWriter().write(geom);
            case WKB:
                return WKBWriter.toHex(wkb.bytes);
            default:
                Geometry geo = new WKBReader().read(wkb.bytes);
                return geoJsonWriter.write(geo);
        }
    }

    public static String exportGeoTo(Geometry geom, OutputFormat outputFormat, GeoJsonWriter geoJsonWriter) {
        switch (outputFormat) {
            case WKT:
                return new WKTWriter().write(geom);
            case WKB:
                return WKBWriter.toHex(new WKBWriter().write(geom));
            default:
                return geoJsonWriter.write(geom);
        }
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
