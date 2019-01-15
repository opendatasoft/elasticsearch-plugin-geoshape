package org.opendatasoft.elasticsearch.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.hash.MurmurHash3;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.CoordinateList;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.io.geojson.GeoJsonWriter;
import org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.InternalGeoShape;

import java.util.Arrays;
import java.util.List;

public class GeoUtils {
    public static long getHashFromWKB(BytesRef wkb) {
        return MurmurHash3.hash128(wkb.bytes, wkb.offset, wkb.length, 0, new MurmurHash3.Hash128()).h1;
    }

    public static List<GeoPoint> getBboxFromCoords(Coordinate[] coords) {
        GeoPoint topLeft = new GeoPoint(coords[0].y, coords[0].x);
        GeoPoint bottomRight = new GeoPoint(coords[2].y, coords[2].x);
        return Arrays.asList(topLeft, bottomRight);
    }

    public static GeoPoint getCentroidFromGeom(Geometry geom) {
        Geometry geom_centroid = geom.getCentroid();
        return new GeoPoint(geom_centroid.getCoordinate().y, geom_centroid.getCoordinate().x);
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


    public static String exportWkbTo(BytesRef wkb, InternalGeoShape.OutputFormat output_format, GeoJsonWriter geoJsonWriter)
            throws ParseException {
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

    public static String exportGeoTo(Geometry geom, InternalGeoShape.OutputFormat outputFormat, GeoJsonWriter geoJsonWriter) {
        switch (outputFormat) {
            case WKT:
                return new WKTWriter().write(geom);
            case WKB:
                return WKBWriter.toHex(new WKBWriter().write(geom));
            default:
                return geoJsonWriter.write(geom);
        }
    }

    public static Geometry removeDuplicateCoordinates(Geometry geom) {
        if (geom.isEmpty()) {
            return geom;
        }

        if (geom instanceof Polygon) {
            return removeDuplicateCoordinates((Polygon) geom);
        }

        if (geom instanceof MultiPolygon) {
            return removeDuplicateCoordinates((MultiPolygon) geom);
        }

        if (geom instanceof GeometryCollection) {
            return removeDuplicateCoordinates((GeometryCollection) geom);
        }

        return geom;
    }

    public static Polygon removeDuplicateCoordinates(Polygon polygon) {
        LinearRing polygonShell = removeDuplicateCoordinates((LinearRing) polygon.getExteriorRing());
        LinearRing[] holes = new LinearRing[polygon.getNumInteriorRing()];
        for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            holes[i] = removeDuplicateCoordinates((LinearRing) polygon.getInteriorRingN(i));
        }
        return polygon.getFactory().createPolygon(polygonShell, holes);
    }

    public static LinearRing removeDuplicateCoordinates(LinearRing linearRing) {
        return linearRing.getFactory().createLinearRing(
                new CoordinateList(linearRing.getCoordinates(), false).toCoordinateArray()
        );
    }

    public static MultiPolygon removeDuplicateCoordinates(MultiPolygon multiPolygon) {

        Polygon[] polygons = new Polygon[multiPolygon.getNumGeometries()];

        for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
            polygons[i] = (Polygon) removeDuplicateCoordinates(multiPolygon.getGeometryN(i));
        }

        return multiPolygon.getFactory().createMultiPolygon(polygons);
    }

    public static GeometryCollection removeDuplicateCoordinates(GeometryCollection geometryCollection) {

        Geometry[] geometries = new Geometry[geometryCollection.getNumGeometries()];

        for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
            geometries[i] = removeDuplicateCoordinates(geometryCollection.getGeometryN(i));
        }

        return geometryCollection.getFactory().createGeometryCollection(geometries);
    }
}
