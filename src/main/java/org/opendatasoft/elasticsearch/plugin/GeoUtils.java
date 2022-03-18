package org.opendatasoft.elasticsearch.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.hash.MurmurHash3;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.io.geojson.GeoJsonWriter;

import java.util.Arrays;
import java.util.List;

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


    public static String exportWkbTo(BytesRef wkb, OutputFormat output_format, GeoJsonWriter geoJsonWriter)
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
}
