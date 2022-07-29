package org.opendatasoft.elasticsearch.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.legacygeo.builders.GeometryCollectionBuilder;
import org.elasticsearch.legacygeo.builders.LineStringBuilder;
import org.elasticsearch.legacygeo.builders.MultiPolygonBuilder;
import org.elasticsearch.legacygeo.builders.PolygonBuilder;
import org.elasticsearch.legacygeo.builders.ShapeBuilder;
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
import org.elasticsearch.geometry.Line;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
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

    public static LineStringBuilder removeDuplicateCoordinates(LineStringBuilder builder) {
        Line line = (Line)builder.buildGeometry(); // no direct access to coordinates
        Vector<Coordinate> newCoordinates = new Vector<Coordinate>();

        Point previous = null;
        for (int i = 0; i < line.length(); i++) {
            Point current = new Point(line.getX(i), line.getY(i));
            if ((previous != null) && (previous.equals(current))) {
                continue;
            }
            newCoordinates.add(new Coordinate(current.getX(), current.getY()));
            previous = current;
        }
        return new LineStringBuilder(newCoordinates);
    }

    public static PolygonBuilder removeDuplicateCoordinates(PolygonBuilder builder) {
        LineStringBuilder exteriorBuilder = removeDuplicateCoordinates(builder.shell());
        PolygonBuilder pb = new PolygonBuilder(exteriorBuilder, /* unused */Orientation.RIGHT, true);
        List<LineStringBuilder> holes = builder.holes();
        for (int i = 0; i < holes.size(); i++) {
            pb.hole(removeDuplicateCoordinates(holes.get(i)), true);
        }
        return pb;
    }

    public static MultiPolygonBuilder removeDuplicateCoordinates(MultiPolygonBuilder builder) {
        MultiPolygonBuilder mpb = new MultiPolygonBuilder();
        List<PolygonBuilder> polygons = builder.polygons();
        for (int i = 0; i < polygons.size(); i++) {
            mpb.polygon(removeDuplicateCoordinates(polygons.get(i)));
        }
        return mpb;
    }

    public static GeometryCollectionBuilder removeDuplicateCoordinates(GeometryCollectionBuilder builder) {
        GeometryCollectionBuilder gcb = new GeometryCollectionBuilder();
        for (int i = 0; i < builder.numShapes(); i++) {
            gcb.shape(removeDuplicateCoordinates(builder.getShapeAt(i)));
        }
        return gcb;
    }

    public static ShapeBuilder removeDuplicateCoordinates(ShapeBuilder builder){
        if (builder instanceof LineStringBuilder) {
            return removeDuplicateCoordinates((LineStringBuilder)builder);
        }
        if (builder instanceof PolygonBuilder) {
            return removeDuplicateCoordinates((PolygonBuilder)builder);
        }
        if (builder instanceof MultiPolygonBuilder) {
            return removeDuplicateCoordinates((MultiPolygonBuilder)builder);
        }
        if (builder instanceof GeometryCollectionBuilder) {
            return removeDuplicateCoordinates((GeometryCollectionBuilder) builder);
        }
        return builder;
    }
}
