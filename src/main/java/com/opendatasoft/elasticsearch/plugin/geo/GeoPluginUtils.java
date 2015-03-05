package com.opendatasoft.elasticsearch.plugin.geo;

import com.vividsolutions.jts.geom.Envelope;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.hash.MurmurHash3;

public class GeoPluginUtils {

    public static long getHashFromWKB(BytesRef wkb) {
        return MurmurHash3.hash128(wkb.bytes, wkb.offset, wkb.length, 0, new MurmurHash3.Hash128()).h1;
    }

    public static double getMetersFromDecimalDegree(double decimalDegree) {
        return GeoUtils.EARTH_EQUATOR * decimalDegree / 360;
    }

    public static double getMetersFromDecimalDegree(double decimalDegree, double latitude) {
        return (GeoUtils.EARTH_EQUATOR * Math.cos(Math.toRadians(latitude))) * decimalDegree / 360;
    }

    public static double getDecimalDegreeFromMeter(double meter) {
        return meter * 360 / GeoUtils.EARTH_EQUATOR;
    }

    public static double getDecimalDegreeFromMeter(double meter, double latitude) {
        return meter * 360 / (GeoUtils.EARTH_EQUATOR * Math.cos(Math.toRadians(latitude)));
    }

    public static Envelope tile2boundingBox(final int x, final int y, int z, int tileSize) throws Exception {
        if (tileSize == 512) {
            z -= 1;
        }
        else if (tileSize != 256) {
            throw new Exception("Only 256 and 512 tile sizes are supported for map rendering");
        }

        return new Envelope(tile2lon(x, z), tile2lon(x + 1, z), tile2lat(y + 1, z), tile2lat(y, z));
    }

    static double tile2lon(int x, int z) {
        return x / Math.pow(2.0, z) * 360.0 - 180;
    }

    static double tile2lat(int y, int z) {
        double n = Math.PI - (2.0 * Math.PI * y) / Math.pow(2.0, z);
        return Math.toDegrees(Math.atan(Math.sinh(n)));
    }


    public static double getMeterByPixel(int zoom, double lat) {
        return (GeoUtils.EARTH_EQUATOR / 256) * (Math.cos(Math.toRadians(lat)) / Math.pow(2, zoom));
    }


    public static double getShapeLimit(int zoom, double latitude, int nbPixel) {
       return getDecimalDegreeFromMeter(getMeterByPixel(zoom, latitude) * nbPixel, latitude);
    }

    public static double getShapeLimit (int zoom, double latitude) {
        return getShapeLimit(zoom, latitude, 1);
    }

    public static double getOverflow(double coord1, double coord2, int pixelOverflow, int tileSize) {
        double distance = Math.abs(coord1 - coord2);
        double pixelPerDistance = distance / tileSize;

        return  pixelOverflow * pixelPerDistance;
    }

    public static double normalizeLat(double latitude) {
        if (latitude < -90) {
            return -90;
        }
        if (latitude > 90) {
            return 90;
        }
        return latitude;
    }

    public static double normalizeLon(double longitude) {
        if (longitude < -179.9) {
            return -179.9;
        }
        if (longitude > 179.9) {
            return 179.9;
        }
        return longitude;
    }

    public static Envelope overflowEnvelope(Envelope envelope, int pixelOverflow, int tileSize) {

        double latOverFlow = getOverflow(envelope.getMaxY(), envelope.getMinY(), pixelOverflow, tileSize);
        double lonOverFlow = getOverflow(envelope.getMinX(), envelope.getMaxX(), pixelOverflow, tileSize);

        Envelope res = new Envelope();

        res.init(normalizeLon(envelope.getMinX() - lonOverFlow), normalizeLon(envelope.getMaxX() + lonOverFlow),
                normalizeLat(envelope.getMinY() - latOverFlow), normalizeLat(envelope.getMaxY() + latOverFlow));

        return res;

    }

}
