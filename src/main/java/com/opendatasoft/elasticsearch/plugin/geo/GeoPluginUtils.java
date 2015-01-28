package com.opendatasoft.elasticsearch.plugin.geo;

import com.spatial4j.core.io.GeohashUtils;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.hash.MurmurHash3;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: clement
 * Date: 09/01/15
 * Time: 14:04
 * To change this template use File | Settings | File Templates.
 */
public class GeoPluginUtils {

//    public static String getHashFromWKB(byte[] wkb) {
//        try {
//            byte[] mdBytes = MessageDigest.getInstance("md5").digest(wkb);
//            StringBuffer sb = new StringBuffer();
//
//            for (byte mdByte : mdBytes) {
//                sb.append(Integer.toString((mdByte & 0xff) + 0x100, 16).substring(1));
//            }
//
//            return sb.toString();
//        } catch (NoSuchAlgorithmException e) {
//            return "";
////            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//    }

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

//    public static List<Double> getEnvelopeForTile(int x, int y, int z, int tileSize) {
//
//    }

    public static class Envelope {
        public double north;
        public double west;
        public double south;
        public double east;
    }
    public static Envelope tile2boundingBox(final int x, final int y, int z, int tileSize) throws Exception {
        if (tileSize == 512) {
            z -= 1;
        }
        else if (tileSize != 256) {
            throw new Exception("Only 256 and 512 tile sizes are supported for map rendering");
        }

        Envelope bb = new Envelope();
        bb.north = tile2lat(y, z);
        bb.west = tile2lon(x, z);
        bb.south = tile2lat(y + 1, z);
        bb.east = tile2lon(x + 1, z);
        return bb;
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

        double latOverFlow = getOverflow(envelope.north, envelope.south, pixelOverflow, tileSize);
        double lonOverFlow = getOverflow(envelope.west, envelope.east, pixelOverflow, tileSize);

        Envelope res = new Envelope();

        res.north = normalizeLat(envelope.north + latOverFlow);
        res.south = normalizeLat(envelope.south - latOverFlow);
        res.west = normalizeLon(envelope.west - lonOverFlow);
        res.east = normalizeLon(envelope.east + lonOverFlow);

        return res;

    }

    public static void main(String[] args) {
//        System.out.println(getMetersFromDecimalDegree(1));
//        System.out.println(getMetersFromDecimalDegree(0.6) / 1000);
//        System.out.println(getMetersFromDecimalDegree(0.6, 47) / 1000);


        for (int i = 0; i< 20; i++) {
//            System.out.println("zoom : " + i + " == " + GeoUtils.geoHashLevelsForPrecision(GeoPluginUtils.getMeterByPixel(i, 42)));


            double diago = Math.sqrt(Math.pow(40,2) *2 );

            double meterByPixel = GeoPluginUtils.getMeterByPixel(i, 42);

//            double degrees = 360.0 * 40 * meterByPixel / GeoUtils.EARTH_EQUATOR;

//            System.out.println(degrees);

            double degrees = getDecimalDegreeFromMeter(40 * meterByPixel, 42);

            System.out.println("spatial4j : " + i + " : " + GeohashUtils.lookupHashLenForWidthHeight(degrees, degrees));

            System.out.println("elastic : " + i + " : " +  GeoUtils.geoHashLevelsForPrecision(GeoPluginUtils.getMeterByPixel(i, 42) * diago));

//            System.out.println("zoom : " + i + " == " + GeoUtils.geoHashLevelsForPrecision(GeoPluginUtils.getMeterByPixel(i, 42) * 40));
//
//            System.out.println("zoom : " + i + " == " + getDecimalDegreeFromMeter(getMeterByPixel(i, 0) * 10));
//            System.out.println("zoom : " + i + " == " + getShapeLimit(i, 0, 10));


//            System.out.println((int) (100 / (i + 1)));
        }


//        System.out.println(getMeterByPixel(18, 0) * 512);
//        System.out.println(getDecimalDegreeFromMeter(getMeterByPixel(18, 0) * 512));
//        System.out.println(getDecimalDegreeFromMeter(getMeterByPixel(18, 0) * 512, 42));


//        System.out.println(GeoUtils.geoHashLevelsForPrecision(GeoPluginUtils.getMeterByPixel(12, 42)));
//        System.out.println(GeoUtils.geoHashLevelsForPrecision(GeoPluginUtils.getMeterByPixel(12, 42) * 40));

        System.out.println();

    }



}
