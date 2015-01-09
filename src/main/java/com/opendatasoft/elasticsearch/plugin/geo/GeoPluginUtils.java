package com.opendatasoft.elasticsearch.plugin.geo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created with IntelliJ IDEA.
 * User: clement
 * Date: 09/01/15
 * Time: 14:04
 * To change this template use File | Settings | File Templates.
 */
public class GeoPluginUtils {

    public static String getHashFromWKB(byte[] wkb) {
        try {
            byte[] mdBytes = MessageDigest.getInstance("md5").digest(wkb);
            StringBuffer sb = new StringBuffer();

            for (byte mdByte : mdBytes) {
                sb.append(Integer.toString((mdByte & 0xff) + 0x100, 16).substring(1));
            }

            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            return "";
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

}
