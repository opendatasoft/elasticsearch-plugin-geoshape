package com.opendatasoft.elasticsearch.odsscript;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

import java.util.Map;

/*
 *  Copied from elasticsearch-ext-opendatasoft/GeoPreviewPlugin
 */
public class OdsScriptFactory implements NativeScriptFactory {

    @Override
    public ExecutableScript newScript(@Nullable Map<String, Object> params) {
        // geoshapestringfield and zoom are mandatory

        if (params == null) {
            // throw npe error
        }

        String geoShapeStringField = (String) params.get("geoshapestringfield");


        if (geoShapeStringField == null) {
            throw new ElasticsearchIllegalArgumentException("Missing the geoshapestringfield parameter");
        }

        Boolean simplify = (Boolean) params.get("simplify");

        if (simplify == null) {
            simplify = false;
        }

        Integer zoom = (Integer) params.get("zoom");

        if (simplify && zoom == null) {
            throw new ElasticsearchIllegalArgumentException("Missing the zoom parameter");
        }

        double tolerance = 360 / (256 * Math.pow(zoom, 3));

        int nbDecimals = 20;

        String stringOutputFormat = (String) params.get("output_format");

        GeoScript.OutputFormat outputFormat = GeoScript.OutputFormat.GEOJSON;

        if (stringOutputFormat != null) {
            outputFormat = GeoScript.OutputFormat.valueOf(stringOutputFormat.toUpperCase());
        }

        String algorithmString = (String) params.get("algorithm");

        GeoScript.Algorithm algorithm = GeoScript.Algorithm.DOUGLAS_PEUCKER;

        if (algorithmString != null) {
            algorithm = GeoScript.Algorithm.valueOf(algorithmString.toUpperCase());
        }

        Boolean isPoint = (Boolean) params.get("is_point");

        if (isPoint == null) {
            isPoint = false;
        }

        return new GeoScript(geoShapeStringField, tolerance, nbDecimals, outputFormat, algorithm, isPoint, simplify);
    }
}
