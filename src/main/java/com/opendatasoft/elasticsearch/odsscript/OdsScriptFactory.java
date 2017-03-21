package com.opendatasoft.elasticsearch.odsscript;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

import java.util.Map;

public class OdsScriptFactory implements NativeScriptFactory {

    @Override
    public ExecutableScript newScript(@Nullable Map<String, Object> params) {

        if (params == null) {
            throw new ElasticsearchIllegalArgumentException("'params' field is mandatory");
        }

        String field = (String) params.get("field");

        if (field == null) {
            throw new ElasticsearchIllegalArgumentException("'field' parameter is mandatory");
        }

        Integer zoom = (Integer) params.get("zoom");

        if (zoom == null) {
            throw new ElasticsearchIllegalArgumentException("'zoom' parameter is mandatory");
        }

        double tolerance = 360 / (256 * Math.pow(zoom, 3));

        int nbDecimals = 20;

        String stringOutputFormat = (String) params.get("output_format");

        GeoSimplifyScript.OutputFormat outputFormat = GeoSimplifyScript.OutputFormat.GEOJSON;

        if (stringOutputFormat != null) {
            outputFormat = GeoSimplifyScript.OutputFormat.valueOf(stringOutputFormat.toUpperCase());
        }

        String algorithmString = (String) params.get("algorithm");

        GeoSimplifyScript.Algorithm algorithm = GeoSimplifyScript.Algorithm.DOUGLAS_PEUCKER;

        if (algorithmString != null) {
            algorithm = GeoSimplifyScript.Algorithm.valueOf(algorithmString.toUpperCase());
        }

        return new GeoSimplifyScript(field, tolerance, nbDecimals, outputFormat, algorithm);
    }
}
