package org.opendatasoft.elasticsearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.search.lookup.SearchLookup;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.geojson.GeoJsonWriter;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;
import org.opendatasoft.elasticsearch.plugin.GeoUtils;
import org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.GeoShape;
import org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.InternalGeoShape;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.GeoShape.Algorithm.TOPOLOGY_PRESERVING;


public class ScriptGeoSimplify implements ScriptEngine {
    public static final ScriptContext<GeoSearchLeafFactory> CONTEXT = new ScriptContext<>("geo_simplify", GeoSearchLeafFactory.class);

    @Override
    public String getType() {
        return "geo_extension_scripts";
    }

    @Override
    public <T> T compile(String scriptName, String scriptSource,
                         ScriptContext<T> context, Map<String, String> params) {
        if (!context.equals(FieldScript.CONTEXT)) {
            throw new IllegalArgumentException(getType()
                    + " scripts cannot be used for context ["
                    + context.name + "]");
        }
        if ("geo_simplify".equals(scriptName)) {
            FieldScript.Factory factory = GeoSearchLeafFactory::new;
            return context.factoryClazz.cast(factory);
        }
        throw new IllegalArgumentException("Unknown script name " + scriptSource);
    }

    @Override
    public Set<ScriptContext<?>> getSupportedContexts() {
        return Collections.singleton(CONTEXT);
    }

    @Override
    public void close() {
        // optionally close resources
    }

    private static class GeoSearchLeafFactory implements FieldScript.LeafFactory {
        private final Map<String, Object> params;
        private final SearchLookup lookup;
        private final String field;
        private final double tolerance;
        InternalGeoShape.OutputFormat output_format;
        GeoShape.Algorithm algorithm;
//        private final int geojson_decimals;
        GeoJsonWriter geoJsonWriter;


        private Geometry getSimplifiedShape(Geometry geometry) {
            if (algorithm == TOPOLOGY_PRESERVING)
                return TopologyPreservingSimplifier.simplify(geometry, tolerance);
            else
                return DouglasPeuckerSimplifier.simplify(geometry, tolerance);
        }

        private GeoSearchLeafFactory(
                Map<String, Object> params, SearchLookup lookup) {

            if (params.isEmpty()) {
                throw new IllegalArgumentException("[params] field is mandatory");

            }
            if (!params.containsKey("field")) {
                throw new IllegalArgumentException("Missing mandatory parameter [field]");
            }
            if (!params.containsKey("zoom")) {
                throw new IllegalArgumentException("Missing mandatory parameter [zoom]");
            }
            this.params = params;
            this.lookup = lookup;
            field = params.get("field").toString();
            int zoom = (int) params.get("zoom");
            tolerance = 360 / (256 * Math.pow(zoom, 3));

            output_format = InternalGeoShape.OutputFormat.GEOJSON;
            if (params.containsKey("output_format")) {
                String string_output_format = params.get("output_format").toString();
                if (string_output_format != null)
                    output_format = InternalGeoShape.OutputFormat.valueOf(string_output_format.toUpperCase(Locale.getDefault()));
            }

            algorithm = GeoShape.Algorithm.DOUGLAS_PEUCKER;
            if (params.containsKey("algorithm")) {
                String algorithm_string = params.get("algorithm").toString();
                if (algorithm_string != null)
                    algorithm = GeoShape.Algorithm.valueOf(algorithm_string.toUpperCase(Locale.getDefault()));
            }

//            geojson_decimals = 20;
            geoJsonWriter = new GeoJsonWriter();
        }

        @Override
        public FieldScript newInstance(LeafReaderContext context) {
            return new FieldScript(params, lookup, context) {
                @Override
                public Object execute() {
                    Map<String, String> resMap = new HashMap<>();

                    BytesRef wkb;
                    try {
                        ScriptDocValues<?> values_list = getDoc().get(field);
                        wkb = (BytesRef) values_list.get(0);
                    }
                    catch (Exception e) {
                        return resMap;
                    }

                    GeometryFactory geometryFactory = new GeometryFactory();
                    try {
                        Geometry geom = new WKBReader().read(wkb.bytes);
                        String realType = geom.getGeometryType();
                        geom = getSimplifiedShape(geom);
                        if (!geom.isEmpty()) {
                            resMap.put("shape", GeoUtils.exportGeoTo(geom, output_format, geoJsonWriter));
                            resMap.put("type", geom.getGeometryType());
                            resMap.put("real_type", realType);
                        } else {
                            // If the simplified polygon is empty because it was too small, return a point
                            Geometry point = geometryFactory.createPoint(geom.getCoordinate());
                            resMap.put("shape", GeoUtils.exportGeoTo(point, output_format, geoJsonWriter));
                            resMap.put("type", "SimplificationPoint");
                        }
                    } catch (ParseException e) {
                        throw new ScriptException("Can't parse WKB", e.getCause(), Collections.emptyList(),
                                "geo_simplified", "geo_extension_scripts");
                    }

                    return resMap;
                }

            };
        }

    }

}

