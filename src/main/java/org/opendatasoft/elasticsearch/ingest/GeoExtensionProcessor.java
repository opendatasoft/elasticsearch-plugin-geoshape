package org.opendatasoft.elasticsearch.ingest;


import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Shape;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.opendatasoft.elasticsearch.plugin.GeoUtils;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.IngestDocument.deepCopyMap;


public class GeoExtensionProcessor extends AbstractProcessor {
    public static final String TYPE = "geo_extension";

    private final String field;
    private final String path;
    private final boolean fix;
    private final String wkbField;
    private final String hashField;
    private final String typeField;
    private final String areaField;
    private final String bboxField;
    private final String centroidField;

    private GeoExtensionProcessor(String tag, String field, String path, boolean fix, String wkbField, String hashField,
                                  String typeField, String areaField, String bboxField, String centroidField)  {
        super(tag);
        this.field = field;
        this.path = path;
        this.fix = fix;
        this.wkbField = wkbField;
        this.hashField = hashField;
        this.typeField = typeField;
        this.areaField = areaField;
        this.bboxField = bboxField;
        this.centroidField = centroidField;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> removeFollowingDuplicatedPoints(Map<String, Object> oldGeoMap) {
        Map<String, Object> geoMap = new HashMap<>();
        geoMap.put("type", oldGeoMap.get("type"));

        List<List<List<Double>>> newCoordinates = new ArrayList<>();
        for (List<List<Double>> bound: (List<List<List<Double>>>) oldGeoMap.get("coordinates")) {
            List<List<Double>> newBound = new ArrayList<>();
            List<Double> lastCoordinates = null;
            for(List<Double> coordinates: bound) {
                if (lastCoordinates == null || ! lastCoordinates.equals(coordinates)) {
                    newBound.add(coordinates);
                    lastCoordinates = coordinates;
                }
            }
            newCoordinates.add(newBound);
        }
        geoMap.put("coordinates", newCoordinates);

        return geoMap;
    }

    private void setAndcleanGeoSourceField(IngestDocument document, String geoShapeField, Map<String, Object> geo_map) {
        /* We need to move geo_shape fields (type, coordinates, etc.) to one lower level under geoshape_x.geo_shape. */

        Map<String, Object> tmp_geo_map = deepCopyMap(geo_map);
        for (Map.Entry<String, Object> geo_shape_field : tmp_geo_map.entrySet()) {
            document.removeField(geoShapeField + "." + geo_shape_field.getKey());
        }
        document.setFieldValue(geoShapeField+".geo_shape", tmp_geo_map);
    }

    @SuppressWarnings("unchecked")
    private List<String> getGeoShapeFieldsFromDoc(IngestDocument ingestDocument) {
        List<String> fields = new ArrayList<>();

        Map<String, Object> baseMap;
        if (path != null) {
            baseMap = ingestDocument.getFieldValue(this.path, Map.class);
        } else {
            baseMap = ingestDocument.getSourceAndMetadata();
        }

        for (String fieldName : baseMap.keySet()) {
            if (Regex.simpleMatch(field, fieldName)) {
                if (path != null) {
                    fieldName = path + "." + fieldName;
                }
                fields.add(fieldName);
            }
        }

        return fields;
    }

    private ShapeBuilder<?,?> getShapeBuilderFromMap(Map<String, Object> geoMap) throws IOException{
        XContentBuilder contentBuilder = JsonXContent.contentBuilder().map(geoMap);

        XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                BytesReference.bytes(contentBuilder).streamInput()
        );

        parser.nextToken();
        return ShapeParser.parse(parser);
    }

    @SuppressWarnings("unchecked")
    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws IOException {
        List<String> geo_objects_list = getGeoShapeFieldsFromDoc(ingestDocument);
        for (String geoShapeField : geo_objects_list) {

            Map<String, Object> geoMap = ingestDocument.getFieldValue(geoShapeField, Map.class);

            ShapeBuilder<?,?> shapeBuilder = getShapeBuilderFromMap(geoMap);

            Shape shape = null;
            try {
                shape = shapeBuilder.build();
            }
            catch (InvalidShapeException ignored) {}

            // fix shapes if needed
            if (shape == null && fix) {
                geoMap = removeFollowingDuplicatedPoints(geoMap);
                shapeBuilder = getShapeBuilderFromMap(geoMap);
                try {
                    shape = shapeBuilder.build();
                } catch (InvalidShapeException ignored) {}
            }

            if (shape == null) {
                throw new IllegalArgumentException("unable to parse shape [" + shapeBuilder.toWKT() + "]");
            }

            setAndcleanGeoSourceField(ingestDocument, geoShapeField, geoMap);

            // compute and add extra geo sub-fields
            Geometry geom = ShapeBuilder.SPATIAL_CONTEXT.getShapeFactory().getGeometryFrom(shape);
            byte[] wkb = new WKBWriter().write(geom);  // elastic will auto-encode this as b64

            if (hashField != null) ingestDocument.setFieldValue(
                    geoShapeField + ".hash", String.valueOf(GeoUtils.getHashFromWKB(new BytesRef(wkb))));
            if (wkbField != null) ingestDocument.setFieldValue(
                    geoShapeField + "." + wkbField, wkb);
            if (typeField != null) ingestDocument.setFieldValue(
                    geoShapeField + "." + typeField, geom.getGeometryType());
            if (areaField != null) ingestDocument.setFieldValue(
                    geoShapeField + "." + areaField, geom.getArea());
            if (centroidField != null) ingestDocument.setFieldValue(
                    geoShapeField + "." + centroidField, GeoUtils.getCentroidFromGeom(geom));
            if (bboxField != null) {
                Coordinate[] coords = geom.getEnvelope().getCoordinates();
                if (coords.length >= 4) ingestDocument.setFieldValue(
                        geoShapeField + "." + bboxField,
                        GeoUtils.getBboxFromCoords(coords));
            }
        }
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public GeoExtensionProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                            Map<String, Object> config) {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");

            String path = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "path");

            boolean fix = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "fix_shape", true);

            boolean needWkb = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "wkb", true);
            String wkbField = null;
            if (needWkb) {
                wkbField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "wkb_field", "wkb");
            }

            boolean needHash = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "hash", true);
            String hashField = null;
            if (needHash) {
                hashField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "hash_field", "hash");
            }

            boolean needType = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "type", true);
            String typeField = null;
            if (needType) {
                typeField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "type_field", "type");
            }

            boolean needArea = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "area", true);
            String areaField = null;
            if (needArea) {
                areaField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "area_field", "area");
            }

            boolean needBbox = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "bbox", true);
            String bboxField = null;
            if (needBbox) {
                bboxField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "bbox_field", "bbox");
            }

            boolean needCentroid = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "centroid", true);
            String centroidField = null;
            if (needCentroid) {
                centroidField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "centroid_field", "centroid");
            }

            return new GeoExtensionProcessor(
                    processorTag,
                    field,
                    path,
                    fix,
                    wkbField,
                    hashField,
                    typeField,
                    areaField,
                    bboxField,
                    centroidField
            );
        }
    }
}
