package org.opendatasoft.elasticsearch.ingest;


import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.legacygeo.parsers.ShapeParser;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.spatial4j.shape.Shape;
import org.elasticsearch.legacygeo.builders.ShapeBuilder;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.opendatasoft.elasticsearch.plugin.GeoUtils;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class GeoExtensionProcessor extends AbstractProcessor {
    public static final String TYPE = "geo_extension";

    private final String field;
    private final String path;
    private final Boolean keepShape;
    private final String shapeField;
    private final String fixedField;
    private final String wkbField;
    private final String hashField;
    private final String typeField;
    private final String areaField;
    private final String bboxField;
    private final String centroidField;

    private GeoExtensionProcessor(String tag, String description, String field, String path, Boolean keepShape, String shapeField,
                                  String fixedField, String wkbField, String hashField, String typeField,
                                  String areaField, String bboxField, String centroidField)  {
        super(tag, description);
        this.field = field;
        this.path = path;
        this.keepShape = keepShape;
        this.shapeField = shapeField;
        this.fixedField = fixedField;
        this.wkbField = wkbField;
        this.hashField = hashField;
        this.typeField = typeField;
        this.areaField = areaField;
        this.bboxField = bboxField;
        this.centroidField = centroidField;
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

    private ShapeBuilder<?,?, ?> getShapeBuilderFromObject(Object object) throws IOException{
        XContentBuilder contentBuilder = JsonXContent.contentBuilder().value(object);

        XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                BytesReference.bytes(contentBuilder).streamInput()
        );

        parser.nextToken();
        return ShapeParser.parse(parser);
    }

    @SuppressWarnings("unchecked")
    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws IOException, ParseException {
        List<String> geo_objects_list = getGeoShapeFieldsFromDoc(ingestDocument);
        for (String geoShapeField : geo_objects_list) {

            Object geoShapeObject = ingestDocument.getFieldValue(geoShapeField, Object.class);

            if (geoShapeObject == null) {
                continue;
            }

            ShapeBuilder<?,?, ?> shapeBuilder = getShapeBuilderFromObject(geoShapeObject);

            // buildS4J() will try to clean up and fix the shape. If it fails, an exception is raised
            // Included fixes:
            // - point deduplication
            // - dateline warping (enforce lon in [-180,180])
            Shape shape = shapeBuilder.buildS4J();

            Geometry geom = ((JtsGeometry) shape).getGeom();

            ingestDocument.removeField(geoShapeField);

            if (keepShape) {
                ingestDocument.setFieldValue(geoShapeField + "." + shapeField, geoShapeObject);
            }

            if (fixedField != null) {
                ingestDocument.setFieldValue(geoShapeField + "." + fixedField, new WKTWriter().write(geom));
            }

            // compute and add extra geo sub-fields
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
                                            String description,
                                            Map<String, Object> config) {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String path = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "path");

            boolean keep_shape = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "keep_original_shape", true);
            String shapeField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "shape_field", "shape");

            boolean fix = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "fix_shape", true);
            String fixedField = null;
            if (fix) {
                fixedField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "fixed_field", "fixed_shape");
            }

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
                    description,
                    field,
                    path,
                    keep_shape,
                    shapeField,
                    fixedField,
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
