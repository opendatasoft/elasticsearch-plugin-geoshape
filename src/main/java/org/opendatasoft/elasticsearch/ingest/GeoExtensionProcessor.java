package org.opendatasoft.elasticsearch.ingest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTWriter;
import org.opendatasoft.elasticsearch.plugin.GeoUtils;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
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

    private final GeometryFactory geomFactory;
    private final WKTWriter wktWriter;

    private GeoExtensionProcessor(
        String tag,
        String description,
        String field,
        String path,
        Boolean keepShape,
        String shapeField,
        String fixedField,
        String wkbField,
        String hashField,
        String typeField,
        String areaField,
        String bboxField,
        String centroidField
    ) {
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

        PrecisionModel precisionModel = new PrecisionModel(PrecisionModel.FLOATING);
        this.geomFactory = new GeometryFactory(precisionModel, 0);

        this.wktWriter = new WKTWriter();
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

    // WARNING: I wonder if some stuff we do in our plugin can be replaced by the x-pack spatial
    // GeoShapeWithDocValuesFieldMapper index mapper

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws IOException, ParseException {
        GeometryParser geoParser = new GeometryParser(true, true, true);
        List<String> geo_objects_list = getGeoShapeFieldsFromDoc(ingestDocument);
        for (String geoShapeField : geo_objects_list) {

            Object geoShapeObject = ingestDocument.getFieldValue(geoShapeField, Object.class);

            if (geoShapeObject == null) {
                continue;
            }

            // Parse the GeoJSON geometry from the ingestDoc
            org.elasticsearch.geometry.Geometry geom = geoParser.parseGeometry(geoShapeObject);

            // Try to remove some duplicated coordinates. Duplicated coordinates don't make the geom invalid
            // but the GeometryNormalizer won't accept duplicated coords.
            try {
                geom = GeoUtils.removeDuplicateCoordinates(geom);
            } catch (Throwable e) {
                throw new IllegalArgumentException("unable to parse the geometry [" + WellKnownText.toWKT(geom) + "]" + e.getMessage());
            }

            // Can break geometries that cross the dateline
            org.elasticsearch.geometry.Geometry fixedGeom = GeometryNormalizer.apply(Orientation.RIGHT, geom);
            String altWKT = WellKnownText.toWKT(fixedGeom);

            ingestDocument.removeField(geoShapeField);

            if (keepShape) {
                ingestDocument.setFieldValue(geoShapeField + "." + shapeField, geoShapeObject);
            }

            if (fixedField != null) {
                ingestDocument.setFieldValue(geoShapeField + "." + fixedField, altWKT);
            }

            String geomType = fixedGeom.type().toString();

            // compute and add extra geo sub-fields
            // NOTE: elasticsearch.common.geo encodes with Little-Endianess.
            byte[] wkb = WellKnownBinary.toWKB(fixedGeom, ByteOrder.LITTLE_ENDIAN);

            if (hashField != null) ingestDocument.setFieldValue(
                geoShapeField + ".hash",
                String.valueOf(GeoUtils.getHashFromWKB(new BytesRef(wkb)))
            );
            if (wkbField != null) ingestDocument.setFieldValue(geoShapeField + "." + wkbField, wkb);
            if (typeField != null) ingestDocument.setFieldValue(geoShapeField + "." + typeField, geomType);
            if (areaField != null) ingestDocument.setFieldValue(geoShapeField + "." + areaField, GeoUtils.getArea(fixedGeom));
            if (centroidField != null) ingestDocument.setFieldValue(
                geoShapeField + "." + centroidField,
                GeoUtils.getCentroidFromGeom(fixedGeom)
            );
            if (bboxField != null) {
                Coordinate[] coords = GeoUtils.getEnvelope(fixedGeom).getCoordinates();
                if (coords.length >= 4) {
                    ingestDocument.setFieldValue(geoShapeField + "." + bboxField, GeoUtils.getBboxFromCoords(coords));
                } else if (coords.length == 1) {
                    GeoPoint point = new GeoPoint(
                        org.elasticsearch.common.geo.GeoUtils.normalizeLat(coords[0].y),
                        org.elasticsearch.common.geo.GeoUtils.normalizeLon(coords[0].x)
                    );
                    ingestDocument.setFieldValue(geoShapeField + "." + bboxField, Arrays.asList(point, point));
                }
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
        public GeoExtensionProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) {
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
