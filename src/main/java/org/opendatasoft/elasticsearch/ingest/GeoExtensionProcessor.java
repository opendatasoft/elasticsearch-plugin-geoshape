package org.opendatasoft.elasticsearch.ingest;


import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.legacygeo.XShapeCollection;
import org.elasticsearch.legacygeo.builders.ShapeBuilder;
import org.elasticsearch.legacygeo.parsers.ShapeParser;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.locationtech.spatial4j.shape.jts.JtsPoint;
import org.opendatasoft.elasticsearch.plugin.GeoUtils;

import java.io.IOException;
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
    private final WKTReader wktReader;

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

        PrecisionModel precisionModel = new PrecisionModel(PrecisionModel.FLOATING);
        this.geomFactory = new GeometryFactory(precisionModel, 0);

        this.wktWriter = new WKTWriter();
        this.wktReader = new WKTReader();
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

    private class GeometryWithWkt {
        public Geometry geometry;
        public String wkt;
    }

    private GeometryWithWkt s4jToJts(Shape shape) {
        // Convert a Spatial4J shape into a JTSGeometry
        // A Spatial4J shape is basically either:
        // - a JtsPoint
        // - a JtsGeometry
        // - an XShapeCollection (multi-* and GeometryCollection)
        // There is a special case for MultiPoint since the WKTWriter of JTS
        // is not compliant with the wkt reader of ES.

        GeometryWithWkt geomWithWkt = new GeometryWithWkt();
        Geometry geom = null;

        String altWKT = null;
        if (shape instanceof JtsPoint) {
            geom = ((JtsPoint) shape).getGeom();
        } else if (shape instanceof JtsGeometry) {
            geom = ((JtsGeometry) shape).getGeom();
        } else if (shape instanceof XShapeCollection) {
            XShapeCollection collection = (XShapeCollection) shape;
            ArrayList<Geometry> geoms = new ArrayList<>(collection.size());
            ArrayList<String> wkts = new ArrayList<>(collection.size());

            boolean hasWkt = false;
            for (int i = 0; i < collection.size(); i++) {
                GeometryWithWkt g = s4jToJts(collection.get(i));
                geoms.add(g.geometry);
                wkts.add(g.wkt);
                if (g.wkt != null) {
                    hasWkt = true;
                }
            }

            if (hasWkt) {
                // if it has a WKT, it means it is a geometrycolleciton
                // containing a multipoint
                altWKT = "GEOMETRYCOLLECTION(";
                for (int i = 0; i < collection.size(); i++) {
                    if (wkts.get(i) != null) {
                        altWKT += wkts.get(i);
                    } else {
                        altWKT += wktWriter.write(geoms.get(i));
                    }
                    if (i < collection.size() - 1) {
                        altWKT += ",";
                    }
                }
                altWKT += ")";
            }
            geom = geomFactory.buildGeometry(geoms);

            if (geom.getGeometryType() == "MultiPoint") {
                // special case of multipoint where the WKT must be corrected
                // ES wants multipoint without extra parenthesis between points
                altWKT = wktWriter.write(geom).replace("((", "(").replace("))", ")").replace("), (", ", ");
            }
        }
        geomWithWkt.geometry = geom;
        geomWithWkt.wkt = altWKT;
        return geomWithWkt;
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

            // From an input Object that represents a GeoJSON we need to:
            // - store a WKT version of the geometry, that will also be indexed as a geometry by ES
            // - make sure this WKT does not have duplicated points, since ES refuse them
            // - make sure this WKT geometry is equal (or close enough) to the geometry indexed by ES (why ?).
            //   It means if ES splits the geometry around the dateline, we want to do the same
            // - compute its area
            // - compute its WKB representation
            // - compute its centroid
            //
            // We currently do this by:
            // 1/ creating a Spatial4J shape from an Object (thanks to legacygeo.ShapeBuilder)
            // 2/ converting this Spatial4J shape to a JTSGeometry
            // 3/ call area(), wkbwriter(), centroid() and wktwriter on the JTSGeometry
            //
            // 2/ could be avoided but the S4J WKTWriter is of poor quality (e.g. a multipoint can be represented as a geometrycollection of points)
            // 1/ could be avoided if we find how to create a JTSGeometry from an Object

            ShapeBuilder<?,?, ?> shapeBuilder = getShapeBuilderFromObject(geoShapeObject);

            // buildS4J() will try to clean up and fix the shape. If it fails, an exception is raised
            // Included fixes:
            // - dateline warping (enforce lon in [-180,180])
            Shape shape = null;
            try {
                try {
                    shape = shapeBuilder.buildS4J();
                } catch (InvalidShapeException e) {
                    // buildS4J does not always deduplicate points
                    shapeBuilder = GeoUtils.removeDuplicateCoordinates(shapeBuilder);
                    shape = shapeBuilder.buildS4J();
                }
            }
            catch (Throwable e) {
                // sometimes it is still not enough
                // e.g. when non-EPSG:4326 coordinates are input
                // buildS4J will try to warp date lines and may generate lots of small components
                // which could yield a GeometryCollection, which raises an AssertionError
                // So, we catch here both Error (AssertionError) and Exceptions
                throw new IllegalArgumentException("Unable to parse shape [" + shapeBuilder.toWKT() + "]: " + e.getMessage());
            }

            GeometryWithWkt geometryWithWkt = s4jToJts(shape);
            Geometry geom = geometryWithWkt.geometry;
            String altWKT = geometryWithWkt.wkt;

            if (geom == null) {
                throw new IllegalArgumentException("Unable to parse shape [" + shapeBuilder.toWKT() + "]");
            }

            ingestDocument.removeField(geoShapeField);

            if (keepShape) {
                ingestDocument.setFieldValue(geoShapeField + "." + shapeField, geoShapeObject);
            }

            if (fixedField != null) {
                ingestDocument.setFieldValue(geoShapeField + "." + fixedField,
                        altWKT != null ? altWKT : wktWriter.write(geom));
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
                if (coords.length >= 4) {
                    ingestDocument.setFieldValue(
                            geoShapeField + "." + bboxField,
                            GeoUtils.getBboxFromCoords(coords));
                } else if (coords.length == 1) {
                    GeoPoint point = new GeoPoint(
                            org.elasticsearch.common.geo.GeoUtils.normalizeLat(coords[0].y),
                            org.elasticsearch.common.geo.GeoUtils.normalizeLon(coords[0].x)
                    );
                    ingestDocument.setFieldValue(
                            geoShapeField + "." + bboxField,
                            Arrays.asList(point, point));
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
