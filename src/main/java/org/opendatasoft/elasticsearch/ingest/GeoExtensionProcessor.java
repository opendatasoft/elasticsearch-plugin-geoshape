package org.opendatasoft.elasticsearch.ingest;


import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Shape;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.locationtech.spatial4j.shape.ShapeCollection;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.opendatasoft.elasticsearch.plugin.GeoUtils;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.ingest.IngestDocument.deepCopyMap;


public class GeoExtensionProcessor extends AbstractProcessor {
    public static final String TYPE = "geo_extension";

    private final String geo_field_prefix;
    private final String geo_field_path;
    private final String wkb_field;
    private final String type_field;
    private final String area_field;
    private final String bbox_field;
    private final String centroid_field;

    private GeoExtensionProcessor(String tag, String geo_field_path, String geo_field_prefix, String wkb_field,
                                  String type_field, String area_field, String bbox_field, String centroid_field)  {
        super(tag);
        this.geo_field_path = geo_field_path;
        this.geo_field_prefix = geo_field_prefix;
        this.wkb_field = wkb_field;
        this.type_field = type_field;
        this.area_field = area_field;
        this.bbox_field = bbox_field;
        this.centroid_field = centroid_field;
    }

    private Geometry getGeometryFromShape(Shape shape) {
        if (shape instanceof ShapeCollection) {
            GeometryFactory geometryFactory = JtsSpatialContext.GEO.getGeometryFactory();
            Geometry [] geometries = new Geometry[((ShapeCollection) shape).size()];
            int i = 0;
            for(Object s :((ShapeCollection) shape).getShapes()) {
                geometries[i++] = getGeometryFromShape((Shape)s);
            }
            return geometryFactory.createGeometryCollection(geometries);
        } else if (shape instanceof JtsGeometry) {
            return ((JtsGeometry) shape).getGeom();
        } else {
            return JtsSpatialContext.GEO.getGeometryFrom(shape);
        }
    }

    private String mapToXContent(Map<String, Object> geo_map) throws IOException {
        XContentBuilder geo_json_builder = jsonBuilder();
        geo_json_builder.startObject();
        for (Map.Entry<String, Object> entry : geo_map.entrySet()) {
            geo_json_builder.field(entry.getKey(), entry.getValue());
        }
        geo_json_builder.endObject();
        return Strings.toString(geo_json_builder);
    }

    private Object shapeBuilderFromString(String geo_json) throws IOException {
        XContentType xContentType = XContentType.JSON;
        XContentParser parser = xContentType.xContent().createParser(NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE, geo_json);
        parser.nextToken();
        return ShapeParser.parse(parser);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> removeFollowingDuplicatedPoints(Map<String, Object> old_geo_map) {
        Map<String, Object> new_geo_map = new HashMap<>();
        for (Map.Entry<String, Object> geo_entry : old_geo_map.entrySet()) {
            if (geo_entry.getKey().equals("coordinates")) {
                ArrayList<ArrayList<ArrayList<Double>>> new_coordinates = new ArrayList<>();
                for (Object old_coords_group : ((ArrayList) geo_entry.getValue())) {
                    ArrayList<ArrayList<Double>> new_coords_group = new ArrayList<>();
                    List<Double> last_point = Arrays.asList(999., 999.);
                    for (Object point : ((ArrayList) old_coords_group)) {
                        if (!point.equals(last_point)) {
                            new_coords_group.add(((ArrayList<Double>) point));
                        }
                        last_point = (List<Double>) point;
                    }
                    new_coordinates.add(new_coords_group);
                }
                new_geo_map.put(geo_entry.getKey(), new_coordinates);

            } else {
                new_geo_map.put(geo_entry.getKey(), geo_entry.getValue());
            }
        }
        return new_geo_map;
    }

    private void setAndcleanGeoSourceField(IngestDocument document, String cur_geo_source_fullpath, Map<String, Object> geo_map) {
        /* We need to move geo_shape fields (type, coordinates, etc.) to one lower level under geoshape_x.geo_shape. */

        Map<String, Object> tmp_geo_map = deepCopyMap(geo_map);
        for (Map.Entry<String, Object> geo_shape_field : tmp_geo_map.entrySet()) {
            document.removeField(cur_geo_source_fullpath + "." + geo_shape_field.getKey());
        }
        document.setFieldValue(cur_geo_source_fullpath+".geo_shape", tmp_geo_map);
    }

    @SuppressWarnings("unchecked")
    private ArrayList<String> getGeoShapeFieldsFromDoc(IngestDocument document) {
        ArrayList<String> geo_objects_list = new ArrayList<>();
        Pattern re_pattern = Pattern.compile(geo_field_prefix + "(\\w|_\\w)");
        boolean is_geoshape;
        Object document_source;

        if (geo_field_path == null) document_source = document.getSourceAndMetadata();
        else document_source = document.getSourceAndMetadata().getOrDefault(geo_field_path, null);

        if (document_source == null)
            return geo_objects_list;

        for (String document_field_key : ((Map<String, Object>) document_source).keySet()) {
            Matcher re_matcher = re_pattern.matcher(document_field_key);
            is_geoshape = re_matcher.matches();
            if (is_geoshape)
                geo_objects_list.add(document_field_key);
        }
        return geo_objects_list;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(IngestDocument document) throws IOException {
        ArrayList<String> geo_objects_list = getGeoShapeFieldsFromDoc(document);
        for (String cur_geo_source_field : geo_objects_list) {
            String cur_geo_source_fullpath;
            if (geo_field_path == null) cur_geo_source_fullpath = cur_geo_source_field;
            else cur_geo_source_fullpath = geo_field_path + '.' + cur_geo_source_field;

            Object geo_object = document.getFieldValue(cur_geo_source_fullpath, Object.class);
            Map<String, Object> geo_map = (Map<String, Object>) geo_object;
            String geo_json = mapToXContent(geo_map);
            Object shapeBuilder = shapeBuilderFromString(geo_json);

            // fix shapes if needed
            Shape shape;
            boolean patch_self_referencing = false;
            try {
                shape = ((ShapeBuilder) shapeBuilder).build();
                patch_self_referencing = true;
            }
            catch (InvalidShapeException e) {
                geo_map = removeFollowingDuplicatedPoints(geo_map);
                geo_json = mapToXContent(geo_map);
                shapeBuilder = shapeBuilderFromString(geo_json);
                // ToDo: we must raise an elastic error here if it fails, in order to tell ODS-python-platform that the
                //  shapes bulk is corrupted and then records are not indexed
                shape = ((ShapeBuilder) shapeBuilder).build();
            }
            setAndcleanGeoSourceField(document, cur_geo_source_fullpath, geo_map);

            // compute and add extra geo sub-fields
            Geometry geom = getGeometryFromShape(shape);
            byte[] wkb = new WKBWriter().write(geom);  // elastic will auto-encode this as b64

            document.setFieldValue(cur_geo_source_fullpath + ".hash", String.valueOf(GeoUtils.getHashFromWKB(new BytesRef(wkb))));
            if (!wkb_field.equals("false")) document.setFieldValue(cur_geo_source_fullpath + "." + wkb_field, wkb);
            if (!type_field.equals("false")) document.setFieldValue(cur_geo_source_fullpath + "." + type_field, geom.getGeometryType());
            if (!area_field.equals("false")) document.setFieldValue(cur_geo_source_fullpath + "." + area_field, geom.getArea());
            if (!centroid_field.equals("false")) document.setFieldValue(cur_geo_source_fullpath + "." + centroid_field,
                    GeoUtils.getCentroidFromGeom(geom));
            if (!bbox_field.equals("false")) {
                Coordinate[] coords = geom.getEnvelope().getCoordinates();
                if (coords.length >= 4) document.setFieldValue(cur_geo_source_fullpath + "." + bbox_field,
                        GeoUtils.getBboxFromCoords(coords));
            }
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public GeoExtensionProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                            Map<String, Object> config) {
            String geo_field_path = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "geo_field_path");
            String geo_field_prefix = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "geo_field_prefix", "geo_shape");
            String wkb_field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "wkb_field", "true");
            if (wkb_field.equals("true")) wkb_field = "wkb";
            String type_field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "type_field", "true");
            if (type_field.equals("true")) type_field = "type";
            String area_field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "area_field", "true");
            if (area_field.equals("true")) area_field = "area";
            String bbox_field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "bbox_field", "true");
            if (bbox_field.equals("true")) bbox_field = "bbox";
            String centroid_field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "centroid_field", "true");
            if (centroid_field.equals("true")) centroid_field = "centroid";

            return new GeoExtensionProcessor(
                    processorTag,
                    geo_field_path,
                    geo_field_prefix,
                    wkb_field,
                    type_field,
                    area_field,
                    bbox_field,
                    centroid_field
            );
        }
    }
}
