package com.opendatasoft.elasticsearch.odsscript;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTWriter;
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier;
import com.vividsolutions.jts.simplify.TopologyPreservingSimplifier;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Base64;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ScriptException;
import org.geotools.geojson.geom.GeometryJSON;

import java.util.HashMap;
import java.util.Map;

/*
 *  Copied from elasticsearch-ext-opendatasoft/GeoPreviewPlugin
 */
class GeoScript extends AbstractSearchScript {

    private final String geoShapeStringFieldName;

    private final double epsilon;
    private final WKTWriter wktWriter;
    private final WKBWriter wkbWriter;
    private final GeometryJSON geometryJSON;
    private final OutputFormat outputFormat;
    private final Algorithm algorithm;
    private final boolean isPoint;
    private final boolean simplify;
    private AtomicReaderContext currentContext;
    private int docId;

    enum OutputFormat {
        GEOJSON,
        WKT,
        WKB
    }

    enum Algorithm {
        DOUGLAS_PEUCKER,
        TOPOLOGY_PRESERVING
    }

    public GeoScript(String geoShapeStringField, double epsilon, int coordDecimalsTrunc, OutputFormat outputFormat, Algorithm algorithm, boolean isPoint, boolean simplify) {
        this.geoShapeStringFieldName = geoShapeStringField;
        this.epsilon = epsilon;
        this.wktWriter = new WKTWriter();
        this.wkbWriter = new WKBWriter();
        this.geometryJSON = new GeometryJSON(coordDecimalsTrunc);
        this.outputFormat = outputFormat;
        this.algorithm = algorithm;
        this.isPoint = isPoint;
        this.simplify = simplify;
    }

    private String outputGeometry(Geometry geo) {

        switch (outputFormat) {
            case WKT:
                return wktWriter.write(geo);
            case WKB:
                return Base64.encodeBytes(wkbWriter.write(geo));
            default:
                return geometryJSON.toString(geo);
        }
    }

    private Geometry getSimplifiedShape(Geometry geometry) {
        switch (algorithm) {
            case TOPOLOGY_PRESERVING:
                return TopologyPreservingSimplifier.simplify(geometry, this.epsilon);
            default:
                return DouglasPeuckerSimplifier.simplify(geometry, this.epsilon);
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        currentContext = context;
        super.setNextReader(context);
    }

    @Override
    public void setNextDocId(int doc) {
        docId = doc;
        super.setNextDocId(doc);
    }

    @Override
    public Object run() {

        GeometryFactory geometryFactory = new GeometryFactory();
        Map<String, String> resMap = new HashMap<>();

        if (isPoint) {
            ScriptDocValues.GeoPoints geoPoints = (ScriptDocValues.GeoPoints) doc().get(this.geoShapeStringFieldName);
            Geometry geom = geometryFactory.createPoint(new Coordinate(geoPoints.getLon(), geoPoints.getLat()));

            resMap.put("geom", outputGeometry(geom));
            resMap.put("type", geom.getGeometryType());
            resMap.put("real_type", geom.getGeometryType());


        } else {
            FieldMapper mapper = doc().mapperService().smartNameFieldMapper(this.geoShapeStringFieldName + ".wkb");
            SortedBinaryDocValues bytes = doc().fieldDataService().getForField(mapper).load(currentContext).getBytesValues();
            bytes.setDocument(docId);
            BytesRef wkb = bytes.valueAt(0);
            WKBReader reader = new WKBReader();

            try {
                Geometry geometry = reader.read(wkb.bytes);
                String realType = geometry.getGeometryType();
                if (simplify) {
                    geometry = getSimplifiedShape(geometry);
                }
                if (!geometry.isEmpty()) {
                    resMap.put("geom", outputGeometry(geometry));
                    resMap.put("type", geometry.getGeometryType());
                    resMap.put("real_type", realType);
                } else {
                    // If the simplified polygon is empty because it was too small, return a point

                    Geometry point = geometryFactory.createPoint(geometry.getCoordinate());

                    resMap.put("geom", outputGeometry(point));
                    resMap.put("type", "SimplificationPoint");
                }
            } catch (ParseException e) {
                throw new ScriptException("Can't parse WKB", e);
            }
        }
        return resMap;
    }
}
