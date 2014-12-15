package com.opendatasoft.elasticsearch.index.mapper.geo;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.TermQueryPrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.core.BinaryFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.core.TypeParsers;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;
import org.geotools.geojson.geom.GeometryJSON;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class GeoMapper2 extends GeoShapeFieldMapper{


    public static final String CONTENT_TYPE = "geo";

    public static class Names {
        public static final String TREE = "tree";
        public static final String TREE_GEOHASH = "geohash";
        public static final String TREE_QUADTREE = "quadtree";
        public static final String TREE_LEVELS = "tree_levels";
        public static final String TREE_PRESISION = "precision";
        public static final String DISTANCE_ERROR_PCT = "distance_error_pct";
        public static final String STRATEGY = "strategy";
        public static final String WKB = "wkb";
        public static final String TYPE = "type";

    }

    public static class Defaults {

        public static final String TREE = Names.TREE_GEOHASH;
        public static final String STRATEGY = SpatialStrategy.RECURSIVE.getStrategyName();
        public static final int GEOHASH_LEVELS = GeoUtils.geoHashLevelsForPrecision("50m");
        public static final int QUADTREE_LEVELS = GeoUtils.quadTreeLevelsForPrecision("50m");
        public static final double DISTANCE_ERROR_PCT = 0.025d;

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexed(false);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setStoreTermVectors(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY);
            FIELD_TYPE.freeze();
        }

    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, GeoMapper2> {


        private BinaryFieldMapper.Builder wkbBuilder = new BinaryFieldMapper.Builder(Names.WKB);
        private StringFieldMapper.Builder typeBuilder = new StringFieldMapper.Builder(Names.TYPE);

        private String tree = Defaults.TREE;
        private String strategyName = Defaults.STRATEGY;
        private int treeLevels = 0;
        private double precisionInMeters = -1;
        private double distanceErrorPct = Defaults.DISTANCE_ERROR_PCT;

        private SpatialPrefixTree prefixTree;

        protected Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE));
        }
//
//        public Builder fieldDataSettings(Settings settings) {
//            this.fieldDataSettings = settings;
//            return builder;
//        }

        public Builder tree(String tree) {
            this.tree = tree;
            return this;
        }

        public Builder strategy(String strategy) {
            this.strategyName = strategy;
            return this;
        }

        public Builder treeLevelsByDistance(double meters) {
            this.precisionInMeters = meters;
            return this;
        }

        public Builder treeLevels(int treeLevels) {
            this.treeLevels = treeLevels;
            return this;
        }

        public Builder distanceErrorPct(double distanceErrorPct) {
            this.distanceErrorPct = distanceErrorPct;
            return this;
        }

        @Override
        public GeoMapper2 build(BuilderContext context) {
            final FieldMapper.Names names = buildNames(context);

            if (Names.TREE_GEOHASH.equals(tree)) {
                prefixTree = new GeohashPrefixTree(ShapeBuilder.SPATIAL_CONTEXT, getLevels(treeLevels, precisionInMeters, Defaults.GEOHASH_LEVELS, true));
            } else if (Names.TREE_QUADTREE.equals(tree)) {
                prefixTree = new QuadPrefixTree(ShapeBuilder.SPATIAL_CONTEXT, getLevels(treeLevels, precisionInMeters, Defaults.QUADTREE_LEVELS, false));
            } else {
                throw new ElasticsearchIllegalArgumentException("Unknown prefix tree type [" + tree + "]");
            }

            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(ContentPath.Type.FULL);

            context.path().add(name);

            BinaryFieldMapper wkbMapper = wkbBuilder.docValues(true).build(context);
            StringFieldMapper typeMapper = typeBuilder.tokenized(false).docValues(true).includeInAll(false).omitNorms(true).index(true).build(context);

            context.path().remove();
            context.path().pathType(origPathType);


            return new GeoMapper2(names, prefixTree, strategyName, distanceErrorPct, wkbMapper, typeMapper, fieldDataSettings, docValues, fieldType, postingsProvider,
                    docValuesProvider, multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    private static final int getLevels(int treeLevels, double precisionInMeters, int defaultLevels, boolean geoHash) {
        if (treeLevels > 0 || precisionInMeters >= 0) {
            return Math.max(treeLevels, precisionInMeters >= 0 ? (geoHash ? GeoUtils.geoHashLevelsForPrecision(precisionInMeters)
                    : GeoUtils.quadTreeLevelsForPrecision(precisionInMeters)) : 0);
        }
        return defaultLevels;
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new GeoMapper2.Builder(name);
            TypeParsers.parseField(builder, name, node, parserContext);

            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (Names.TREE.equals(fieldName)) {
                    builder.tree(fieldNode.toString());
                } else if (Names.TREE_LEVELS.equals(fieldName)) {
                    builder.treeLevels(Integer.parseInt(fieldNode.toString()));
                } else if (Names.TREE_PRESISION.equals(fieldName)) {
                    builder.treeLevelsByDistance(DistanceUnit.parse(fieldNode.toString(), DistanceUnit.DEFAULT, DistanceUnit.DEFAULT));
                } else if (Names.DISTANCE_ERROR_PCT.equals(fieldName)) {
                    builder.distanceErrorPct(Double.parseDouble(fieldNode.toString()));
                } else if (Names.STRATEGY.equals(fieldName)) {
                    builder.strategy(fieldNode.toString());
                }
            }

            return builder;
        }
    }

    private final BinaryFieldMapper wkbMapper;
    private final StringFieldMapper typeMapper;
    private final Pattern pattern;

    private final PrefixTreeStrategy defaultStrategy;
    private final RecursivePrefixTreeStrategy recursiveStrategy;
    private final TermQueryPrefixTreeStrategy termStrategy;


    public GeoMapper2(FieldMapper.Names names,
                      SpatialPrefixTree tree, String defaultStrategyName, double distanceErrorPct,
                      BinaryFieldMapper wkbMapper, StringFieldMapper typeMapper,
                      @Nullable Settings fieldDataSettings, Boolean docValues, FieldType fieldType,
                      PostingsFormatProvider postingsProvider, DocValuesFormatProvider docValuesProvider,
                      MultiFields multiFields, CopyTo copyTo) {
        super(names, tree, defaultStrategyName, distanceErrorPct, fieldType, postingsProvider, docValuesProvider, multiFields, copyTo);
//        super(names, 1, fieldType, docValues, null, null, postingsProvider, docValuesProvider, null, null, fieldDataSettings , null, multiFields, copyTo);
        this.wkbMapper = wkbMapper;
        this.typeMapper = typeMapper;
        this.pattern = Pattern.compile("^.*type\":\"([^\"]+).*");

        this.recursiveStrategy = new RecursivePrefixTreeStrategy(tree, names.indexName());
        this.recursiveStrategy.setDistErrPct(distanceErrorPct);
        this.termStrategy = new TermQueryPrefixTreeStrategy(tree, names.indexName());
        this.termStrategy.setDistErrPct(distanceErrorPct);
        this.defaultStrategy = resolveStrategy(defaultStrategyName);


    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return null;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        try {
            ShapeBuilder shapeBuilder = context.parseExternalValue(ShapeBuilder.class);

            if (shapeBuilder == null) {
                shapeBuilder = ShapeBuilder.parse(context.parser());
                if (shapeBuilder == null) {
                    return;
                }
            }

            Field[] fields = defaultStrategy.createIndexableFields(shapeBuilder.build());
            if (fields == null || fields.length == 0) {
                return;
            }
            for (Field field : fields) {
                if (!customBoost()) {
                    field.setBoost(boost);
                }
                if (context.listener().beforeFieldAdded(this, field, context)) {
                    context.doc().add(field);
                }
            }

            String geoJson = shapeBuilder.toString();


            Matcher matcher = this.pattern.matcher(geoJson);

            matcher.find();

            String type = matcher.group(1);

            geoJson = geoJson.replaceFirst(type, getGeoJsonType(type));

            typeMapper.parse(context.createExternalValueContext(getGeoJsonType(type)));

            parseWkb(context, geoJson);

        } catch (Exception e) {
            throw new MapperParsingException("failed to parse [" + names.fullName() + "]", e);
        }
    }

    private String getGeoJsonType(String geoType) throws IOException {
        if (geoType.equals("point")) {
            return "Point";
        }
        if (geoType.equals("multipoint")) {
            return "MultiPoint";
        }
        if (geoType.equals("linestring")) {
            return "LineString";
        }
        if (geoType.equals("multilinestring")) {
            return "MultiLineString";
        }
        if (geoType.equals("polygon")) {
            return "Polygon";
        }
        if (geoType.equals("multipolygon")) {
            return "MultiPolygon";
        }
        if (geoType.equals("geometrycollection")) {
            return "GeometryCollection";
        }

        throw new IOException("Geo Type unknown");
    }

    /**
     * Writes the WKB serialization of the <tt>shape</tt> to the given ParseContext
     * as an external value.
     */
    private void parseWkb(ParseContext context, String geoJson) throws IOException {


        GeometryJSON geometryJSON = new GeometryJSON();

        Geometry geom = geometryJSON.read(geoJson);

//        Geometry geom = ShapeBuilder.SPATIAL_CONTEXT.getGeometryFrom(shapeBuilder.build());
        byte[] wkb = new WKBWriter().write(geom);

        wkbMapper.parse(context.createExternalValueContext(wkb));

        BinaryFieldMapper.CustomBinaryDocValuesField f = (BinaryFieldMapper.CustomBinaryDocValuesField) context.doc().getByKey(names().indexName());

        if (f == null) {
            f = new BinaryFieldMapper.CustomBinaryDocValuesField(names().indexName(), wkb);
            context.doc().addWithKey(names().indexName(), f);
        } else {
            f.add(wkb);
        }

    }


    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field("type", contentType());

        // TODO: Come up with a better way to get the name, maybe pass it from builder
        if (defaultStrategy.getGrid() instanceof GeohashPrefixTree) {
            // Don't emit the tree name since GeohashPrefixTree is the default
            // Only emit the tree levels if it isn't the default value
            if (includeDefaults || defaultStrategy.getGrid().getMaxLevels() != Defaults.GEOHASH_LEVELS) {
                builder.field(Names.TREE_LEVELS, defaultStrategy.getGrid().getMaxLevels());
            }
        } else {
            builder.field(Names.TREE, Names.TREE_QUADTREE);
            if (includeDefaults || defaultStrategy.getGrid().getMaxLevels() != Defaults.QUADTREE_LEVELS) {
                builder.field(Names.TREE_LEVELS, defaultStrategy.getGrid().getMaxLevels());
            }
        }

        if (includeDefaults || defaultStrategy.getDistErrPct() != Defaults.DISTANCE_ERROR_PCT) {
            builder.field(Names.DISTANCE_ERROR_PCT, defaultStrategy.getDistErrPct());
        }
    }


    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }


    @Override
    public void traverse(FieldMapperListener fieldMapperListener) {
        super.traverse(fieldMapperListener);
        wkbMapper.traverse(fieldMapperListener);
        typeMapper.traverse(fieldMapperListener);
    }


    @Override
    public void close() {
        super.close();
        wkbMapper.close();
        typeMapper.close();
    }

//    @Override
//    public Object value(Object value) {
//        throw new UnsupportedOperationException("GeoShape fields cannot be converted to String values");
//    }

    public PrefixTreeStrategy defaultStrategy() {
        return this.defaultStrategy;
    }

    public PrefixTreeStrategy recursiveStrategy() {
        return this.recursiveStrategy;
    }

    public PrefixTreeStrategy termStrategy() {
        return this.termStrategy;
    }


    public PrefixTreeStrategy resolveStrategy(String strategyName) {
        if (SpatialStrategy.RECURSIVE.getStrategyName().equals(strategyName)) {
            return recursiveStrategy;
        }
        if (SpatialStrategy.TERM.getStrategyName().equals(strategyName)) {
            return termStrategy;
        }
        throw new ElasticsearchIllegalArgumentException("Unknown prefix tree strategy [" + strategyName + "]");
    }
}
