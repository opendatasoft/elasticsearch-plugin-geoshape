package com.opendatasoft.elasticsearch.index.mapper.geo;

import com.opendatasoft.elasticsearch.plugin.geo.GeoPluginUtils;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.ShapeCollection;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.*;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class GeoMapper extends GeoShapeFieldMapper{


    public static final String CONTENT_TYPE = "geo";

    public static class Names {
        public static final String TREE = "tree";
        public static final String TREE_GEOHASH = "geohash";
        public static final String TREE_QUADTREE = "quadtree";
        public static final String TREE_LEVELS = "tree_levels";
        public static final String TREE_PRESISION = "precision";
        public static final String DISTANCE_ERROR_PCT = "distance_error_pct";
        public static final String ORIENTATION = "orientation";
        public static final String STRATEGY = "strategy";
        public static final String BOOST_PRECISION = "boost_precision";
        public static final String WKB = "wkb";
        public static final String WKB_TEXT = "wkbtext";
        public static final String TYPE = "type";
        public static final String AREA = "area";
        public static final String BBOX = "bbox";
        public static final String HASH = "hash";
        public static final String CENTROID = "centroid";

    }

    public static class Defaults {

        public static final String TREE = Names.TREE_GEOHASH;
        public static final String STRATEGY = SpatialStrategy.RECURSIVE.getStrategyName();
        public static final int GEOHASH_LEVELS = GeoUtils.geoHashLevelsForPrecision("50m");
        public static final int QUADTREE_LEVELS = GeoUtils.quadTreeLevelsForPrecision("50m");
        public static final double DISTANCE_ERROR_PCT = 0.025d;
        public static final ShapeBuilder.Orientation ORIENTATION = ShapeBuilder.Orientation.RIGHT;
        public static final double BOOST_PRECISION = 0;

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

    public static class Builder extends AbstractFieldMapper.Builder<Builder, GeoMapper> {


        private BinaryFieldMapper.Builder wkbBuilder = new BinaryFieldMapper.Builder(Names.WKB);
        private StringFieldMapper.Builder typeBuilder = new StringFieldMapper.Builder(Names.TYPE);
        private DoubleFieldMapper.Builder areaBuilder = new DoubleFieldMapper.Builder(Names.AREA);
        private GeoPointFieldMapper.Builder bboxBuilder = new GeoPointFieldMapper.Builder(Names.BBOX);
        private StringFieldMapper.Builder hashBuilder = new StringFieldMapper.Builder(Names.HASH);
//        private StringFieldMapper.Builder wkbTextBuilder = new StringFieldMapper.Builder(Names.WKB_TEXT);
        private GeoPointFieldMapper.Builder centroidBuilder = new GeoPointFieldMapper.Builder(Names.CENTROID);

        private String tree = Defaults.TREE;
        private String strategyName = Defaults.STRATEGY;
        private double boostPrecision = 0;
        private int treeLevels = 0;
        private double precisionInMeters = -1;
        private double distanceErrorPct = Defaults.DISTANCE_ERROR_PCT;

        private ShapeBuilder.Orientation orientation = Defaults.ORIENTATION;

        private SpatialPrefixTree prefixTree;

        protected Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE));
        }
//
//        public Builder fieldDataSettings(Settings settings) {
//            this.fieldDataSettings = settings;
//            return builder;
//        }

        public Builder boostPrecision(double precision) {
            if (precision > 1) {
                precision = 1;
            }
            this.boostPrecision = precision;
            return this;
        }

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

        public Builder orientation(ShapeBuilder.Orientation orientation) {
            this.orientation = orientation;
            return this;
        }

        @Override
        public GeoMapper build(BuilderContext context) {
            final FieldMapper.Names names = buildNames(context);

//            if (Names.TREE_GEOHASH.equals(tree)) {
//                prefixTree = new GeohashPrefixTree(ShapeBuilder.SPATIAL_CONTEXT, getLevels(treeLevels, precisionInMeters, Defaults.GEOHASH_LEVELS, true));
//            } else if (Names.TREE_QUADTREE.equals(tree)) {
//                prefixTree = new QuadPrefixTree(ShapeBuilder.SPATIAL_CONTEXT, getLevels(treeLevels, precisionInMeters, Defaults.QUADTREE_LEVELS, false));
//            } else {
//                throw new ElasticsearchIllegalArgumentException("Unknown prefix tree type [" + tree + "]");
//            }

            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(ContentPath.Type.FULL);

            context.path().add(name);

            BinaryFieldMapper wkbMapper = wkbBuilder.docValues(true).build(context);
            StringFieldMapper typeMapper = typeBuilder.tokenized(false).docValues(true).includeInAll(false).omitNorms(true).index(true).build(context);
            DoubleFieldMapper doubleMapper = areaBuilder.tokenized(false).docValues(true).includeInAll(false).index(true).build(context);
            GeoPointFieldMapper bboxMapper = bboxBuilder.enableGeoHash(false).tokenized(false).docValues(true).build(context);
            StringFieldMapper hashMapper = hashBuilder.tokenized(false).includeInAll(false).omitNorms(true).index(true).docValues(true).build(context);
//            StringFieldMapper wkbTextMapper = wkbTextBuilder.tokenized(false).includeInAll(false).omitNorms(true).index(true).docValues(true).build(context);
            GeoPointFieldMapper centroidMapper = centroidBuilder.enableGeoHash(true).geohashPrefix(true).docValues(true).build(context);

            context.path().remove();
            context.path().pathType(origPathType);


            return new GeoMapper(names, tree, strategyName, distanceErrorPct, orientation, boostPrecision, wkbMapper, typeMapper, doubleMapper, bboxMapper, hashMapper, centroidMapper, fieldDataSettings, docValues, fieldType, postingsProvider,
                    docValuesProvider, multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new GeoMapper.Builder(name);
//            TypeParsers.parseField(builder, name, node, parserContext);

            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (Names.TREE.equals(fieldName)) {
                    builder.tree(fieldNode.toString());
//                } else if (Names.TREE_LEVELS.equals(fieldName)) {
//                    builder.treeLevels(Integer.parseInt(fieldNode.toString()));
//                } else if (Names.TREE_PRESISION.equals(fieldName)) {
//                    builder.treeLevelsByDistance(DistanceUnit.parse(fieldNode.toString(), DistanceUnit.DEFAULT, DistanceUnit.DEFAULT));
                } else if (Names.DISTANCE_ERROR_PCT.equals(fieldName)) {
                    builder.distanceErrorPct(Double.parseDouble(fieldNode.toString()));
                } else if (Names.STRATEGY.equals(fieldName)) {
                    builder.strategy(fieldNode.toString());
                } else if (Names.BOOST_PRECISION.equals(fieldName)) {
                    builder.boostPrecision(Double.parseDouble(fieldNode.toString()));
                } else if (Names.ORIENTATION.equals(fieldName)) {
                    builder.orientation(ShapeBuilder.orientationFromString(fieldNode.toString()));
                }
            }
            return builder;
        }
    }

    private final BinaryFieldMapper wkbMapper;
    private final StringFieldMapper typeMapper;
    private final DoubleFieldMapper areaMapper;
    private final GeoPointFieldMapper bboxMapper;
    private final StringFieldMapper hashMapper;
//    private final StringFieldMapper wkbTextMapper;
    private final GeoPointFieldMapper centroidMapper;

//    private final PrefixTreeStrategy defaultStrategy;
//    private final RecursivePrefixTreeStrategy recursiveStrategy;
//    private final TermQueryPrefixTreeStrategy termStrategy;

    private final Map<Integer, PrefixTreeStrategy> hashTreeLevels;

    // Tree Type : Geohash or quadTree
    private final String tree;
    private final String defaultStrategyName;
    private final double distanceErrorPct;
    private final double boostPrecision;
    private ShapeBuilder.Orientation shapeOrientation;

    private static SpatialPrefixTree getSearchPrefixTree(String treeType) {
        if (Names.TREE_GEOHASH.equals(treeType)) {
            return new GeohashPrefixTree(ShapeBuilder.SPATIAL_CONTEXT, GeohashPrefixTree.getMaxLevelsPossible());
//            return new GeohashPrefixTree(ShapeBuilder.SPATIAL_CONTEXT, 1);
        } else if (Names.TREE_QUADTREE.equals(treeType)) {
            return new QuadPrefixTree(ShapeBuilder.SPATIAL_CONTEXT, QuadPrefixTree.DEFAULT_MAX_LEVELS);
        } else {
            throw new ElasticsearchIllegalArgumentException("Unknown prefix tree type [" + treeType + "]");
        }
    }

    public GeoMapper(FieldMapper.Names names,
                     String tree, String defaultStrategyName, double distanceErrorPct, ShapeBuilder.Orientation shapeOrientation, double boostPrecision, BinaryFieldMapper wkbMapper,
                     StringFieldMapper typeMapper, DoubleFieldMapper areaMapper, GeoPointFieldMapper bboxMapper,
                     StringFieldMapper hashMapper, GeoPointFieldMapper centroidMapper, @Nullable Settings fieldDataSettings, Boolean docValues, FieldType fieldType,
                     PostingsFormatProvider postingsProvider, DocValuesFormatProvider docValuesProvider,
                     MultiFields multiFields, CopyTo copyTo) {
        super(names, getSearchPrefixTree(tree), defaultStrategyName, distanceErrorPct, shapeOrientation, fieldType, postingsProvider, docValuesProvider, multiFields, copyTo);
//        super(names, 1, fieldType, docValues, null, null, postingsProvider, docValuesProvider, null, null, fieldDataSettings , null, multiFields, copyTo);
        this.wkbMapper = wkbMapper;
        this.typeMapper = typeMapper;
        this.areaMapper = areaMapper;
        this.bboxMapper = bboxMapper;
        this.hashMapper = hashMapper;
//        this.wkbTextMapper = wkbTextMapper;
        this.centroidMapper = centroidMapper;
        this.defaultStrategyName = defaultStrategyName;
        this.shapeOrientation = shapeOrientation;

//        this.recursiveStrategy = new RecursivePrefixTreeStrategy(tree, names.indexName());
//        this.recursiveStrategy.setDistErrPct(distanceErrorPct);
//        this.termStrategy = new TermQueryPrefixTreeStrategy(tree, names.indexName());
//        this.termStrategy.setDistErrPct(distanceErrorPct);
//        this.defaultStrategy = resolveStrategy(defaultStrategyName);

        this.tree = tree;

        this.hashTreeLevels = new HashMap<>();
        this.distanceErrorPct = distanceErrorPct;
        this.boostPrecision = boostPrecision;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return null;
    }


    private int boostLevel(double level) {
        return (int) (level + level * this.boostPrecision);
    }

    private int getTreelevelForGeometry(Geometry geom, Geometry centroid) {
        // For mutliple geometry, we must take the smaller length precision
        // For geohash, level is between 1 -> 12


//        double geomLength = geom.getArea();
        double geomLength = geom.getLength();

//        double geomArea = geom.getArea();

        // little magic here
//        if (geomArea > 0) {
//            if (geomArea < 0.1) {
//                geomLength /= 4;
//            } else {
//                geomLength = geomArea;
//            }
//
//        }

//        if (geomLength == 0) {
//            geomLength = geom.getLength();
//        }
        if (tree.equals(Names.TREE_GEOHASH)) {

            int level;

            // For point
            if (geomLength == 0) {
                level = GeohashPrefixTree.getMaxLevelsPossible() / 2;
            } else {
                level = GeoUtils.geoHashLevelsForPrecision(GeoPluginUtils.getMetersFromDecimalDegree(geomLength, centroid.getCoordinate().y));


                if (level < 4) {
                    level = 4;
                }
//                level = 4;
            }

            level = boostLevel(level);

            if (level > GeohashPrefixTree.getMaxLevelsPossible()) {
                level = GeohashPrefixTree.getMaxLevelsPossible();
            }

            return level;

        } else if (tree.equals(Names.TREE_QUADTREE)) {

            int level;

            // For point
            if (geomLength == 0) {
                level = QuadPrefixTree.DEFAULT_MAX_LEVELS;
            } else {
                level = GeoUtils.quadTreeLevelsForPrecision(GeoPluginUtils.getMetersFromDecimalDegree(geomLength, centroid.getCoordinate().y));
            }


            if (level < 8) {
                level = 8;
            }

            level = boostLevel(level);
            if (level > QuadPrefixTree.DEFAULT_MAX_LEVELS) {
                level = QuadPrefixTree.DEFAULT_MAX_LEVELS;
            }

            return level;

//            return QuadPrefixTree.DEFAULT_MAX_LEVELS;
//            return -1;
        } else {
            return -1;
        }
    }

    private PrefixTreeStrategy createPrefixTreeStrategyForLevel(int level) {

        SpatialPrefixTree prefixTree;

        String strategyName = defaultStrategyName;

        if (Names.TREE_GEOHASH.equals(tree)) {
            prefixTree = new GeohashPrefixTree(ShapeBuilder.SPATIAL_CONTEXT, level);

//            if (level == GeohashPrefixTree.getMaxLevelsPossible()) {
//                strategyName = SpatialStrategy.TERM.getStrategyName();
//            }
        } else if (Names.TREE_QUADTREE.equals(tree)) {
            prefixTree = new QuadPrefixTree(ShapeBuilder.SPATIAL_CONTEXT, level);
//            if (level == QuadPrefixTree.) {
//                strategyName = SpatialStrategy.TERM.getStrategyName();
//            }
        } else {
            throw new ElasticsearchIllegalArgumentException("Unknown prefix tree type [" + tree + "]");
        }

        PrefixTreeStrategy prefixTreeStrategy;

        if (SpatialStrategy.RECURSIVE.getStrategyName().equals(strategyName)) {
            prefixTreeStrategy = new RecursivePrefixTreeStrategy(prefixTree, names.indexName());
            prefixTreeStrategy.setDistErrPct(distanceErrorPct);
        } else if (SpatialStrategy.TERM.getStrategyName().equals(strategyName)) {
            prefixTreeStrategy = new TermQueryPrefixTreeStrategy(prefixTree, names.indexName());
            prefixTreeStrategy.setDistErrPct(distanceErrorPct);
        } else {
            throw new ElasticsearchIllegalArgumentException("Unknown prefix tree strategy [" + strategyName + "]");
        }
        return prefixTreeStrategy;
    }

    private PrefixTreeStrategy getTreeStrategyForSpecificGeometry(Geometry geom, Geometry centroid) {
        int level = getTreelevelForGeometry(geom, centroid);

        if (level == -1) {
            return null;
        }

        PrefixTreeStrategy prefixTreeStrategy = hashTreeLevels.get(level);

        if (prefixTreeStrategy == null) {
            prefixTreeStrategy = createPrefixTreeStrategyForLevel(level);
            hashTreeLevels.put(level, prefixTreeStrategy);
        }

        return prefixTreeStrategy;
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
        } else if (shape instanceof JtsGeometry){
            return ((JtsGeometry) shape).getGeom();
        } else {
            throw new MapperParsingException("failed to parse [" + names.fullName() + "]");
        }
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

            Shape shape = shapeBuilder.build();

            Geometry geom = getGeometryFromShape(shape);

            Geometry centroid = geom.getCentroid();

            PrefixTreeStrategy prefixTreeStrategy = getTreeStrategyForSpecificGeometry(geom, centroid);

            if (prefixTreeStrategy !=null) {

                Field[] fields = prefixTreeStrategy.createIndexableFields(shape);
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
            }

            byte[] wkb = new WKBWriter().write(geom);

            wkbMapper.parse(context.createExternalValueContext(wkb));

            BinaryFieldMapper.CustomBinaryDocValuesField f = (BinaryFieldMapper.CustomBinaryDocValuesField) context.doc().getByKey(names().indexName());

            if (f == null) {
                f = new BinaryFieldMapper.CustomBinaryDocValuesField(names().indexName(), wkb);
                context.doc().addWithKey(names().indexName(), f);
            } else {
                f.add(wkb);
            }

            areaMapper.parse(context.createExternalValueContext(geom.getLength()));

            typeMapper.parse(context.createExternalValueContext(geom.getGeometryType()));

            Coordinate[] coords = geom.getEnvelope().getCoordinates();

            GeoPoint topLeft = new GeoPoint(coords[0].y, coords[0].x);
            bboxMapper.parse(context.createExternalValueContext(topLeft));

            if (coords.length == 5) {
                GeoPoint bottomRight = new GeoPoint(coords[2].y, coords[2].x);
                bboxMapper.parse(context.createExternalValueContext(bottomRight));
            }

            hashMapper.parse(context.createExternalValueContext(String.valueOf(GeoPluginUtils.getHashFromWKB(new BytesRef(wkb)))));
//            wkbTextMapper.parse(context.createExternalValueContext(Base64.encodeBytes(wkb)));


//            Point c = shape.getCenter();
            centroidMapper.parse(context.createExternalValueContext(new GeoPoint(centroid.getCoordinate().y, centroid.getCoordinate().x)));

        } catch (Exception e) {
            throw new MapperParsingException("failed to parse [" + names.fullName() + "]", e);
        }
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field("type", contentType());
        builder.field(Names.TREE, tree);
        builder.field(Names.BOOST_PRECISION, boostPrecision);
        builder.field(Names.DISTANCE_ERROR_PCT, distanceErrorPct);

//        if (defaultStrategy.getGrid() instanceof GeohashPrefixTree) {
//            // Don't emit the tree name since GeohashPrefixTree is the default
//            // Only emit the tree levels if it isn't the default value
//            if (includeDefaults || defaultStrategy.getGrid().getMaxLevels() != Defaults.GEOHASH_LEVELS) {
//                builder.field(Names.TREE_LEVELS, defaultStrategy.getGrid().getMaxLevels());
//            }
//        } else {
//            builder.field(Names.TREE, Names.TREE_QUADTREE);
//            if (includeDefaults || defaultStrategy.getGrid().getMaxLevels() != Defaults.QUADTREE_LEVELS) {
//                builder.field(Names.TREE_LEVELS, defaultStrategy.getGrid().getMaxLevels());
//            }
//        }
//
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
        areaMapper.traverse(fieldMapperListener);
        bboxMapper.traverse(fieldMapperListener);
        hashMapper.traverse(fieldMapperListener);
//        wkbTextMapper.traverse(fieldMapperListener);
        centroidMapper.traverse(fieldMapperListener);
    }


    @Override
    public void close() {
        super.close();
        wkbMapper.close();
        typeMapper.close();
        areaMapper.close();
        bboxMapper.close();
        hashMapper.close();
//        wkbTextMapper.close();
        centroidMapper.close();
    }

}
