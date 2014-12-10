package com.opendatasoft.elasticsearch.index.mapper.geo;

import com.spatial4j.core.context.jts.JtsSpatialContextFactory;
import com.spatial4j.core.io.jts.JtsBinaryCodec;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.core.BinaryFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.core.TypeParsers;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;


public class GeoMapper extends AbstractFieldMapper<Object>{


    public static final String CONTENT_TYPE = "geo";

    public static class Names {
        public static final String WKB = "wkb";
        public static final String TYPE = "type";

    }

    public static class Defaults {

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

        protected Builder(String name) {
            super(name, new FieldType(Defaults.FIELD_TYPE));
        }

        public Builder fieldDataSettings(Settings settings) {
            this.fieldDataSettings = settings;
            return builder;
        }

        @Override
        public GeoMapper build(BuilderContext context) {
            final FieldMapper.Names names = buildNames(context);

            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(ContentPath.Type.FULL);

            context.path().add(name);

            BinaryFieldMapper wkbMapper = wkbBuilder.index(true).docValues(true).build(context);
            StringFieldMapper typeMapper = typeBuilder.tokenized(false).docValues(true).includeInAll(false).omitNorms(true).index(true).build(context);

            context.path().remove();
            context.path().pathType(origPathType);


            return new GeoMapper(names, wkbMapper, typeMapper, fieldDataSettings, docValues, fieldType, postingsProvider,
                    docValuesProvider, multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new GeoMapper.Builder(name);
            TypeParsers.parseField(builder, name, node, parserContext);
            return builder;
        }
    }

    private final BinaryFieldMapper wkbMapper;
    private final StringFieldMapper typeMapper;


    public GeoMapper(FieldMapper.Names names, BinaryFieldMapper wkbMapper, StringFieldMapper typeMapper,
                     @Nullable Settings fieldDataSettings, Boolean docValues, FieldType fieldType,
                     PostingsFormatProvider postingsProvider, DocValuesFormatProvider docValuesProvider,
                     MultiFields multiFields, CopyTo copyTo) {
        super(names, 1, fieldType, docValues, null, null, postingsProvider, docValuesProvider, null, null, fieldDataSettings , null, multiFields, copyTo);
        this.wkbMapper = wkbMapper;
        this.typeMapper = typeMapper;
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
    public boolean hasDocValues() {
        return false;
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

            typeMapper.parse(context.createExternalValueContext(shapeBuilder.type().toString().toLowerCase()));

            parseWkb(context, shapeBuilder.build());

        } catch (Exception e) {
            throw new MapperParsingException("failed to parse [" + names.fullName() + "]", e);
        }
    }

    /**
     * Writes the WKB serialization of the <tt>shape</tt> to the given ParseContext
     * as an external value.
     */
    private void parseWkb(ParseContext context, Shape shape) throws IOException {
        JtsBinaryCodec jtsBinaryCodec = new JtsBinaryCodec(ShapeBuilder.SPATIAL_CONTEXT, new JtsSpatialContextFactory());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(baos);
        jtsBinaryCodec.writeJtsGeom(dataOutputStream, shape);
        byte[] wkb = baos.toByteArray();
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

    @Override
    public Object value(Object value) {
        throw new UnsupportedOperationException("GeoShape fields cannot be converted to String values");
    }
}
