package com.opendatasoft.elasticsearch.action.geo.parser;

import com.opendatasoft.elasticsearch.action.geo.GeoContext;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FieldsParseElement;
import org.elasticsearch.search.fetch.explain.ExplainParseElement;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsParseElement;
import org.elasticsearch.search.query.QueryPhase;

import java.util.HashMap;
import java.util.Map;

public class GeoParser {

    private final ImmutableMap<String, ? extends SearchParseElement> elementParsers;

    @Inject
    public GeoParser(QueryPhase queryPhase, FetchPhase fetchPhase) {
        ImmutableMap.Builder<String, SearchParseElement> elementParsers = ImmutableMap.builder();
        elementParsers.putAll(queryPhase.parseElements());
        elementParsers.put("fields", new FieldsParseElement());
        elementParsers.put("field", new FieldDataFieldsParseElement());
        elementParsers.put("explain", new ExplainParseElement());
        elementParsers.put("simplify", new SimplifyParser());
        elementParsers.put("output_format", new OutputFormatParser());
        elementParsers.put("fielddata_fields", new FieldDataFieldsParseElement())
                .put("fielddataFields", new FieldDataFieldsParseElement());
        this.elementParsers = elementParsers.build();
    }


    /**
     * Main method of this class to parse given payload of _export action
     *
     * @param context
     * @param source
     * @throws org.elasticsearch.search.SearchParseException
     */
    public void parseSource(GeoContext context, BytesReference source) throws SearchParseException {
        XContentParser parser = null;
        try {
            if (source != null && source.length() != 0) {
                parser = XContentFactory.xContent(source).createParser(source);
                XContentParser.Token token;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        String fieldName = parser.currentName();
                        parser.nextToken();
                        SearchParseElement element = elementParsers.get(fieldName);
                        if (element == null) {
                            throw new SearchParseException(context, "No parser for element [" + fieldName + "]");
                        }
                        element.parse(parser, context);
                    } else if (token == null) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            String sSource = "_na_";
            try {
                sSource = XContentHelper.convertToJson(source, false);
            } catch (Throwable e1) {
                // ignore
            }
            throw new SearchParseException(context, "Failed to parse source [" + sSource + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

}
