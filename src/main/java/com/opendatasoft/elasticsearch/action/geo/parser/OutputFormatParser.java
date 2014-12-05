package com.opendatasoft.elasticsearch.action.geo.parser;

import com.opendatasoft.elasticsearch.action.geo.GeoContext;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

/**
 * Created with IntelliJ IDEA.
 * User: clement
 * Date: 05/12/14
 * Time: 10:23
 * To change this template use File | Settings | File Templates.
 */
public class OutputFormatParser implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token.isValue()) {
            ((GeoContext)context).setOutputFormat(parser.text());
        }
    }
}
