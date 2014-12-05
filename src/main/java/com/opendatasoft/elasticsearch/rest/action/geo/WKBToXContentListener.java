package com.opendatasoft.elasticsearch.rest.action.geo;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.netty.handler.codec.base64.Base64Encoder;
import org.elasticsearch.common.settings.loader.JsonSettingsLoader;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestResponseListener;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import sun.misc.BASE64Encoder;

import java.io.StringWriter;
import java.util.Map;

public class WKBToXContentListener extends RestResponseListener<SearchResponse> {

    public WKBToXContentListener(RestChannel channel) {
        super(channel);
    }

    @Override
    public RestResponse buildResponse(SearchResponse response) throws Exception {

        StringWriter writer = new StringWriter();
        writer.write("pouet");

        JsonSettingsLoader settingsLoader = new JsonSettingsLoader();

        if (response.getHits() != null && response.getHits().getHits() != null && response.getHits().getHits().length > 0) {
            for (SearchHit hit : response.getHits().getHits()) {
                Map<String, SearchHitField> fields = hit.getFields();
            }
//            fields.get("toto").
        }

//        Terms agg = response.getAggregations().get("term");

//        for (Terms.Bucket buck : agg.getBuckets()) {
//            writer.write(Base64.encodeBytes(buck.getKey().getBytes()));
//        }

        return new BytesRestResponse(response.status(), "text", new BytesArray(writer.toString()), true);

    }
}
