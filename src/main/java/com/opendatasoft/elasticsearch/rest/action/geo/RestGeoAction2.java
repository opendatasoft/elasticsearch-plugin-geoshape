package com.opendatasoft.elasticsearch.rest.action.geo;

import com.opendatasoft.elasticsearch.action.geo.*;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.rest.RestStatus.OK;

public class RestGeoAction2 extends BaseRestHandler {

    @Inject
    public RestGeoAction2(Settings settings, Client client, RestController controller) {
        super(settings, controller, client);

        // register search handler
        // specifying and index and type is optional
        controller.registerHandler(RestRequest.Method.GET, "/_geo2", this);
        controller.registerHandler(RestRequest.Method.GET, "/{index}/_geo2", this);
        controller.registerHandler(RestRequest.Method.GET, "/{index}/{type}/_geo2", this);
        controller.registerHandler(RestRequest.Method.POST, "/_geo2", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_geo2", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/{type}/_geo2", this);
    }

    @Override
    protected void handleRequest(final RestRequest request, final RestChannel channel, Client client) throws Exception {
        SearchRequest searchRequest = RestSearchAction.parseSearchRequest(request);
        final GeoSimpleRequest geoSimpleRequest = new GeoSimpleRequest();


//        List< arbres_remarquables_20113-geo_shape-geo_shape-wkb
        final String geoField = request.param("field");
        final String geoFieldWKB = geoField + "-wkb";
        final String geoFieldString = geoField + "-string";
        final String geoFieldHash = geoField + "-hash";

        final String zoom = request.param("zoom");
        final String outputFormat = request.param("output_format", "wkt");
        final boolean getSource = request.paramAsBoolean("source", true);


        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();


        Map<String, Object> params = new HashMap<>();
        params.put("geoshapestringfield", geoFieldWKB);
        params.put("zoom", Integer.valueOf(zoom));
        params.put("output_format", outputFormat);


        searchSourceBuilder.scriptField("geopreview", "native", "geopreview", params);
        searchSourceBuilder.scriptField("geo-type", "doc['"+ geoField +"-type'].value");

//        searchSourceBuilder.fieldDataField(geo_field);

//        searchSourceBuilder.size(5);
        searchRequest.extraSource(searchSourceBuilder);

        geoSimpleRequest.setSearchRequest(searchRequest);

        client.execute(GeoSimpleAction.INSTANCE, geoSimpleRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {
                try {
                    XContentBuilder builder = channel.newBuilder();

                    builder.startArray();
                    for (SearchHit hit : response.getHits().getHits()) {
                        builder.startObject();
                        SearchHitField geoPreview = hit.field("geopreview");

                        builder.field(outputFormat, geoPreview.getValue());
                        Map<String, Object> source = hit.sourceAsMap();
                        
                        builder.field("geo-type", hit.field("geo-type"));


                        if (getSource) {
                            builder.startObject("properties");

                            for (String key: source.keySet()) {
                                if (!key.equals(geoField) && ! key.equals(geoFieldWKB) &&
                                        ! key.equals(geoFieldString) && !key.equals(geoFieldHash)) {
                                    builder.field(key, source.get(key));
                                }
                            }

                            builder.endObject();
                        }


                        builder.endObject();

                    }
                    builder.endArray();

                    channel.sendResponse(new BytesRestResponse(OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    logger.error(e.getMessage(), e);
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });

    }
}
