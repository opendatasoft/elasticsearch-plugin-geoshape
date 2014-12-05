package com.opendatasoft.elasticsearch.rest.action.geo;

import com.opendatasoft.elasticsearch.action.geo.GeoAction;
import com.opendatasoft.elasticsearch.action.geo.GeoRequest;
import com.opendatasoft.elasticsearch.action.geo.GeoRequestBuilder;
import com.opendatasoft.elasticsearch.action.geo.GeoResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.support.RestActions;

import java.io.IOException;

import static org.elasticsearch.rest.RestStatus.OK;

public class RestGeoAction extends BaseRestHandler {

    @Inject
    public RestGeoAction(Settings settings, Client client, RestController controller) {
        super(settings, controller, client);

        // register search handler
        // specifying and index and type is optional
        controller.registerHandler(RestRequest.Method.GET, "/_geo", this);
        controller.registerHandler(RestRequest.Method.GET, "/{index}/_geo", this);
        controller.registerHandler(RestRequest.Method.GET, "/{index}/{type}/_geo", this);
        controller.registerHandler(RestRequest.Method.POST, "/_geo", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_geo", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/{type}/_geo", this);
    }

    @Override
    protected void handleRequest(final RestRequest request, final RestChannel channel, Client client) throws Exception {
        final GeoRequest geoRequest = new GeoRequest(Strings.splitStringByCommaToArray(request.param("index")));
        geoRequest.indicesOptions(IndicesOptions.fromRequest(request, geoRequest.indicesOptions()));
        geoRequest.listenerThreaded(false);
        if (request.hasContent()) {
            geoRequest.source(request.content(), request.contentUnsafe());
        } else {
            String source = request.param("source");
            if (source != null) {
                geoRequest.source(source);
            } else {
                QuerySourceBuilder querySourceBuilder = RestActions.parseQuerySource(request);
                if (querySourceBuilder != null) {
                    geoRequest.source(querySourceBuilder);
                }
            }
        }
        geoRequest.routing(request.param("routing"));
        geoRequest.types(Strings.splitStringByCommaToArray(request.param("type")));

//        SearchRequest searchRequest = RestSearchAction.parseSearchRequest(request);
////        QuerySourceBuilder querySourceBuilder = RestActions.parseQuerySource(request);
////        System.out.println(XContentHelper.convertToJson(querySourceBuilder.buildAsBytes(Requests.CONTENT_TYPE), false));
//        searchRequest.listenerThreaded(false);
////        GeoRequest geoRequest = new GeoRequest();
////        client.search(searchRequest, new WKBToXContentListener(channel));
//        GeoRequestBuilder geoRequestBuilder = new GeoRequestBuilder(client);
        client.execute(GeoAction.INSTANCE, geoRequest, new ActionListener<GeoResponse>() {
            @Override
            public void onResponse(GeoResponse response) {
                try {
                    XContentBuilder builder = channel.newBuilder();
                    builder.startObject();
                    builder.field("salut", "pouet");
                    builder.endObject();
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
