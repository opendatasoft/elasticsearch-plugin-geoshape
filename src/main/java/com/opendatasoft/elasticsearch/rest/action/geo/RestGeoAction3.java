package com.opendatasoft.elasticsearch.rest.action.geo;

import com.opendatasoft.elasticsearch.action.geo.GeoSimpleAction;
import com.opendatasoft.elasticsearch.action.geo.GeoSimpleRequest;
import com.opendatasoft.elasticsearch.index.mapper.geo.GeoMapper2;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.GeoShape;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.GeoShapeBuilder;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.InternalGeoShape;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKTWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.geotools.geojson.geom.GeometryJSON;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.rest.RestStatus.OK;

public class RestGeoAction3 extends BaseRestHandler {


    @Inject
    public RestGeoAction3(Settings settings, Client client, RestController controller) {
        super(settings, controller, client);

        // register search handler
        // specifying and index and type is optional
        controller.registerHandler(RestRequest.Method.GET, "/_geo3", this);
        controller.registerHandler(RestRequest.Method.GET, "/{index}/_geo3", this);
        controller.registerHandler(RestRequest.Method.GET, "/{index}/{type}/_geo3", this);
        controller.registerHandler(RestRequest.Method.POST, "/_geo3", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_geo3", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/{type}/_geo3", this);
    }

    @Override
    protected void handleRequest(final RestRequest request, final RestChannel channel, final Client client) throws Exception {
        SearchRequest searchRequest = RestSearchAction.parseSearchRequest(request);
        searchRequest.listenerThreaded(false);
        final GeoSimpleRequest geoSimpleRequest = new GeoSimpleRequest();

//        List< arbres_remarquables_20113-geo_shape-geo_shape-wkb
        final String geoField = request.param("field");
        if (geoField == null) {
            throw new Exception("Field parameter is mandatory");
        }
        final String geoFieldWKB = geoField + "." + GeoMapper2.Names.WKB;
        final String geoFieldWKBText = geoField + "." + GeoMapper2.Names.WKB_TEXT;
        final String geoFieldType = geoField + "." + GeoMapper2.Names.TYPE;
        final String geoFieldHash = geoField + "." + GeoMapper2.Names.HASH;

        final int zoom = request.paramAsInt("zoom", 0);
        final int nbGeom = request.paramAsInt("nb_geom", 1000);
        final String stringOutputFormat = request.param("output_format", "wkt");
        final boolean getSource = request.paramAsBoolean("get_source", true);


        Map<String, Object> paramMaps = new HashMap<>();
        paramMaps.put("geoshapestringfield", geoFieldWKBText);
        paramMaps.put("output_format", stringOutputFormat);
        paramMaps.put("zoom", zoom);


//        final InternalGeoShape.OutputFormat outputFormat = InternalGeoShape.OutputFormat.valueOf(stringOutputFormat.toUpperCase());

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();


        searchSourceBuilder.scriptField("shape", "native", "geopreview", paramMaps);
//        searchSourceBuilder.fieldDataField(geoFieldHash);
        searchSourceBuilder.size(100);
//        searchSourceBuilder.scriptField("type", "doc['" + geoFieldType + "'].value");
//        searchSourceBuilder.scriptField("hash", "doc['" + geoFieldHash + "'].value");

//        GeoShapeBuilder geoShapeBuilder = new GeoShapeBuilder("shape").field(geoFieldWKB).size(nbGeom).zoom(zoom).simplifyShape(true).outputFormat(outputFormat);
//        if (getSource) {
//            geoShapeBuilder.subAggregation(
//                    new TopHitsBuilder("top_shape").setSize(1).setFetchSource(false).addScriptField("geo-hash", "doc['" + geoFieldHash + "'].value")
//            );
//        }

//        searchSourceBuilder.aggregation(geoShapeBuilder);

//        searchSourceBuilder.size(0);
        
        searchRequest.extraSource(searchSourceBuilder);
        searchRequest.scroll(new TimeValue(60000));

        geoSimpleRequest.setSearchRequest(searchRequest);

        client.execute(GeoSimpleAction.INSTANCE, geoSimpleRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {
                try {
                    XContentBuilder builder = channel.newBuilder();

//                    GeoShape shapeAggregation = (GeoShape) response.getAggregations().getAsMap().get("shape");
//                    builder.startObject();
//                    builder.field("time", response.getTookInMillis());

//                    builder.field("res");
                    builder.startArray();

                    while (true) {
                        if (response.getHits().getHits().length == 0) {
                            break;
                        }
                        for (SearchHit searchHit : response.getHits()) {
                            builder.startObject();
                            builder.field("shape", searchHit.field("shape").value());
//                            builder.field("hash", searchHit.field(geoFieldHash).value());
                            builder.endObject();
                        }
                        response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
                    }


//                    for (SearchHit searchHit : response.getHits().getHits()) {
//                        builder.startObject();
//                        builder.field("shape", searchHit.field("shape").value());
//                        builder.field("hash", searchHit.field("hash").value());
//                        builder.field("type", searchHit.field("type").value());
//                        builder.endObject();
//                    }

                    builder.endArray();
//                    builder.endObject();
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
