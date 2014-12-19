package com.opendatasoft.elasticsearch.rest.action.geo;

import com.opendatasoft.elasticsearch.action.geo.*;
import com.opendatasoft.elasticsearch.index.mapper.geo.GeoMapper;
import com.opendatasoft.elasticsearch.index.mapper.geo.GeoMapper2;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.GeoShape;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.GeoShapeBuilder;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.GeoShapeParser;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.InternalGeoShape;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.support.RestUtils;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.geotools.geojson.geom.GeometryJSON;

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
        searchRequest.listenerThreaded(true);
        final GeoSimpleRequest geoSimpleRequest = new GeoSimpleRequest();

        final GeometryJSON geometryJSON = new GeometryJSON(20);


//        List< arbres_remarquables_20113-geo_shape-geo_shape-wkb
        final String geoField = request.param("field");
        if (geoField == null) {
            throw new Exception("Field parameter is mandatory");
        }
        final String geoFieldWKB = geoField + "." + GeoMapper2.Names.WKB;
        final String geoFieldType = geoField + "." + GeoMapper2.Names.TYPE;
        final String geoFieldHash = geoField + "." + GeoMapper2.Names.HASH;

        final int zoom = request.paramAsInt("zoom", 0);
        final int nbGeom = request.paramAsInt("nb_geom", 1000);
        final String stringOutputFormat = request.param("output_format", "wkt");
        final boolean getSource = request.paramAsBoolean("get_source", true);



        final InternalGeoShape.OutputFormat outputFormat = InternalGeoShape.OutputFormat.valueOf(stringOutputFormat.toUpperCase());

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();


        GeoShapeBuilder geoShapeBuilder = new GeoShapeBuilder("shape").field(geoFieldWKB).size(nbGeom).zoom(zoom).simplifyShape(true).outputFormat(outputFormat);
        if (getSource) {
            geoShapeBuilder.subAggregation(
                    new TopHitsBuilder("top_shape").setSize(1).setFetchSource(false).addScriptField("geo-hash", "doc['" + geoFieldHash + "'].value")
            );
        }

        searchSourceBuilder.aggregation(geoShapeBuilder);

        searchSourceBuilder.size(0);
        
        searchRequest.extraSource(searchSourceBuilder);

        geoSimpleRequest.setSearchRequest(searchRequest);

        client.execute(GeoSimpleAction.INSTANCE, geoSimpleRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {
                try {
                    XContentBuilder builder = channel.newBuilder();
                    GeoShape shapeAggregation = (GeoShape) response.getAggregations().getAsMap().get("shape");
                    builder.startObject();
                    builder.field("time", response.getTookInMillis());

                    builder.field("res");

                    builder.startArray();

                    for (GeoShape.Bucket bucket : shapeAggregation.getBuckets()) {
                        BytesRef wkb = bucket.getShapeAsByte();

                        String geoString;
                        Geometry geo = new WKBReader().read(wkb.bytes);
                        switch (outputFormat) {
                            case WKT:
                                geoString = new WKTWriter().write(geo); break;
                            case WKB:
                                geoString =  Base64.encodeBytes(wkb.bytes); break;
                            default:
                                geoString = geometryJSON.toString(geo);
                        }

                        builder.startObject();

                        builder.field("shape", geoString);

                        builder.field("geo-type", geo.getGeometryType());

                        if (getSource) {
                            TopHits topHitsAggregation = (TopHits) bucket.getAggregations().getAsMap().get("top_shape");

                            SearchHit hit = topHitsAggregation.getHits().getHits()[0];

                            builder.field("geo-hash", hit.field("geo-hash").getValue());
                            builder.startObject("properties");
                            Map<String, Object> source = hit.sourceAsMap();

                            if (source != null) {
                                for (String key: source.keySet()) {
                                    if (!key.equals(geoField) && ! key.equals(geoFieldWKB)) {
                                        builder.field(key, source.get(key));
                                    }
                                }
                            }
                            builder.endObject();
                        }

                        builder.endObject();
                    }

                    builder.endArray();
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
