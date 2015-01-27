package com.opendatasoft.elasticsearch.rest.action.geo;

import com.opendatasoft.elasticsearch.action.geo.GeoSimpleAction;
import com.opendatasoft.elasticsearch.action.geo.GeoSimpleRequest;
import com.opendatasoft.elasticsearch.index.mapper.geo.GeoMapper2;
import com.opendatasoft.elasticsearch.plugin.geo.GeoPluginUtils;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.GeoShape;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.GeoShapeBuilder;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.InternalGeoShape;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKTWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregator;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.geotools.geojson.geom.GeometryJSON;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.rest.RestStatus.OK;

public class RestGeoAction4 extends BaseRestHandler {


    @Inject
    public RestGeoAction4(Settings settings, Client client, RestController controller) {
        super(settings, controller, client);

        // register search handler
        // specifying and index and type is optional
        controller.registerHandler(RestRequest.Method.GET, "/_geo4/{zoom}/{x}/{y}", this);
        controller.registerHandler(RestRequest.Method.GET, "/{index}/_geo4/{zoom}/{x}/{y}", this);
        controller.registerHandler(RestRequest.Method.GET, "/{index}/{type}/_geo4/{zoom}/{x}/{y}", this);
        controller.registerHandler(RestRequest.Method.POST, "/_geo4/{zoom}/{x}/{y}", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_geo4/{zoom}/{x}/{y}", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/{type}/_geo4/{zoom}/{x}/{y}", this);
    }

    @Override
    protected void handleRequest(final RestRequest request, final RestChannel channel, final Client client) throws Exception {
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
        final String geoFieldArea = geoField + "." + GeoMapper2.Names.AREA;
        final String geoFieldCentroid = geoField + "." + GeoMapper2.Names.CENTROID;


        final int zoom = request.paramAsInt("zoom", 1);
        final int x = request.paramAsInt("x", 0);
        final int y = request.paramAsInt("y", 0);
        final int tileSize = request.paramAsInt("tile_size", 256);


        GeoPluginUtils.Envelope tileEnvelope = GeoPluginUtils.tile2boundingBox(x, y, zoom, tileSize);

        Envelope jtsEnvelope = new Envelope(tileEnvelope.west, tileEnvelope.east, tileEnvelope.south, tileEnvelope.north);

        Coordinate centroid = jtsEnvelope.centre();

        // Compute shape length limit delimiting big and small shapes

        double shapeLimit = GeoPluginUtils.getShapeLimit(zoom, centroid.y, 10);

        final int nbGeom = request.paramAsInt("nb_geom", 1000);
        final String stringOutputFormat = request.param("output_format", "wkt");
        final String stringAlgorithm = request.param("algorithm", "TOPOLOGY_PRESERVING");
//        final boolean getSource = request.paramAsBoolean("get_source", true);

        final InternalGeoShape.OutputFormat outputFormat = InternalGeoShape.OutputFormat.valueOf(stringOutputFormat.toUpperCase());

        final GeoShape.Algorithm algorithm = GeoShape.Algorithm.valueOf(stringAlgorithm.toUpperCase());

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        GeoPluginUtils.Envelope overflowEnvelope = GeoPluginUtils.overflowEnvelope(tileEnvelope, 40, tileSize);
        
        int geohashLevel = GeoUtils.geoHashLevelsForPrecision(GeoPluginUtils.getMeterByPixel(zoom, centroid.y) * 40);

        int nbShape = 300;
        if (zoom > 0) {
            nbShape =  300 / zoom;
        }

        FilterBuilder pointFilter = new BoolFilterBuilder().must(
                new TermFilterBuilder(geoFieldType, "Point"),
                new GeoShapeFilterBuilder(geoField,
                        ShapeBuilder.newEnvelope()
                                .topLeft(overflowEnvelope.west, overflowEnvelope.north)
                                .bottomRight(overflowEnvelope.east, overflowEnvelope.south))
        );


//        FilterBuilder shapeFilter = new BoolFilterBuilder().must(
//                new BoolFilterBuilder().should(
//                        new NotFilterBuilder(new TermFilterBuilder(geoFieldType, "Point")),
//                        new GeoShapeFilterBuilder(geoField,
//                                ShapeBuilder.newEnvelope()
//                                        .topLeft(overflowEnvelope.west, overflowEnvelope.north)
//                                        .bottomRight(overflowEnvelope.east, overflowEnvelope.south))
//                ),
//                new BoolFilterBuilder().should(
//                        new TermFilterBuilder(geoFieldType, "Point"),
//                        new GeoShapeFilterBuilder(geoField,
//                                ShapeBuilder.newEnvelope()
//                                        .topLeft(tileEnvelope.west, tileEnvelope.north)
//                                        .bottomRight(tileEnvelope.east, tileEnvelope.south))
//                )
//        );

        FilterBuilder shapeFilter = new BoolFilterBuilder().must(
                new NotFilterBuilder(new TermFilterBuilder(geoFieldType, "Point")),
                new GeoShapeFilterBuilder(geoField,
                        ShapeBuilder.newEnvelope()
                                .topLeft(tileEnvelope.west, tileEnvelope.north)
                                .bottomRight(tileEnvelope.east, tileEnvelope.south))
        );


        GeoShapeBuilder pointGeoShapeBuilder = new GeoShapeBuilder("shape").field(geoFieldWKB).algorithm(algorithm).zoom(zoom).simplifyShape(true).outputFormat(outputFormat).size(1);
        GeoHashGridBuilder pointGeoHashGridBuilder = new GeoHashGridBuilder("point_grid").field(geoFieldCentroid).precision(geohashLevel).size(1000).subAggregation(pointGeoShapeBuilder);
//        FilterAggregationBuilder pointAggregation = new FilterAggregationBuilder("points").filter(pointFilter).subAggregation(pointGeoHashGridBuilder);


        GeoShapeBuilder smallGeoShapeBuilder = new GeoShapeBuilder("shape").field(geoFieldWKB).algorithm(algorithm).zoom(zoom).simplifyShape(true).outputFormat(outputFormat).size(nbShape);
        GeoHashGridBuilder geoHashGridBuilder = new GeoHashGridBuilder("small_grid").field(geoFieldCentroid).precision(geohashLevel).size(1000).subAggregation(smallGeoShapeBuilder);
        FilterAggregationBuilder smallShapeAggregation = new FilterAggregationBuilder("small_shapes").
                filter(new RangeFilterBuilder(geoFieldArea).gte(0).lt(shapeLimit)).subAggregation(geoHashGridBuilder);


        GeoShapeBuilder geoShapeBuilder = new GeoShapeBuilder("shape").field(geoFieldWKB).algorithm(algorithm).zoom(zoom).simplifyShape(true).outputFormat(outputFormat).size(nbGeom);
        FilterAggregationBuilder bigShapeAggregation = new FilterAggregationBuilder("big_shapes").filter(new RangeFilterBuilder(geoFieldArea).gt(shapeLimit)).subAggregation(geoShapeBuilder);


        searchSourceBuilder.aggregation(new FilterAggregationBuilder("shapes").filter(shapeFilter).subAggregation(smallShapeAggregation).subAggregation(bigShapeAggregation));
        searchSourceBuilder.aggregation(new FilterAggregationBuilder("points").filter(pointFilter).subAggregation(pointGeoHashGridBuilder));


        searchRequest.searchType(SearchType.COUNT);

        searchRequest.extraSource(searchSourceBuilder);

        geoSimpleRequest.setSearchRequest(searchRequest);


        client.execute(GeoSimpleAction.INSTANCE, geoSimpleRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {
                try {


                    SingleBucketAggregation pointAggregation = response.getAggregations().get("points");
                    GeoHashGrid pointGrid = pointAggregation.getAggregations().get("point_grid");

                    SingleBucketAggregation shapes = response.getAggregations().get("shapes");
                    SingleBucketAggregation smallShapeFilterAggregation = shapes.getAggregations().get("small_shapes");
                    GeoHashGrid smallGrid = smallShapeFilterAggregation.getAggregations().get("small_grid");

                    SingleBucketAggregation bigShapeFilterAggregation = shapes.getAggregations().get("big_shapes");
                    GeoShape bigShapeAggregation = bigShapeFilterAggregation.getAggregations().get("shape");

                    final XContentBuilder builder = channel.newBuilder();
                    builder.startObject();
                    builder.field("time", response.getTookInMillis());
                    builder.field("count", bigShapeFilterAggregation.getDocCount() + smallShapeFilterAggregation.getDocCount() + pointAggregation.getDocCount());

                    builder.field("shapes");

                    builder.startArray();

                    for (GeoShape.Bucket bucket : bigShapeAggregation.getBuckets()) {
                        BytesRef wkb = bucket.getShapeAsByte();

                        String geoString;
                        Geometry geo = new WKBReader().read(wkb.bytes);
                        switch (outputFormat) {
                            case WKT:
                                geoString = new WKTWriter().write(geo);
                                break;
                            case WKB:
                                geoString = Base64.encodeBytes(wkb.bytes);
                                break;
                            default:
                                geoString = geometryJSON.toString(geo);
                        }

                        builder.startObject();

                        builder.field("shape", geoString);
                        builder.field("digest", bucket.getHash());
                        builder.field("type", bucket.getType());
                        builder.field("doc_count", bucket.getDocCount());

//                        builder.field("geo-type", geo.getGeometryType());

                        builder.endObject();
                    }


                    for (GeoHashGrid.Bucket bucketGrid : smallGrid.getBuckets()) {

                        GeoShape smallShapeAggregation = bucketGrid.getAggregations().get("shape");

                        for (GeoShape.Bucket bucket: smallShapeAggregation.getBuckets()){

                            BytesRef wkb = bucket.getShapeAsByte();

                            String geoString;
                            Geometry geo = new WKBReader().read(wkb.bytes);
                            switch (outputFormat) {
                                case WKT:
                                    geoString = new WKTWriter().write(geo);
                                    break;
                                case WKB:
                                    geoString = Base64.encodeBytes(wkb.bytes);
                                    break;
                                default:
                                    geoString = geometryJSON.toString(geo);
                            }

                            builder.startObject();

                            builder.field("shape", geoString);
                            builder.field("digest", bucket.getHash());
                            builder.field("type", bucket.getType());
                            builder.field("doc_count", bucket.getDocCount());
                            builder.field("cluster_count", bucketGrid.getDocCount());
                            builder.field("grid", bucketGrid.getKey());


                            builder.endObject();
                        }
                    }

                    for (GeoHashGrid.Bucket bucketGrid : pointGrid.getBuckets()) {

                        GeoShape smallShapeAggregation = bucketGrid.getAggregations().get("shape");

                        for (GeoShape.Bucket bucket: smallShapeAggregation.getBuckets()){

                            BytesRef wkb = bucket.getShapeAsByte();

                            String geoString;
                            Geometry geo = new WKBReader().read(wkb.bytes);
                            switch (outputFormat) {
                                case WKT:
                                    geoString = new WKTWriter().write(geo);
                                    break;
                                case WKB:
                                    geoString = Base64.encodeBytes(wkb.bytes);
                                    break;
                                default:
                                    geoString = geometryJSON.toString(geo);
                            }

                            builder.startObject();

                            builder.field("shape", geoString);
                            builder.field("digest", bucket.getHash());
                            builder.field("type", bucket.getType());
                            builder.field("doc_count", bucket.getDocCount());
                            builder.field("cluster_count", bucketGrid.getDocCount());
                            builder.field("grid", bucketGrid.getKey());


                            builder.endObject();
                        }
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
