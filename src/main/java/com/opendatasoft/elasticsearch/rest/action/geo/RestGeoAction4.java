package com.opendatasoft.elasticsearch.rest.action.geo;

import com.opendatasoft.elasticsearch.action.geo.GeoSimpleAction;
import com.opendatasoft.elasticsearch.action.geo.GeoSimpleRequest;
import com.opendatasoft.elasticsearch.index.mapper.geo.GeoMapper2;
import com.opendatasoft.elasticsearch.plugin.geo.GeoPluginUtils;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.GeoShape;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.GeoShapeBuilder;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.InternalGeoShape;
import com.spatial4j.core.context.jts.JtsSpatialContextFactory;
import com.spatial4j.core.io.GeohashUtils;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.geotools.geojson.geom.GeometryJSON;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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

        final WKTWriter wktWriter = new WKTWriter();
        final WKBReader wkbReader = new WKBReader();

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
        final String geoFieldDigest = geoField + "." + GeoMapper2.Names.HASH;


        final int zoom = request.paramAsInt("zoom", 1);
        final int x = request.paramAsInt("x", 0);
        final int y = request.paramAsInt("y", 0);
        final int tileSize = request.paramAsInt("tile_size", 256);
        final int nbGeom = request.paramAsInt("nb_geom", 1000);
        final String stringOutputFormat = request.param("output_format", "wkt");
//        final String stringAlgorithm = request.param("algorithm", "TOPOLOGY_PRESERVING");
        final String stringAlgorithm = request.param("algorithm", "TOPOLOGY_PRESERVING");
        final InternalGeoShape.OutputFormat outputFormat = InternalGeoShape.OutputFormat.valueOf(stringOutputFormat.toUpperCase());
        final GeoShape.Algorithm algorithm = GeoShape.Algorithm.valueOf(stringAlgorithm.toUpperCase());


        Envelope tileEnvelope = GeoPluginUtils.tile2boundingBox(x, y, zoom, tileSize);

//        Envelope jtsEnvelope = new Envelope(tileEnvelope.west, tileEnvelope.east, tileEnvelope.south, tileEnvelope.north);

        EnvelopeBuilder envelopeBuilder = ShapeBuilder.newEnvelope();
        envelopeBuilder.topLeft(tileEnvelope.getMinX(), tileEnvelope.getMaxY()).bottomRight(tileEnvelope.getMaxX(), tileEnvelope.getMinY());

        Coordinate centroid = tileEnvelope.centre();

        // Compute shape length limit delimiting big and small shapes

        int smallShapePixels = 10;
        double shapeLimit = GeoPluginUtils.getShapeLimit(zoom, centroid.y, smallShapePixels);
        double dropshapeLimit = GeoPluginUtils.getShapeLimit(zoom, centroid.y, 3);


        double degrees = GeoPluginUtils.getDecimalDegreeFromMeter(40 * GeoPluginUtils.getMeterByPixel(zoom, centroid.y), centroid.y);

        int geohashLevel = GeohashUtils.lookupHashLenForWidthHeight(degrees, degrees);

        double meterByPixel = GeoPluginUtils.getMeterByPixel(zoom, centroid.y);

        int geoHashNbPixels = (int) ((GeoUtils.geoHashCellWidth(geohashLevel) * GeoUtils.geoHashCellHeight(geohashLevel)) / Math.pow(meterByPixel, 2));
        int nbBuckets = (tileSize * tileSize) / geoHashNbPixels;

        // Compute an error precision. (we have false positive shapes, so we need to request more buckets) TODO : use precision in field mapping ??
        // Add 50%
        nbBuckets += nbBuckets * 0.5;

        Envelope overflowEnvelope = GeoPluginUtils.overflowEnvelope(tileEnvelope, 40, tileSize);
//        Envelope overflowEnvelope = GeoPluginUtils.overflowEnvelope(tileEnvelope, (int) (GeoUtils.geoHashCellWidth(geohashLevel) / meterByPixel), tileSize);

//        int geohashLevel = GeoUtils.geoHashLevelsForPrecision(GeoPluginUtils.getMeterByPixel(zoom, centroid.y) * distanceDiagonal);

        FilterBuilder pointFilter = new BoolFilterBuilder().must(
                new TermFilterBuilder(geoFieldType, "Point"),
                new GeoShapeFilterBuilder(geoField,
                        ShapeBuilder.newEnvelope()
                                .topLeft(overflowEnvelope.getMinX(), overflowEnvelope.getMaxY())
                                .bottomRight(overflowEnvelope.getMaxX(), overflowEnvelope.getMinY()))
        );


//        FilterBuilder pointBufferFilter = new BoolFilterBuilder().must(
//                new TermFilterBuilder(geoFieldType, "Point"),
//                new GeoShapeFilterBuilder(geoField,
//                        ShapeBuilder.newEnvelope()
//                                .topLeft(overflowEnvelope.getMinX(), overflowEnvelope.getMaxY())
//                                .bottomRight(overflowEnvelope.getMaxX(), overflowEnvelope.getMinY())),
//                new NotFilterBuilder(new GeoShapeFilterBuilder(geoField,
//                        ShapeBuilder.newEnvelope()
//                                .topLeft(tileEnvelope.getMinX(), tileEnvelope.getMaxY())
//                                .bottomRight(tileEnvelope.getMaxX(), tileEnvelope.getMinY())
//                ))
//
//        );


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
                                .topLeft(tileEnvelope.getMinX(), tileEnvelope.getMaxY())
                                .bottomRight(tileEnvelope.getMaxX(), tileEnvelope.getMinY()))
        );


        TopHitsBuilder topHitsBuilder = new TopHitsBuilder("shape").setFetchSource(geoField, null).setSize(1)
                .addSort(geoFieldDigest, SortOrder.DESC)
                .addSort(new GeoDistanceSortBuilder(geoFieldCentroid).order(SortOrder.ASC).point(centroid.y, centroid.x));
        GeoHashGridBuilder pointGeoHashGridBuilder = new GeoHashGridBuilder("point_grid").field(geoFieldCentroid).precision(geohashLevel).size(1000).subAggregation(topHitsBuilder);


        GeoShapeBuilder smallGeoShapeBuilder = new GeoShapeBuilder("shape").field(geoFieldWKB).clippedEnvelope(envelopeBuilder).clippedBuffer(1).algorithm(algorithm).zoom(zoom).simplifyShape(true).smallPolygon(true).outputFormat(outputFormat).size(geoHashNbPixels);
        GeoHashGridBuilder geoHashGridBuilder = new GeoHashGridBuilder("small_grid").field(geoFieldCentroid).precision(geohashLevel).size(nbBuckets).subAggregation(smallGeoShapeBuilder);
        FilterAggregationBuilder smallShapeAggregation = new FilterAggregationBuilder("small_shapes").
                filter(new RangeFilterBuilder(geoFieldArea).lt(shapeLimit)).subAggregation(geoHashGridBuilder);


        GeoShapeBuilder geoShapeBuilder = new GeoShapeBuilder("shape").field(geoFieldWKB).clippedEnvelope(envelopeBuilder).clippedBuffer(1).algorithm(algorithm).zoom(zoom).simplifyShape(true).outputFormat(outputFormat).size(nbGeom);
        FilterAggregationBuilder bigShapeAggregation = new FilterAggregationBuilder("big_shapes").filter(new RangeFilterBuilder(geoFieldArea).gt(shapeLimit)).subAggregation(geoShapeBuilder);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.aggregation(new FilterAggregationBuilder("shapes").filter(shapeFilter).subAggregation(smallShapeAggregation).subAggregation(bigShapeAggregation));
//        searchSourceBuilder.aggregation(new FilterAggregationBuilder("shapes").filter(shapeFilter).subAggregation(smallShapeAggregation));
        searchSourceBuilder.aggregation(new FilterAggregationBuilder("points").filter(pointFilter).subAggregation(pointGeoHashGridBuilder));


        searchSourceBuilder.size(0);

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
                        switch (outputFormat) {
                            case WKT:
                                geoString = new WKTWriter().write(wkbReader.read(wkb.bytes));
                                break;
                            case WKB:
                                geoString = Base64.encodeBytes(wkb.bytes);
                                break;
                            default:
                                geoString = geometryJSON.toString(wkbReader.read(wkb.bytes));
                        }

                        builder.startObject();

                        builder.field("shape", geoString);
                        builder.field("digest", String.valueOf(bucket.getHash()));
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
                            switch (outputFormat) {
                                case WKT:
                                    geoString = new WKTWriter().write(wkbReader.read(wkb.bytes));
                                    break;
                                case WKB:
                                    geoString = Base64.encodeBytes(wkb.bytes);
                                    break;
                                default:
                                    geoString = geometryJSON.toString(wkbReader.read(wkb.bytes));
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

                        TopHits smallShapeAggregation = bucketGrid.getAggregations().get("shape");

                        for (SearchHit bucket: smallShapeAggregation.getHits()){

                            String [] geoFieldsHierarchy = geoField.split("\\.");

                            List<Double> coords = getCoordinatesFromSource(geoFieldsHierarchy, bucket.sourceAsMap());

                            Point geom = new GeometryFactory().createPoint(new Coordinate(coords.get(0), coords.get(1)));

                            byte[] wkb = new WKBWriter().write(geom);

//                            BytesRef wkb = bucket.getShapeAsByte();

                            String geoString;
                            switch (outputFormat) {
                                case WKT:
                                    geoString = new WKTWriter().write(geom);
                                    break;
                                case WKB:
                                    geoString = Base64.encodeBytes(wkb);
                                    break;
                                default:
                                    geoString = geometryJSON.toString(geom);
                            }

                            builder.startObject();

                            builder.field("shape", geoString);
//                            builder.field("digest", bucket.getFields().get(geoFieldDigest).getValue());
                            builder.field("digest", bucket.getSortValues()[0]);
                            builder.field("type", geom.getGeometryType());
                            builder.field("doc_count", smallShapeAggregation.getHits().getTotalHits());
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

    private static List<Double> getCoordinatesFromSource(String[] geoFieldHierarchy, Map<String, Object> sourceMap) {
        for (String hiera: geoFieldHierarchy) {
            Object level = sourceMap.get(hiera);
            if (level instanceof Map)
                sourceMap = (Map<String, Object>)level;
        }
        return (List<Double>) sourceMap.get("coordinates");
    }
}
