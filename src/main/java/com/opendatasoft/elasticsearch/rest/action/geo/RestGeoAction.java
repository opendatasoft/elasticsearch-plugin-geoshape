package com.opendatasoft.elasticsearch.rest.action.geo;

import com.opendatasoft.elasticsearch.action.geo.GeoSimpleAction;
import com.opendatasoft.elasticsearch.action.geo.GeoSimpleRequest;
import com.opendatasoft.elasticsearch.index.mapper.geo.GeoMapper;
import com.opendatasoft.elasticsearch.plugin.geo.GeoPluginUtils;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.geohashclustering.GeoHashClustering;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.geohashclustering.GeoHashClusteringBuilder;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.GeoShape;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.GeoShapeBuilder;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.InternalGeoShape;
import com.spatial4j.core.io.GeohashUtils;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTWriter;
import com.vividsolutions.jts.operation.buffer.BufferParameters;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.elasticsearch.rest.RestStatus.OK;

public class RestGeoAction extends BaseRestHandler {


    @Inject
    public RestGeoAction(Settings settings, Client client, RestController controller) {
        super(settings, controller, client);

        // register search handler
        // specifying and index and type is optional
        controller.registerHandler(RestRequest.Method.GET, "/_geo/{zoom}/{x}/{y}", this);
        controller.registerHandler(RestRequest.Method.GET, "/{index}/_geo/{zoom}/{x}/{y}", this);
        controller.registerHandler(RestRequest.Method.GET, "/{index}/{type}/_geo/{zoom}/{x}/{y}", this);
        controller.registerHandler(RestRequest.Method.POST, "/_geo/{zoom}/{x}/{y}", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_geo/{zoom}/{x}/{y}", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/{type}/_geo/{zoom}/{x}/{y}", this);
    }

    @Override
    protected void handleRequest(final RestRequest request, final RestChannel channel, final Client client) throws Exception {
        SearchRequest searchRequest = RestSearchAction.parseSearchRequest(request);
        searchRequest.listenerThreaded(true);
        final GeoSimpleRequest geoSimpleRequest = new GeoSimpleRequest();

        final WKTWriter wktWriter = new WKTWriter();
        final WKBReader wkbReader = new WKBReader();

        final GeometryJSON geometryJSON = new GeometryJSON(20);

        String geoField = request.param("field");
        final int zoom = request.paramAsInt("zoom", 1);
        final int x = request.paramAsInt("x", 0);
        final int y = request.paramAsInt("y", 0);
        int tileSize = request.paramAsInt("tile_size", 256);
        final int nbGeom = request.paramAsInt("nb_geom", 1000);
        String stringOutputFormat = request.param("output_format", "wkt");
        String outputProjection = request.param("output_projection", "EPSG:4326");
        String stringAlgorithm = request.param("algorithm", "TOPOLOGY_PRESERVING");


        XContentParser parser = XContentHelper.createParser(request.content());
        XContentParser.Token token;
        String currentFieldName = null;

        Map<String, Object> aggsParam = null;

        SearchSourceBuilder defaultQuery = new SearchSourceBuilder();

        while((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("aggs".equals(currentFieldName) || "aggregations".equals(currentFieldName)) {
                    aggsParam = parser.map();
                } else if ("query".equals(currentFieldName)) {
                    defaultQuery.query(parser.map());
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("field".equals(currentFieldName)) {
                    geoField = parser.text();
                } else if ("output_format".equals(currentFieldName)) {
                    stringOutputFormat = parser.text();
                } else if ("algorithm".equals(currentFieldName)) {
                    stringAlgorithm = parser.text();
                } else if ("output_projection".equals(currentFieldName)) {
                    outputProjection = parser.text();
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("tile_size".equals(currentFieldName)) {
                    tileSize = parser.intValue();
                }
            }
        }
        if (geoField == null) {
            throw new Exception("Field parameter is mandatory");
        }
        final String geoFieldWKB = geoField + "." + GeoMapper.Names.WKB;
        final String geoFieldType = geoField + "." + GeoMapper.Names.TYPE;
        final String geoFieldArea = geoField + "." + GeoMapper.Names.AREA;
        final String geoFieldCentroid = geoField + "." + GeoMapper.Names.CENTROID;
        final String geoFieldDigest = geoField + "." + GeoMapper.Names.HASH;

        final GeoShape.Algorithm algorithm = GeoShape.Algorithm.valueOf(stringAlgorithm.toUpperCase());
        final InternalGeoShape.OutputFormat outputFormat = InternalGeoShape.OutputFormat.valueOf(stringOutputFormat.toUpperCase());

        // Force axis order : http://docs.geotools.org/latest/userguide/library/referencing/order.html
        System.setProperty("org.geotools.referencing.forceXY", "true");

        final CoordinateReferenceSystem sourceCRS = CRS.decode("EPSG:4326");
        final CoordinateReferenceSystem targetCRS = CRS.decode(outputProjection);

        final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS);

        final boolean mustProject = ! outputProjection.equals("EPSG:4326");

        Envelope tileEnvelope = GeoPluginUtils.tile2boundingBox(x, y, zoom, tileSize);

//        Envelope jtsEnvelope = new Envelope(tileEnvelope.west, tileEnvelope.east, tileEnvelope.south, tileEnvelope.north);

        EnvelopeBuilder envelopeBuilder = ShapeBuilder.newEnvelope();
        envelopeBuilder.topLeft(tileEnvelope.getMinX(), tileEnvelope.getMaxY()).bottomRight(tileEnvelope.getMaxX(), tileEnvelope.getMinY());

        Coordinate centroid = tileEnvelope.centre();

        // Compute shape length limit delimiting big and small shapes

        int smallShapePixels = 10;
        double shapeLimit = GeoPluginUtils.getShapeLimit(zoom, centroid.y, smallShapePixels);
        double lineStringLimit = GeoPluginUtils.getShapeLimit(zoom, centroid.y, 1);


        double degrees = GeoPluginUtils.getDecimalDegreeFromMeter(40 * GeoPluginUtils.getMeterByPixel(zoom, centroid.y), centroid.y);

        int geohashLevel = GeohashUtils.lookupHashLenForWidthHeight(degrees, degrees);

        double meterByPixel = GeoPluginUtils.getMeterByPixel(zoom, centroid.y);

        int geoHashNbPixels = (int) ((GeoUtils.geoHashCellWidth(geohashLevel) * GeoUtils.geoHashCellHeight(geohashLevel)) / Math.pow(meterByPixel, 2));
        int nbBuckets = (tileSize * tileSize) / geoHashNbPixels;

        Envelope overflowEnvelope = GeoPluginUtils.overflowEnvelope(tileEnvelope, 40, tileSize);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();


        // Aggregate Point geometries

        FilterBuilder pointFilter = new BoolFilterBuilder().must(
                new TermFilterBuilder(geoFieldType, "Point"),
                new GeoShapeFilterBuilder(geoField,
                        ShapeBuilder.newEnvelope()
                                .topLeft(overflowEnvelope.getMinX(), overflowEnvelope.getMaxY())
                                .bottomRight(overflowEnvelope.getMaxX(), overflowEnvelope.getMinY()))
        );

        GeoHashClusteringBuilder pointGeoHashGridBuilder = new GeoHashClusteringBuilder("point_agg").field(geoFieldCentroid).zoom(zoom).distance(10);

        if (aggsParam != null) {
            pointGeoHashGridBuilder.subAggregation(aggsParam);
        }

        searchSourceBuilder.aggregation(new FilterAggregationBuilder("point_filter").filter(pointFilter).subAggregation(pointGeoHashGridBuilder));

        // Aggregate LineString geometries
        // We drop geometry with too small length

        FilterBuilder lineStringFilter = new BoolFilterBuilder().must(
                new TermFilterBuilder(geoFieldType, "LineString"),
                new GeoShapeFilterBuilder(geoField,
                        ShapeBuilder.newEnvelope()
                                .topLeft(tileEnvelope.getMinX(), tileEnvelope.getMaxY())
                                .bottomRight(tileEnvelope.getMaxX(), tileEnvelope.getMinY()))
        );

//        GeoShapeBuilder samallLineStringFirstHit = new GeoShapeBuilder("small_line_string_top_hit").field(geoFieldWKB).zoom(zoom).simplifyShape(true).outputFormat(outputFormat).size(1);
//        GeoHashClusteringBuilder smallLineStringGrid = new GeoHashClusteringBuilder("small_line_string_agg").field(geoFieldCentroid).zoom(zoom).distance(1).subAggregation(samallLineStringFirstHit);

        GeoHashClusteringBuilder smallLineStringGrid = new GeoHashClusteringBuilder("small_line_string_agg").field(geoFieldCentroid).zoom(zoom).distance(1);

        if (aggsParam != null) {
            smallLineStringGrid.subAggregation(aggsParam);
        }

        FilterAggregationBuilder smallLineStringFilter = new FilterAggregationBuilder("small_line_string_filter").
                filter(new RangeFilterBuilder(geoFieldArea).lt(lineStringLimit)).subAggregation(smallLineStringGrid);


        GeoShapeBuilder bigLineStringAggregation = new GeoShapeBuilder("big_line_string_agg").field(geoFieldWKB).clippedEnvelope(envelopeBuilder).clippedBuffer(1).algorithm(algorithm).zoom(zoom).simplifyShape(true).outputFormat(outputFormat).size(nbGeom);

        if (aggsParam != null) {
            bigLineStringAggregation.subAggregation(aggsParam);
        }

        FilterAggregationBuilder bigLineStringFilter = new FilterAggregationBuilder("big_line_string_filter").
                filter(new RangeFilterBuilder(geoFieldArea).gt(lineStringLimit)).subAggregation(bigLineStringAggregation);

        searchSourceBuilder.aggregation(new FilterAggregationBuilder("line_string_filter").filter(lineStringFilter).subAggregation(smallLineStringFilter).subAggregation(bigLineStringFilter));


        // Aggregate Polygons

        FilterBuilder polygonFilter = new BoolFilterBuilder().must(
                new NotFilterBuilder(new TermFilterBuilder(geoFieldType, "Point")),
                new NotFilterBuilder(new TermFilterBuilder(geoFieldType, "LineString")),
                new GeoShapeFilterBuilder(geoField,
                        ShapeBuilder.newEnvelope()
                                .topLeft(tileEnvelope.getMinX(), tileEnvelope.getMaxY())
                                .bottomRight(tileEnvelope.getMaxX(), tileEnvelope.getMinY()))
        );


        // Aggregate small polygons to geohash clusters
        GeoHashClusteringBuilder smallPolygonsGrid = new GeoHashClusteringBuilder("small_polygon_agg").field(geoFieldCentroid).zoom(zoom).distance(1);

        if (aggsParam != null) {
            smallPolygonsGrid.subAggregation(aggsParam);
        }

        FilterAggregationBuilder smallPolygonFilter = new FilterAggregationBuilder("small_polygon_filter").
                filter(new RangeFilterBuilder(geoFieldArea).lt(shapeLimit)).subAggregation(smallPolygonsGrid);


        // Aggregate big polygons to geohash clusters
        GeoShapeBuilder bigPolygonsAggregation = new GeoShapeBuilder("big_polygon_agg").field(geoFieldWKB).clippedEnvelope(envelopeBuilder).clippedBuffer(1).algorithm(algorithm).zoom(zoom).simplifyShape(true).outputFormat(outputFormat).size(nbGeom);

        if (aggsParam != null) {
            bigPolygonsAggregation.subAggregation(aggsParam);
        }

        FilterAggregationBuilder bigPolygonsFilter = new FilterAggregationBuilder("big_polygon_filter").
                filter(new RangeFilterBuilder(geoFieldArea).gt(shapeLimit)).subAggregation(bigPolygonsAggregation);

        searchSourceBuilder.aggregation(new FilterAggregationBuilder("polygon_filter").filter(polygonFilter).subAggregation(smallPolygonFilter).subAggregation(bigPolygonsFilter));

        searchSourceBuilder.size(0);

        searchRequest.searchType(SearchType.COUNT);

        searchRequest.source(searchSourceBuilder);

        searchRequest.extraSource(defaultQuery);

        geoSimpleRequest.setSearchRequest(searchRequest);

        class OutputFormatter {
            String getGeoStringFromWKB(byte[] wkb) throws ParseException {
                switch (outputFormat) {
                    case WKT:
                        return wktWriter.write(new WKBReader().read(wkb));
                    case WKB:
                        return Base64.encodeBytes(wkb);
                    default:
                        return geometryJSON.toString(wkbReader.read(wkb));
                }
            }
        }

        final OutputFormatter outputFormatter = new OutputFormatter();

        client.execute(GeoSimpleAction.INSTANCE, geoSimpleRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {
                try {

                    // Retrieve Point aggregation
                    SingleBucketAggregation pointFilter = response.getAggregations().get("point_filter");
                    GeoHashClustering pointAggregation = pointFilter.getAggregations().get("point_agg");

                    // Retrieve LineString (small and big) aggregations
                    SingleBucketAggregation lineStringFilter = response.getAggregations().get("line_string_filter");

                    // Retrieve small LineString aggregation
                    SingleBucketAggregation smallLineStringFilter = lineStringFilter.getAggregations().get("small_line_string_filter");
                    GeoHashClustering smallLineStringAggregation = smallLineStringFilter.getAggregations().get("small_line_string_agg");


                    // Retrieve big LineString aggregation
                    SingleBucketAggregation bigLineStringFilter = lineStringFilter.getAggregations().get("big_line_string_filter");
                    GeoShape bigLineStringAggregation = bigLineStringFilter.getAggregations().get("big_line_string_agg");

//                    GeoShape lineStringAggregation = lineStringFilter.getAggregations().get("line_string_agg");

                    // Retrieve Polygon (small and big) aggregations

                    SingleBucketAggregation polygonFilter = response.getAggregations().get("polygon_filter");

                    // Retrieve small Polygon aggregation
                    SingleBucketAggregation smallPolygonFilter = polygonFilter.getAggregations().get("small_polygon_filter");
                    GeoHashClustering smallPolygonAggregation = smallPolygonFilter.getAggregations().get("small_polygon_agg");


                    // Retrieve big Polygon aggregation
                    SingleBucketAggregation bigPolygonFilter = polygonFilter.getAggregations().get("big_polygon_filter");
                    GeoShape bigPolygonAggregation = bigPolygonFilter.getAggregations().get("big_polygon_agg");

                    final XContentBuilder builder = channel.newBuilder();

                    GeometryFactory geometryFactory = new GeometryFactory();

                    builder.startObject();
                    builder.field("time", response.getTookInMillis());
                    builder.field("count", polygonFilter.getDocCount() + pointFilter.getDocCount() + lineStringFilter.getDocCount());

                    builder.field("shapes");

                    builder.startArray();

                    for (GeoShape.Bucket bucket : bigPolygonAggregation.getBuckets()) {
                        byte[] wkb = bucket.getShapeAsByte().bytes;

                        if (mustProject) {
                            Geometry geom = wkbReader.read(wkb);
                            geom = JTS.transform(geom, transform);
                            wkb = new WKBWriter().write(geom);
                        }

                        String geoString = outputFormatter.getGeoStringFromWKB(wkb);

                        builder.startObject();

                        builder.field("shape", geoString);
                        builder.field("digest", String.valueOf(bucket.getHash()));
                        builder.field("type", bucket.getType());
                        builder.field("doc_count", bucket.getDocCount());

                        addAggregationResult(builder, bucket);

                        builder.endObject();
                    }

                    for (GeoShape.Bucket bucket : bigLineStringAggregation.getBuckets()) {
                        byte[] wkb = bucket.getShapeAsByte().bytes;

                        if (mustProject) {
                            Geometry geom = wkbReader.read(wkb);
                            geom = JTS.transform(geom, transform);
                            wkb = new WKBWriter().write(geom);
                        }

                        String geoString = outputFormatter.getGeoStringFromWKB(wkb);

                        builder.startObject();

                        builder.field("shape", geoString);
                        builder.field("digest", String.valueOf(bucket.getHash()));
                        builder.field("type", bucket.getType());
                        builder.field("doc_count", bucket.getDocCount());

                        addAggregationResult(builder, bucket);

                        builder.endObject();
                    }

                    for (GeoHashClustering.Bucket bucketGrid : smallPolygonAggregation.getBuckets()) {

                        GeoPoint pointHash = bucketGrid.getClusterCenter();

                        Geometry jtsPoint = geometryFactory.createPoint(new Coordinate(pointHash.lon(), pointHash.lat()));

                        double bufferSize = GeoPluginUtils.getShapeLimit(zoom, jtsPoint.getCoordinate().y);
                        Geometry geom = jtsPoint.buffer(bufferSize, 4, BufferParameters.CAP_SQUARE);

                        if (mustProject) {
                            geom = JTS.transform(geom, transform);
                        }

                        byte[] geoPointWkb = new WKBWriter().write(geom);

                        String geoString = outputFormatter.getGeoStringFromWKB(geoPointWkb);

                        builder.startObject();

                        builder.field("shape", geoString);
//                        builder.field("digest", bucket.getHash());
                        builder.field("type", "Polygon");
                        builder.field("doc_count", bucketGrid.getDocCount());
                        builder.field("cluster_count", bucketGrid.getDocCount());
                        builder.field("grid", bucketGrid.getKey());

                        addAggregationResult(builder, bucketGrid);

                        builder.endObject();
                    }

                    for (GeoHashClustering.Bucket bucketGrid : smallLineStringAggregation.getBuckets()) {

                        GeoPoint pointHash = bucketGrid.getClusterCenter();

                        Geometry jtsPoint = geometryFactory.createPoint(new Coordinate(pointHash.lon(), pointHash.lat()));

                        double bufferSize = GeoPluginUtils.getShapeLimit(zoom, jtsPoint.getCoordinate().y);
                        Geometry geom = jtsPoint.buffer(bufferSize / 3, 4, BufferParameters.CAP_SQUARE);

                        if (mustProject) {
                            geom = JTS.transform(geom, transform);
                        }

                        byte[] geoPointWkb = new WKBWriter().write(geom);

                        String geoString = outputFormatter.getGeoStringFromWKB(geoPointWkb);

                        builder.startObject();

                        builder.field("shape", geoString);
//                        builder.field("digest", bucket.getHash());
                        builder.field("type", "LineString");
                        builder.field("doc_count", bucketGrid.getDocCount());
                        builder.field("cluster_count", bucketGrid.getDocCount());
                        builder.field("grid", bucketGrid.getKey());

                        addAggregationResult(builder, bucketGrid);

                        builder.endObject();
                    }


                    for (GeoHashClustering.Bucket bucketGrid : pointAggregation.getBuckets()) {

                        GeoPoint pointHash = bucketGrid.getClusterCenter();

                        Coordinate coord = new Coordinate(pointHash.getLon(), pointHash.getLat());

                        if (mustProject) {
                            JTS.transform(coord, coord, transform);
                        }

                        Geometry pointGeometry = geometryFactory.createPoint(coord);

                        byte[] geoPointWkb = new WKBWriter().write(pointGeometry);

                        String geoString = outputFormatter.getGeoStringFromWKB(geoPointWkb);

                        builder.startObject();

                        builder.field("shape", geoString);
                        builder.field("type", "Point");
                        builder.field("doc_count", bucketGrid.getDocCount());
                        builder.field("cluster_count", bucketGrid.getDocCount());
                        builder.field("grid", bucketGrid.getKey());

                        addAggregationResult(builder, bucketGrid);

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

    public static void addAggregationResult(XContentBuilder builder, MultiBucketsAggregation.Bucket bucket) throws IOException {
        if (bucket.getAggregations().asList().size() > 0) {
            builder.startObject("aggregations");
            for (Aggregation agg : bucket.getAggregations()) {
                builder.value(bucket.getAggregations().get(agg.getName()));
            }
            builder.endObject();
        }
    }

}
