package com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import com.opendatasoft.elasticsearch.plugin.geo.GeoPluginUtils;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier;
import com.vividsolutions.jts.simplify.TopologyPreservingSimplifier;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class GeoShapeAggregator extends BucketsAggregator {

    private static final int INITIAL_CAPACITY = 50;

    private final int requiredSize;
    private final int shardSize;
    private final ValuesSource.Bytes valuesSource;
    private final BytesRefHash bucketOrds;
    private SortedBinaryDocValues values;
    private final BytesRefBuilder previous;
    private final InternalGeoShape.OutputFormat outputFormat;
    private final GeometryFactory geometryFactory;
    private double tolerance;
    private boolean simplifyShape;
    private GeoShape.Algorithm algorithm;
//    private int zoom;

    public GeoShapeAggregator(String name, AggregatorFactories factories, ValuesSource.Bytes valuesSource,
                                 int requiredSize, int shardSize, InternalGeoShape.OutputFormat outputFormat,
                                 boolean simplifyShape, int zoom, GeoShape.Algorithm algorithm,AggregationContext aggregationContext, Aggregator parent) {

        super(name, BucketAggregationMode.PER_BUCKET, factories, INITIAL_CAPACITY, aggregationContext, parent);
        this.valuesSource = valuesSource;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.outputFormat = outputFormat;
        this.simplifyShape = simplifyShape;
        bucketOrds = new BytesRefHash(estimatedBucketCount, aggregationContext.bigArrays());
        previous = new BytesRefBuilder();
        this.geometryFactory = new GeometryFactory();
        this.algorithm = algorithm;

        if (simplifyShape) {
            tolerance = 360 / (256 * Math.pow(zoom, 3));
        }

//        nbDecimals = zoom / 2;
//        if (zoom >= 20) {
//            nbDecimals = 12;
//        }
    }


    @Override
    public boolean shouldCollect() {
        return true;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        values = valuesSource.bytesValues();
    }

    private Geometry getSimplifiedShape(Geometry geometry) {
        switch (algorithm) {
            case TOPOLOGY_PRESERVING:
                return TopologyPreservingSimplifier.simplify(geometry, tolerance);
            default:
                return DouglasPeuckerSimplifier.simplify(geometry, tolerance);
        }
    }

    private Geometry simplifyGeoShape(Geometry geom) {
//        try {

//            if (geom.getGeometryType().equals("Point")) {
//                return wkb;
//            }
//            Geometry polygonSimplified = TopologyPreservingSimplifier.simplify(geom, tolerance);
            Geometry polygonSimplified = getSimplifiedShape(geom);
            if (polygonSimplified.isEmpty()) {
                polygonSimplified = this.geometryFactory.createPoint(geom.getCoordinate());
            }
        return geom;
//            return new BytesRef(new WKBWriter().write(polygonSimplified));
//        } catch (ParseException e) {
//            return wkb;
//        }
    }


    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        values.setDocument(doc);
        final int valuesCount = values.count();

        previous.clear();
        for (int i = 0; i < valuesCount; ++i) {
            BytesRef bytes = values.valueAt(i);

            if (previous.get().equals(bytes)) {
                continue;
            }
            long bucketOrdinal = bucketOrds.add(bytes);

            if (bucketOrdinal < 0) { // already seen
                bucketOrdinal = - 1 - bucketOrdinal;
                collectExistingBucket(doc, bucketOrdinal);
            } else {
                collectBucket(doc, bucketOrdinal);
            }

            previous.copyBytes(bytes);
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalGeoShape(name, requiredSize, outputFormat, Collections.<InternalGeoShape.Bucket>emptyList());
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        assert owningBucketOrdinal == 0;

        final int size = (int) Math.min(bucketOrds.size(), shardSize);

        InternalGeoShape.BucketPriorityQueue ordered = new InternalGeoShape.BucketPriorityQueue(size);

        InternalGeoShape.Bucket spare = null;

        for (int i=0; i < bucketOrds.size(); i++) {
            if (spare == null) {
                spare = new InternalGeoShape.Bucket(new BytesRef(), 0, null, null, 0, 0, null);
            }

            bucketOrds.get(i, spare.wkb);

            spare.wkb = BytesRef.deepCopyOf(spare.wkb);

            Geometry geom = null;

            try {
                geom = new WKBReader().read(spare.wkb.bytes);
            } catch (ParseException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }

            spare.wkbHash = GeoPluginUtils.getHashFromWKB(spare.wkb);
//            spare.wkbHash = spare.wkb.bytes.hashCode();
            spare.area = geom.getLength();

            if (simplifyShape) {
                Geometry simplifiedGeom = simplifyGeoShape(geom);
                spare.simplifiedType = simplifiedGeom.getGeometryType();
                spare.wkb = new BytesRef(new WKBWriter().write(simplifiedGeom));
                spare.area = simplifiedGeom.getLength();
            }


            spare.docCount = bucketDocCount(i);
            spare.realType = geom.getGeometryType();
            spare.bucketOrd = i;

            spare = ordered.insertWithOverflow(spare);
        }

        final InternalGeoShape.Bucket[] list = new InternalGeoShape.Bucket[ordered.size()];

        for (int i = ordered.size() - 1; i >= 0; --i) {
            final InternalGeoShape.Bucket bucket = ordered.pop();
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
            list[i] = bucket;
        }

        return new InternalGeoShape(name, requiredSize, outputFormat, Arrays.asList(list));

    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrds);
    }
}
