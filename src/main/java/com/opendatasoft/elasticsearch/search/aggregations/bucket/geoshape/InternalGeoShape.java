package com.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.apache.lucene.util.PriorityQueue;
import org.geotools.geojson.geom.GeometryJSON;

import java.io.IOException;
import java.util.*;

public class InternalGeoShape extends InternalAggregation implements GeoShape {

    public static final Type TYPE = new Type("geoshape", "gshape");

    public static enum OutputFormat {
        WKT,
        WKB,
        GEOJSON
    }

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalGeoShape readResult(StreamInput in) throws IOException {
            InternalGeoShape buckets = new InternalGeoShape();
            buckets.readFrom(in);
            return buckets;
        }
    };


    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }


    public static class Bucket implements GeoShape.Bucket, Comparable<Bucket>{

        protected BytesRef wkb;
        protected String wkbHash;
        protected String realType;
        protected String simplifiedType;
        protected long docCount;
        protected double area;
        long bucketOrd;
        protected InternalAggregations aggregations;

        public Bucket(BytesRef wkb, String wkbHash, String realType, String simplifiedType, double area, long docCount, InternalAggregations aggregations) {
            this.wkb = wkb;
            this.wkbHash = wkbHash;
            this.realType = realType;
            this.simplifiedType = simplifiedType;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.area = area;
        }

        @Override
        public String getKey() {
            return wkb.toString();
        }

        @Override
        public Text getKeyAsText() {
            return new StringText(getKey());
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public BytesRef getShapeAsByte() {
            return wkb;
        }

        @Override
        public long getShapeHash() {
            return wkb.hashCode();
        }

        @Override
        public String getType() {
            if (simplifiedType != null) {
                return simplifiedType;
            } else {
                return realType;
            }
        }

        @Override
        public String getHash() {
            return wkbHash;
        }

        @Override
        public int compareTo(Bucket other) {
            if (this.docCount > other.docCount) {
                return 1;
            }
            if (this.docCount < other.docCount) {
                return -1;
            }
            return 0;
        }

        public Bucket reduce(List<? extends Bucket> buckets, ReduceContext context) {
            List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
            long docCount = 0;
            for (Bucket bucket : buckets) {
                docCount += bucket.docCount;
                aggregationsList.add(bucket.aggregations);
            }
            final InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
            return new Bucket(wkb, wkbHash, realType, simplifiedType, area, docCount, aggs);
        }
    }


    private int requiredSize;
    private Collection<Bucket> buckets;
    protected Map<String, Bucket> bucketMap;
    private OutputFormat outputFormat;
    private GeometryJSON geometryJSON;

    InternalGeoShape(){}

    public InternalGeoShape(String name, int requiredSize, OutputFormat outputFormat, Collection<Bucket> buckets) {
        super(name);
        this.requiredSize = requiredSize;
        this.buckets = buckets;
        this.outputFormat = outputFormat;
        this.geometryJSON = new GeometryJSON(20);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public Collection<GeoShape.Bucket> getBuckets() {
        Object o = buckets;
        return (Collection<GeoShape.Bucket>) o;
    }

    @Override
    public GeoShape.Bucket getBucketByKey(String key) {
        if (bucketMap == null) {
            bucketMap = new HashMap<>(buckets.size());
            for (Bucket bucket: buckets) {
                bucketMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketMap.get(key);
    }

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();

        LongObjectPagedHashMap<List<Bucket>> buckets = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalGeoShape shape = (InternalGeoShape) aggregation;
            if (buckets == null) {
                buckets = new LongObjectPagedHashMap<>(shape.buckets.size(), reduceContext.bigArrays());
            }
            for (Bucket bucket : shape.buckets) {
                List<Bucket> existingBuckets = buckets.get(bucket.getShapeHash());
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<>(aggregations.size());
                    buckets.put(bucket.getShapeHash(), existingBuckets);
                }
                existingBuckets.add(bucket);
            }
        }

        final int size = (int) Math.min(requiredSize, buckets.size());

        BucketPriorityQueue ordered = new BucketPriorityQueue(size);
        for (LongObjectPagedHashMap.Cursor<List<Bucket>> cursor : buckets) {
            List<Bucket> sameCellBuckets = cursor.value;
//            System.out.println(outputGeoShape(sameCellBuckets.get(0).wkb));
            ordered.insertWithOverflow(sameCellBuckets.get(0).reduce(sameCellBuckets, reduceContext));
        }
        buckets.close();
        Bucket[] list = new Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = ordered.pop();
//            System.out.println(outputGeoShape(list[i].wkb));
        }

        return new InternalGeoShape(name, requiredSize, outputFormat, Arrays.asList(list));
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readString();
        this.requiredSize = readSize(in);
        int size = in.readVInt();
        List<Bucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            buckets.add(new Bucket(in.readBytesRef(), in.readString(), in.readString(), in.readString(), in.readDouble(), in.readVLong(), InternalAggregations.readAggregations(in)));
        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        writeSize(requiredSize, out);
        out.writeVInt(buckets.size());
        for (Bucket bucket : buckets) {
            out.writeBytesRef(bucket.wkb);
            out.writeString(bucket.wkbHash);
            out.writeString(bucket.realType);
            out.writeString(bucket.simplifiedType);
            out.writeDouble(bucket.area);
            out.writeVLong(bucket.getDocCount());
            ((InternalAggregations) bucket.getAggregations()).writeTo(out);
        }
    }

    public String outputGeoShape(BytesRef wkb) throws ParseException {
        switch (outputFormat) {
            case WKT:
                Geometry geo = new WKBReader().read(wkb.bytes);
                return new WKTWriter().write(geo);
            case WKB:
                return Base64.encodeBytes(wkb.bytes);
            default:
                geo = new WKBReader().read(wkb.bytes);
                return geometryJSON.toString(geo);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {

        builder.startArray(CommonFields.BUCKETS);
        for (Bucket bucket : buckets) {
            builder.startObject();
//            builder.field(CommonFields.KEY, bucket.getKeyAsText());
            try {
                builder.field(CommonFields.KEY, outputGeoShape(bucket.wkb));
                builder.field("digest", bucket.wkbHash);
                builder.field("type", bucket.getType());
            } catch (ParseException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            builder.field(CommonFields.DOC_COUNT, bucket.getDocCount());
            ((InternalAggregations) bucket.getAggregations()).toXContentInternal(builder, params);
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }


    static class BucketPriorityQueue extends PriorityQueue<Bucket> {

        public BucketPriorityQueue(int size) {
            super(size);
        }

        @Override
        protected boolean lessThan(Bucket o1, Bucket o2) {
            double i = o2.area - o1.area;
            if (i == 0) {
                i = o2.compareTo(o1);
                if (i == 0) {
                    i = System.identityHashCode(o2) - System.identityHashCode(o1);
                }
            }
            return i > 0;
        }
    }

}
