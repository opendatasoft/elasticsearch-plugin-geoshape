package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.ESTestCase;
import org.opendatasoft.elasticsearch.plugin.GeoUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Covers the two places the collected values have to survive intact: the wire between a shard and
 * the coordinator, and the merge of the buckets that describe the same shape.
 */
public class CollectFieldsTests extends ESTestCase {

    private static BytesRef ref(String value) {
        return new BytesRef(value);
    }

    /** One bucket holding {@code documents}, given as one array of values per document and per field. */
    private static InternalGeoShape.InternalBucket bucket(String wkbHash, boolean truncated, String[][]... documentsPerField) {
        BytesRef[][][] collected = null;
        if (documentsPerField.length > 0) {
            collected = new BytesRef[documentsPerField.length][][];
            for (int field = 0; field < documentsPerField.length; field++) {
                String[][] documents = documentsPerField[field];
                BytesRef[][] perDocument = new BytesRef[documents.length][];
                for (int document = 0; document < documents.length; document++) {
                    BytesRef[] values = new BytesRef[documents[document].length];
                    for (int value = 0; value < values.length; value++) {
                        values[value] = ref(documents[document][value]);
                    }
                    perDocument[document] = values;
                }
                collected[field] = perDocument;
            }
        }
        return new InternalGeoShape.InternalBucket(
            ref("wkb-" + wkbHash),
            wkbHash,
            "Polygon",
            42.0,
            documentsPerField.length == 0 ? 1 : documentsPerField[0].length,
            InternalAggregations.EMPTY,
            collected,
            truncated
        );
    }

    private static List<String> flatten(InternalGeoShape.InternalBucket bucket, int field) {
        List<String> values = new ArrayList<>();
        for (BytesRef[] perDocument : bucket.collectedValuesOf(field)) {
            for (BytesRef value : perDocument) {
                values.add(value.utf8ToString());
            }
        }
        return values;
    }

    private static InternalGeoShape roundTrip(InternalGeoShape shape) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            shape.writeTo(out);
            // Sub-aggregations are read as named writeables, which needs a registry even when, as
            // here, there is none to resolve.
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), new NamedWriteableRegistry(List.of()))) {
                return new InternalGeoShape(in);
            }
        }
    }

    private static InternalGeoShape shape(CollectFieldsParams collectFields, InternalGeoShape.InternalBucket... buckets) {
        return new InternalGeoShape("g", List.of(buckets), GeoUtils.OutputFormat.WKT, collectFields, 10, 10, 0, Map.of());
    }

    public void testRoundTripCarriesCollectedValues() throws IOException {
        CollectFieldsParams params = new CollectFieldsParams(List.of("id", "tags"), 10);
        InternalGeoShape shape = shape(
            params,
            bucket("1", false, new String[][] { { "a" }, { "c" }, { "d" } }, new String[][] { { "x", "y" }, { "z" }, {} }),
            bucket("2", true, new String[][] { { "b" } }, new String[][] { { "w" } })
        );

        InternalGeoShape read = roundTrip(shape);

        assertEquals(2, read.getBuckets().size());
        assertEquals(List.of("a", "c", "d"), flatten(read.getBuckets().get(0), 0));
        assertEquals(List.of("x", "y", "z"), flatten(read.getBuckets().get(0), 1));
        // A document with no value for a field still holds its own, empty, slot
        assertEquals(3, read.getBuckets().get(0).getCollectedDocCount());
        assertEquals(0, read.getBuckets().get(0).collectedValuesOf(1)[2].length);
        assertFalse(read.getBuckets().get(0).collectedDocsTruncated);

        assertEquals(List.of("b"), flatten(read.getBuckets().get(1), 0));
        assertTrue(read.getBuckets().get(1).collectedDocsTruncated);
    }

    public void testRoundTripWithoutCollectedValuesCarriesNothing() throws IOException {
        InternalGeoShape read = roundTrip(shape(null, bucket("1", false)));

        assertEquals(1, read.getBuckets().size());
        assertNull(read.getBuckets().get(0).collectedValues);
        assertFalse(read.getBuckets().get(0).collectedDocsTruncated);
        assertEquals(0, read.getBuckets().get(0).getCollectedDocCount());
    }

    public void testMergeConcatenatesInContributionOrder() {
        InternalGeoShape.InternalBucket first = bucket("1", false, new String[][] { { "a" }, { "b" } });
        InternalGeoShape.InternalBucket second = bucket("1", false, new String[][] { { "c" } });

        InternalGeoShape.mergeCollectedValues(first, List.of(first, second), 1, 10);

        assertEquals(List.of("a", "b", "c"), flatten(first, 0));
        assertFalse(first.collectedDocsTruncated);
    }

    public void testMergeCutsOnADocumentBoundaryAndRaisesTheFlag() {
        // Two documents per contribution, each carrying two values: a bound of three documents keeps
        // all of the first contribution and one document of the second, values included.
        InternalGeoShape.InternalBucket first = bucket("1", false, new String[][] { { "a1", "a2" }, { "b1", "b2" } });
        InternalGeoShape.InternalBucket second = bucket("1", false, new String[][] { { "c1", "c2" }, { "d1", "d2" } });

        InternalGeoShape.mergeCollectedValues(first, List.of(first, second), 1, 3);

        assertEquals(List.of("a1", "a2", "b1", "b2", "c1", "c2"), flatten(first, 0));
        assertEquals(3, first.getCollectedDocCount());
        assertTrue(first.collectedDocsTruncated);
    }

    public void testMergeKeepsATruncationFlagRaisedByAShard() {
        InternalGeoShape.InternalBucket first = bucket("1", true, new String[][] { { "a" } });
        InternalGeoShape.InternalBucket second = bucket("1", false, new String[][] { { "b" } });

        InternalGeoShape.mergeCollectedValues(first, List.of(first, second), 1, 10);

        assertEquals(List.of("a", "b"), flatten(first, 0));
        assertTrue(first.collectedDocsTruncated);
    }

    public void testParamsRejectAnEmptyFieldList() {
        expectThrows(IllegalArgumentException.class, () -> new CollectFieldsParams(List.of(), 10));
    }

    public void testParamsRejectADuplicatedField() {
        expectThrows(IllegalArgumentException.class, () -> new CollectFieldsParams(List.of("id", "id"), 10));
    }

    public void testParamsRejectAMaxDocsPerBucketOutsideItsRange() {
        expectThrows(IllegalArgumentException.class, () -> new CollectFieldsParams(List.of("id"), 0));
        expectThrows(
            IllegalArgumentException.class,
            () -> new CollectFieldsParams(List.of("id"), CollectFieldsParams.MAX_DOCS_PER_BUCKET_LIMIT + 1)
        );
    }

    public void testParamsRoundTrip() throws IOException {
        CollectFieldsParams params = new CollectFieldsParams(List.of("id", "tags"), 7);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            params.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(params, new CollectFieldsParams(in));
            }
        }
    }
}
