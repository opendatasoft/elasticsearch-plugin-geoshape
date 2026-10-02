package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.List;

/**
 * The values {@code collect_fields} read for one bucket, as {@code [field][document][value]}, and whether the bucket
 * holds more documents than they came from.
 *
 * <p>Grouped by document rather than flattened, so that merging several shards can cut on a document boundary,
 * which is what {@code max_docs_per_bucket} bounds. Every field holds the same number of document groups, empty
 * ones included.
 */
final class CollectedValues implements Writeable {

    private final BytesRef[][][] values;
    private final boolean truncated;

    CollectedValues(BytesRef[][][] values, boolean truncated) {
        this.values = values;
        this.truncated = truncated;
    }

    CollectedValues(StreamInput in) throws IOException {
        int fields = in.readVInt();
        int documents = in.readVInt();
        values = new BytesRef[fields][documents][];
        for (int field = 0; field < fields; field++) {
            for (int document = 0; document < documents; document++) {
                BytesRef[] documentValues = new BytesRef[in.readVInt()];
                for (int value = 0; value < documentValues.length; value++) {
                    documentValues[value] = in.readBytesRef();
                }
                values[field][document] = documentValues;
            }
        }
        truncated = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(values.length);
        out.writeVInt(documentCount());
        for (BytesRef[][] field : values) {
            for (BytesRef[] documentValues : field) {
                out.writeVInt(documentValues.length);
                for (BytesRef value : documentValues) {
                    out.writeBytesRef(value);
                }
            }
        }
        out.writeBoolean(truncated);
    }

    /** How many documents the values were read from. */
    int documentCount() {
        return values.length == 0 ? 0 : values[0].length;
    }

    /** The values of one field, one array per document. */
    BytesRef[][] valuesOf(int field) {
        return values[field];
    }

    boolean truncated() {
        return truncated;
    }

    /**
     * Concatenate what several shards collected for the same shape, in contribution order, and cut at
     * {@code maxDocsPerBucket} documents: a document contributes all of its values or none of them.
     */
    static CollectedValues merge(List<CollectedValues> contributions, int maxDocsPerBucket) {
        int availableDocs = 0;
        boolean truncated = false;
        for (CollectedValues contribution : contributions) {
            availableDocs += contribution.documentCount();
            truncated |= contribution.truncated;
        }
        int keptDocs = Math.min(availableDocs, maxDocsPerBucket);

        int fields = contributions.get(0).values.length;
        BytesRef[][][] merged = new BytesRef[fields][keptDocs][];
        for (int field = 0; field < fields; field++) {
            int document = 0;
            for (CollectedValues contribution : contributions) {
                BytesRef[][] perDocument = contribution.values[field];
                for (int i = 0; i < perDocument.length && document < keptDocs; i++) {
                    merged[field][document++] = perDocument[i];
                }
            }
        }
        return new CollectedValues(merged, truncated || availableDocs > keptDocs);
    }
}
