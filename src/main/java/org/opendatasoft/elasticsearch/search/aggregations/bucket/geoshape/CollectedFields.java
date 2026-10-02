package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.FieldContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;

/**
 * Per-bucket doc-values collected during the aggregation's own collection pass, grouped by document
 * so that merging shards can cut on a document boundary. One group is appended per collected document
 * and per field, empty when that document has no value for that field, so the fields stay aligned.
 *
 * <p>The per-ordinal array comes from {@link BigArrays}; its contents are plain heap, accounted
 * against the request circuit breaker as they are appended. A {@code BytesRefArray} plus offsets
 * would account a few dozen bytes per bucket more exactly, at the price of a structure nobody can read.
 */
class CollectedFields implements Releasable {

    /**
     * What one collected value costs beyond its bytes: the {@link BytesRef} itself plus its slot in
     * the enclosing array. An estimate, used only to feed the circuit breaker.
     */
    private static final long VALUE_OVERHEAD_BYTES = 48;

    /** What one collected document costs per field: an array header plus its slot in the list. */
    private static final long DOCUMENT_OVERHEAD_BYTES = 32;

    private final BigArrays bigArrays;
    private final CollectFieldsParams params;
    private final LongConsumer accountBytes;
    /** One entry per requested field, {@code null} when the field is not mapped on this index. */
    private final FieldContext[] fieldContexts;

    /** Indexed by bucket ordinal; each entry holds, per field, one {@code BytesRef[]} per document. */
    private ObjectArray<List<BytesRef[]>[]> valuesPerBucket;

    CollectedFields(AggregationContext context, CollectFieldsParams params, LongConsumer accountBytes) {
        this.bigArrays = context.bigArrays();
        this.params = params;
        this.accountBytes = accountBytes;
        this.fieldContexts = new FieldContext[params.getFields().size()];
        for (int i = 0; i < fieldContexts.length; i++) {
            String field = params.getFields().get(i);
            // An unmapped field contributes no value; GeoShapeBuilder rejects a mapped one it cannot read.
            fieldContexts[i] = context.getFieldType(field) == null ? null : context.buildFieldContext(field);
        }
        this.valuesPerBucket = bigArrays.newObjectArray(1);
    }

    int getMaxDocsPerBucket() {
        return params.getMaxDocsPerBucket();
    }

    Leaf forLeaf(LeafReaderContext ctx) throws IOException {
        SortedBinaryDocValues[] docValues = new SortedBinaryDocValues[fieldContexts.length];
        for (int i = 0; i < fieldContexts.length; i++) {
            if (fieldContexts[i] != null) {
                docValues[i] = fieldContexts[i].indexFieldData().load(ctx).getBytesValues();
            }
        }
        return new Leaf(docValues);
    }

    /**
     * The values collected for one bucket, holding {@code docCount} documents.
     *
     * <p>Documents are in collection order, and the groups of every field describe the same
     * documents, so cutting every field at the same group index cuts them all at the same document.
     */
    CollectedValues valuesFor(long bucketOrd, long docCount) {
        BytesRef[][][] collected = new BytesRef[fieldContexts.length][][];
        List<BytesRef[]>[] bucket = bucketOrd < valuesPerBucket.size() ? valuesPerBucket.get(bucketOrd) : null;
        for (int f = 0; f < fieldContexts.length; f++) {
            collected[f] = bucket == null ? new BytesRef[0][] : bucket[f].toArray(new BytesRef[0][]);
        }
        return new CollectedValues(collected, docCount > params.getMaxDocsPerBucket());
    }

    @Override
    public void close() {
        Releasables.close(valuesPerBucket);
    }

    /**
     * Reads the requested fields off one segment. A document holding several shapes lands in several
     * buckets, so its values are read once and appended to each: not every doc-values iterator
     * supports re-advancing to the document it stands on.
     */
    class Leaf {
        private final SortedBinaryDocValues[] docValues;
        private final List<BytesRef>[] buffer;
        private int bufferedDoc = -1;

        @SuppressWarnings("unchecked")
        private Leaf(SortedBinaryDocValues[] docValues) {
            this.docValues = docValues;
            this.buffer = new List[docValues.length];
            for (int f = 0; f < docValues.length; f++) {
                buffer[f] = new ArrayList<>();
            }
        }

        /** Append the values of {@code doc} to {@code bucketOrd}. The caller, which counts documents, bounds it. */
        void collect(int doc, long bucketOrd) throws IOException {
            if (bufferedDoc != doc) {
                readIntoBuffer(doc);
                bufferedDoc = doc;
            }
            appendBufferTo(bucketOrd);
        }

        private void readIntoBuffer(int doc) throws IOException {
            for (int f = 0; f < docValues.length; f++) {
                buffer[f].clear();
                if (docValues[f] == null || docValues[f].advanceExact(doc) == false) {
                    continue;
                }
                int count = docValues[f].docValueCount();
                for (int i = 0; i < count; i++) {
                    // nextValue() hands back a reused BytesRef, so the copy is not optional. The
                    // copies are shared with every bucket this document lands in, which is safe:
                    // nothing mutates them afterwards.
                    buffer[f].add(BytesRef.deepCopyOf(docValues[f].nextValue()));
                }
            }
        }

        private void appendBufferTo(long bucketOrd) {
            List<BytesRef[]>[] bucket = bucketFor(bucketOrd);
            long bytes = 0;
            for (int f = 0; f < buffer.length; f++) {
                BytesRef[] documentValues = buffer[f].toArray(new BytesRef[0]);
                bucket[f].add(documentValues);
                bytes += DOCUMENT_OVERHEAD_BYTES;
                for (BytesRef value : documentValues) {
                    bytes += VALUE_OVERHEAD_BYTES + value.length;
                }
            }
            // Once per collected document, not batched: a call costs about 225 ns, mostly the heap read of the
            // real-memory breaker, and calls are bounded by distinct shapes x (1 + max_docs_per_bucket).
            // Batching would only let the request run further past the limit before it trips.
            accountBytes.accept(bytes);
        }

        @SuppressWarnings("unchecked")
        private List<BytesRef[]>[] bucketFor(long bucketOrd) {
            valuesPerBucket = bigArrays.grow(valuesPerBucket, bucketOrd + 1);
            List<BytesRef[]>[] bucket = valuesPerBucket.get(bucketOrd);
            if (bucket == null) {
                bucket = new List[docValues.length];
                for (int f = 0; f < docValues.length; f++) {
                    bucket[f] = new ArrayList<>();
                }
                valuesPerBucket.set(bucketOrd, bucket);
                accountBytes.accept(DOCUMENT_OVERHEAD_BYTES * (docValues.length + 1L));
            }
            return bucket;
        }
    }
}
