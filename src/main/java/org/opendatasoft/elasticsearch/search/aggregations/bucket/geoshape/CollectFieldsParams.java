package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The {@code collect_fields} param: the doc-values fields whose values are returned alongside each
 * bucket, and how many documents per bucket they are read from.
 */
public class CollectFieldsParams implements Writeable, ToXContentObject {

    static final ParseField FIELDS_FIELD = new ParseField("fields");
    static final ParseField MAX_DOCS_PER_BUCKET_FIELD = new ParseField("max_docs_per_bucket");

    /**
     * How many documents per bucket values are read from, when the caller does not say.
     *
     * <p>Not 1: a bucket holding several documents would then silently come back with the values of
     * an arbitrary one of them. 10 returns the common "a handful of documents share a shape" case
     * whole, and keeps the worst case bounded.
     */
    public static final int DEFAULT_MAX_DOCS_PER_BUCKET = 10;

    /**
     * Hard ceiling on {@code max_docs_per_bucket}. Matches the default of
     * {@code index.max_inner_result_window}, which bounds the size of a {@code top_hits}.
     */
    public static final int MAX_DOCS_PER_BUCKET_LIMIT = 100;

    private List<String> fields = List.of();
    private int maxDocsPerBucket = DEFAULT_MAX_DOCS_PER_BUCKET;

    private static final ObjectParser<CollectFieldsParams, Void> PARSER = new ObjectParser<>("collect_fields", CollectFieldsParams::new);
    static {
        PARSER.declareStringArray(CollectFieldsParams::setFields, FIELDS_FIELD);
        PARSER.declareInt(CollectFieldsParams::setMaxDocsPerBucket, MAX_DOCS_PER_BUCKET_FIELD);
    }

    private CollectFieldsParams() {}

    public CollectFieldsParams(List<String> fields, int maxDocsPerBucket) {
        setFields(fields);
        setMaxDocsPerBucket(maxDocsPerBucket);
        validate();
    }

    public CollectFieldsParams(StreamInput in) throws IOException {
        fields = in.readStringCollectionAsList();
        maxDocsPerBucket = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(fields);
        out.writeVInt(maxDocsPerBucket);
    }

    static CollectFieldsParams parse(XContentParser parser) throws IOException {
        CollectFieldsParams collectFieldsParams = PARSER.parse(parser, null);
        collectFieldsParams.validate();
        return collectFieldsParams;
    }

    private void setFields(List<String> fields) {
        Set<String> seen = new HashSet<>();
        List<String> parsed = new ArrayList<>(fields.size());
        for (String field : fields) {
            if (field == null || field.isEmpty()) {
                throw new IllegalArgumentException(
                    "[" + FIELDS_FIELD.getPreferredName() + "] must not hold an empty field name in geoshape aggregation."
                );
            }
            if (seen.add(field) == false) {
                // Field names become object keys in the response, so a duplicate would emit the same
                // key twice.
                throw new IllegalArgumentException(
                    "[" + FIELDS_FIELD.getPreferredName() + "] holds [" + field + "] twice in geoshape aggregation."
                );
            }
            parsed.add(field);
        }
        this.fields = List.copyOf(parsed);
    }

    private void setMaxDocsPerBucket(int maxDocsPerBucket) {
        if (maxDocsPerBucket < 1 || maxDocsPerBucket > MAX_DOCS_PER_BUCKET_LIMIT) {
            throw new IllegalArgumentException(
                "["
                    + MAX_DOCS_PER_BUCKET_FIELD.getPreferredName()
                    + "] must be within [1, "
                    + MAX_DOCS_PER_BUCKET_LIMIT
                    + "], got ["
                    + maxDocsPerBucket
                    + "] in geoshape aggregation."
            );
        }
        this.maxDocsPerBucket = maxDocsPerBucket;
    }

    private void validate() {
        if (fields.isEmpty()) {
            throw new IllegalArgumentException(
                "["
                    + FIELDS_FIELD.getPreferredName()
                    + "] is mandatory and must not be empty in the ["
                    + GeoShapeBuilder.COLLECT_FIELDS_FIELD.getPreferredName()
                    + "] parameter of geoshape aggregation."
            );
        }
    }

    public List<String> getFields() {
        return fields;
    }

    public int getMaxDocsPerBucket() {
        return maxDocsPerBucket;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELDS_FIELD.getPreferredName(), fields);
        builder.field(MAX_DOCS_PER_BUCKET_FIELD.getPreferredName(), maxDocsPerBucket);
        return builder.endObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields, maxDocsPerBucket);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        CollectFieldsParams other = (CollectFieldsParams) obj;
        return Objects.equals(fields, other.fields) && maxDocsPerBucket == other.maxDocsPerBucket;
    }
}
