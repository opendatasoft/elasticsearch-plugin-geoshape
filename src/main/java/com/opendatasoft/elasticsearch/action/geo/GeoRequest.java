package com.opendatasoft.elasticsearch.action.geo;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: clement
 * Date: 03/12/14
 * Time: 18:01
 * To change this template use File | Settings | File Templates.
 */
public class GeoRequest extends BroadcastOperationRequest<GeoRequest> {

    private SearchRequest searchRequest;

    @Nullable
    protected String routing;


    @Nullable
    private String preference;

    private BytesReference source;
    private boolean sourceUnsafe;

    private String[] types = Strings.EMPTY_ARRAY;

    long nowInMillis;


    public GeoRequest(){
    }


    public GeoRequest(String ... indices) {
        super(indices);
    }


    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        return validationException;
    }

    @Override
    protected void beforeStart() {
        if (sourceUnsafe) {
            source = source.copyBytesArray();
            sourceUnsafe = false;
        }
    }

//    public void setSearchRequest(SearchRequest searchRequest) {
//        this.searchRequest = searchRequest;
//    }
//
//    public SearchRequest getSearchRequest() {
//        return searchRequest;
//    }


    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public String routing() {
        return this.routing;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public GeoRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public GeoRequest routing(String... routings) {
        this.routing = Strings.arrayToCommaDelimitedString(routings);
        return this;
    }


    /**
     * Routing preference for executing the search on shards
     */
    public GeoRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
    }


    /**
     * The source to execute.
     */
    BytesReference source() {
        return source;
    }

    /**
     * The source to execute.
     */
    public GeoRequest source(QuerySourceBuilder sourceBuilder) {
        this.source = sourceBuilder.buildAsBytes(Requests.CONTENT_TYPE);
        this.sourceUnsafe = false;
        return this;
    }

    /**
     * The source to execute in the form of a map.
     */
    public GeoRequest source(Map querySource) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
            builder.map(querySource);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + querySource + "]", e);
        }
    }

    public GeoRequest source(XContentBuilder builder) {
        this.source = builder.bytes();
        this.sourceUnsafe = false;
        return this;
    }

    /**
     * The source to execute. It is preferable to use either {@link #source(byte[])}
     * or {@link #source(QuerySourceBuilder)}.
     */
    public GeoRequest source(String querySource) {
        this.source = new BytesArray(querySource);
        this.sourceUnsafe = false;
        return this;
    }

    /**
     * The source to execute.
     */
    public GeoRequest source(byte[] querySource) {
        return source(querySource, 0, querySource.length, false);
    }

    /**
     * The source to execute.
     */
    public GeoRequest source(byte[] querySource, int offset, int length, boolean unsafe) {
        return source(new BytesArray(querySource, offset, length), unsafe);
    }

    public GeoRequest source(BytesReference querySource, boolean unsafe) {
        this.source = querySource;
        this.sourceUnsafe = unsafe;
        return this;
    }


    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public String[] types() {
        return this.types;
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public GeoRequest types(String... types) {
        this.types = types;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        searchRequest = new SearchRequest();
        searchRequest.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        searchRequest.writeTo(out);
    }

}
