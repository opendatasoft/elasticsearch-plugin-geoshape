package com.opendatasoft.elasticsearch.action.geo;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class GeoSimpleRequest extends ActionRequest<GeoSimpleRequest> {

    private SearchRequest searchRequest;

    public void setSearchRequest(SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
    }

    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
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
