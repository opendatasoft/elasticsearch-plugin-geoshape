package com.opendatasoft.elasticsearch.action.geo;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;

public class GeoSimpleRequestBuilder extends ActionRequestBuilder<GeoSimpleRequest, SearchResponse, GeoSimpleRequestBuilder, Client> {

    public GeoSimpleRequestBuilder(Client client) {
        super(client, new GeoSimpleRequest());

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);

        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());

//        request.setSearchRequest(searchRequestBuilder.request());
    }


    @Override
    public GeoSimpleRequest request() {
        return request;
    }

    @Override
    protected void doExecute(ActionListener<SearchResponse> listener) {
        client.execute(GeoSimpleAction.INSTANCE, request, listener);
    }

}
