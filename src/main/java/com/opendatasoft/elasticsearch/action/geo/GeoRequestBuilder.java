package com.opendatasoft.elasticsearch.action.geo;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;

public class GeoRequestBuilder extends ActionRequestBuilder<GeoRequest, GeoResponse, GeoRequestBuilder, Client> {


    public GeoRequestBuilder(Client client) {
        super(client, new GeoRequest());

        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);

        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());

//        request.setSearchRequest(searchRequestBuilder.request());
    }


    @Override
    public GeoRequest request() {
        return request;
    }

    @Override
    protected void doExecute(ActionListener<GeoResponse> listener) {
        client.execute(GeoAction.INSTANCE, request, listener);
    }

}
