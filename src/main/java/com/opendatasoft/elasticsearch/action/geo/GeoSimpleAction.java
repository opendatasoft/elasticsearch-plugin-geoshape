package com.opendatasoft.elasticsearch.action.geo;

import org.elasticsearch.action.ClientAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;

public class GeoSimpleAction extends ClientAction<GeoSimpleRequest, SearchResponse, GeoSimpleRequestBuilder>{

    public static final GeoSimpleAction INSTANCE = new GeoSimpleAction();

    public static final String NAME = "action.geosimple";

    private GeoSimpleAction() {
        super(NAME);
    }

    @Override
    public GeoSimpleRequestBuilder newRequestBuilder(Client client) {
        return new GeoSimpleRequestBuilder(client);
    }

    @Override
    public SearchResponse newResponse() {
        return new SearchResponse();
    }
}
