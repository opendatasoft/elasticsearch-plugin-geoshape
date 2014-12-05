package com.opendatasoft.elasticsearch.action.geo;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ClientAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;

public class GeoAction extends ClientAction<GeoRequest, GeoResponse, GeoRequestBuilder> {


    public static final GeoAction INSTANCE = new GeoAction();

    public static final String NAME = "com.opendatasoft.elasticsearch.action.geo";

    private GeoAction(){
        super(NAME);
    }

    @Override
    public GeoRequestBuilder newRequestBuilder(Client client) {
        return new GeoRequestBuilder(client);
    }

    @Override
    public GeoResponse newResponse() {
        return new GeoResponse();
    }
}
