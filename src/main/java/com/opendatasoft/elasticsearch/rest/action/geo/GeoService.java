package com.opendatasoft.elasticsearch.rest.action.geo;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

/**
 * Created with IntelliJ IDEA.
 * User: clement
 * Date: 04/12/14
 * Time: 16:33
 * To change this template use File | Settings | File Templates.
 */
public class GeoService extends AbstractLifecycleComponent<GeoService> {


    @Inject
    public GeoService(Settings settings) {
        super(settings);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }
}
