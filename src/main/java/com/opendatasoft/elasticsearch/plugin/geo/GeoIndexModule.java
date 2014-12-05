package com.opendatasoft.elasticsearch.plugin.geo;

import com.opendatasoft.elasticsearch.index.mapper.geo.RegisterGeoType;
import org.elasticsearch.common.inject.AbstractModule;

/**
 * Created with IntelliJ IDEA.
 * User: clement
 * Date: 03/12/14
 * Time: 15:00
 * To change this template use File | Settings | File Templates.
 */
public class GeoIndexModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(RegisterGeoType.class).asEagerSingleton();
    }
}
