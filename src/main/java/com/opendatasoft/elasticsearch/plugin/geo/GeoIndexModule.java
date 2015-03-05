package com.opendatasoft.elasticsearch.plugin.geo;

import com.opendatasoft.elasticsearch.index.mapper.geo.RegisterGeoType;
import org.elasticsearch.common.inject.AbstractModule;

public class GeoIndexModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(RegisterGeoType.class).asEagerSingleton();
    }
}
