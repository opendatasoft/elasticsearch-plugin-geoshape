package com.opendatasoft.elasticsearch.rest.action.geo;

import com.opendatasoft.elasticsearch.action.geo.parser.GeoParser;
import org.elasticsearch.common.inject.AbstractModule;

/**
 * Created with IntelliJ IDEA.
 * User: clement
 * Date: 04/12/14
 * Time: 16:39
 * To change this template use File | Settings | File Templates.
 */
public class GeoModule extends AbstractModule {

    @Override
    protected void configure() {
//        bind(GeoPhase.class).asEagerSingleton();

        bind(GeoParser.class).asEagerSingleton();

    }
}
