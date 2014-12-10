package com.opendatasoft.elasticsearch.plugin.geo;

import com.opendatasoft.elasticsearch.action.geo.GeoAction;
import com.opendatasoft.elasticsearch.action.geo.GeoSimpleAction;
import com.opendatasoft.elasticsearch.action.geo.TransportGeoAction;
import com.opendatasoft.elasticsearch.action.geo.TransportGeoSimpleAction;
import com.opendatasoft.elasticsearch.rest.action.geo.GeoService;
import com.opendatasoft.elasticsearch.rest.action.geo.RestGeoAction;
import com.opendatasoft.elasticsearch.rest.action.geo.RestGeoAction2;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

import java.util.ArrayList;
import java.util.Collection;

import static org.elasticsearch.common.collect.Lists.newArrayList;

public class GeoPlugin extends AbstractPlugin{


    @Override
    public String name() {
        return "Geo Plugin";
    }

    @Override
    public String description() {
        return "Geo plugin for Elasticsearch";
    }

    @Override
    public Collection<Class<? extends Module>> indexModules() {
        Collection<Class<? extends Module>> modules = new ArrayList<>();
        modules.add(GeoIndexModule.class);
        return modules;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        Collection<Class<? extends LifecycleComponent>> services = newArrayList();
        services.add(GeoService.class);
        return services;
    }

    public void onModule(RestModule module) {
        module.addRestAction(RestGeoAction.class);
        module.addRestAction(RestGeoAction2.class);
    }

    public void onModule(ActionModule module) {
        module.registerAction(GeoAction.INSTANCE, TransportGeoAction.class);
        module.registerAction(GeoSimpleAction.INSTANCE, TransportGeoSimpleAction.class);
    }
}
