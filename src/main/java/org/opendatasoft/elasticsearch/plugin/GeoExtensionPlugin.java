package org.opendatasoft.elasticsearch.plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.opendatasoft.elasticsearch.ingest.GeoExtensionProcessor;
import org.opendatasoft.elasticsearch.script.ScriptGeoSimplify;
import org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.GeoShapeBuilder;
import org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape.InternalGeoShape;


public class GeoExtensionPlugin extends Plugin implements IngestPlugin, ScriptPlugin, SearchPlugin {
    // Ingest plugin method
    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Collections.singletonMap(GeoExtensionProcessor.TYPE, new GeoExtensionProcessor.Factory());
    }

    // Script plugin method
    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        return new ScriptGeoSimplify();
    }

    // Search plugin method
    @Override
    public ArrayList<SearchPlugin.AggregationSpec> getAggregations() {
        ArrayList<SearchPlugin.AggregationSpec> r = new ArrayList<>();

        r.add(
                new SearchPlugin.AggregationSpec(
                        GeoShapeBuilder.NAME,
                        GeoShapeBuilder::new,
                        GeoShapeBuilder::parse)
                        .addResultReader(InternalGeoShape::new)
        );

        return r;
    }
}
