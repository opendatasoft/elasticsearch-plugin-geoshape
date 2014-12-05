package com.opendatasoft.elasticsearch.action.geo;

import org.apache.lucene.util.Counter;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;

/**
 * Created with IntelliJ IDEA.
 * User: clement
 * Date: 05/12/14
 * Time: 10:26
 * To change this template use File | Settings | File Templates.
 */
public class GeoContext extends DefaultSearchContext {


    private String zoom;
    private String outputFormat;


    public GeoContext(long id, ShardSearchRequest request, SearchShardTarget shardTarget,
                      Engine.Searcher engineSearcher, IndexService indexService, IndexShard indexShard,
                      ScriptService scriptService, CacheRecycler cacheRecycler,PageCacheRecycler pageCacheRecycler,
                      BigArrays bigArrays, Counter timeEstimateCounter) {
        super(id, request, shardTarget, engineSearcher, indexService, indexShard, scriptService, cacheRecycler,
                pageCacheRecycler, bigArrays, timeEstimateCounter);
    }

    @Override
    public FetchSourceContext fetchSourceContext() {
        return super.fetchSourceContext();
    }

    public String getZoom() {
        return zoom;
    }

    public void setZoom(String zoom) {
        this.zoom = zoom;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(String outputFormat) {
        this.outputFormat = outputFormat;
    }
}
