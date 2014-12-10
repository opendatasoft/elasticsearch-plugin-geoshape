package com.opendatasoft.elasticsearch.action.geo;

import com.opendatasoft.elasticsearch.action.geo.parser.GeoParser;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.*;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.type.TransportSearchDfsQueryThenFetchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchLocalRequest;
import org.elasticsearch.search.query.QueryPhaseExecutionException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.elasticsearch.common.collect.Lists.newArrayList;

public class TransportGeoAction extends TransportBroadcastOperationAction<GeoRequest, GeoResponse, ShardGeoRequest, ShardGeoResponse> {


//    private final ClusterService clusterService;


    private final IndicesService indicesService;
    private final ScriptService scriptService;
    private final CacheRecycler cacheRecycler;
    private final PageCacheRecycler pageCacheRecycler;
    private final BigArrays bigArrays;
    private final GeoParser geoParser;

//    TransportSearchDfsQueryThenFetchAction;

    @Inject
    public TransportGeoAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                              TransportService transportService,
                              IndicesService indicesService, ScriptService scriptService, CacheRecycler cacheRecycler,
                              PageCacheRecycler pageCacheRecycler, BigArrays bigArrays, ActionFilters actionFilters,
                              GeoParser geoParser) {

        super(settings, GeoAction.NAME, threadPool, clusterService, transportService, actionFilters);
        this.indicesService = indicesService;
        this.scriptService = scriptService;
        this.cacheRecycler = cacheRecycler;
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays;
        this.geoParser = geoParser;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SEARCH;
    }

    @Override
    protected GeoRequest newRequest() {
        return new GeoRequest();
    }

    @Override
    protected GeoResponse newResponse(GeoRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
//        List<ShardGeoResponse> responses = new ArrayList<ShardGeoResponse>();
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                failedShards++;
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = newArrayList();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
//                responses.add((ShardGeoResponse) shardResponse);
                successfulShards++;
            }
        }
//        return new GeoResponse(responses, shardsResponses.length(), successfulShards, failedShards, shardFailures);
        return new GeoResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ShardGeoRequest newShardRequest() {
        return new ShardGeoRequest();
    }

    @Override
    protected ShardGeoRequest newShardRequest(int numShards, ShardRouting shard, GeoRequest request) {
        String[] filteringAliases = clusterService.state().metaData().filteringAliases(shard.index(), request.indices());
        return new ShardGeoRequest(shard.shardId(), filteringAliases, request);
    }

    @Override
    protected ShardGeoResponse newShardResponse() {
        return new ShardGeoResponse();
    }

    @Override
    protected ShardGeoResponse shardOperation(ShardGeoRequest request) throws ElasticsearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        IndexShard indexShard = indexService.shardSafe(request.shardId().id());

        SearchShardTarget shardTarget = new SearchShardTarget(clusterService.localNode().id(), request.shardId().getIndex(), request.shardId().id());
        GeoContext context = new GeoContext(0,
                new ShardSearchLocalRequest(request.types(), request.nowInMillis(), request.filteringAliases()),
                shardTarget, indexShard.acquireSearcher("exists"), indexService, indexShard,
                scriptService, cacheRecycler, pageCacheRecycler, bigArrays, threadPool.estimatedTimeInMillisCounter());
        GeoContext.setCurrent(context);

        Engine.Searcher searcher = indexShard.acquireSearcher("termcount");
//        try {
//            IndexReader reader = searcher.reader();
//            Fields fields = MultiFields.getFields(reader);
//            if (fields != null) {
//                for (String field : fields) {
//                    // skip internal fields
//                    if (field.charAt(0) == '_') {
//                        continue;
//                    }
//
//                    Terms terms = fields.terms(field);
//                    if (terms != null) {
//                        TermsEnum termsEnum = terms.iterator(null);
//                        BytesRef text;
//                        FieldMapper<?> fieldMapper = indexShard.mapperService().smartNameFieldMapper(field);
////                        if (fieldMapper != null && fieldMapper.isNumeric()) {
////                            TermsEnum numericsEnum = NumericUtils.filterPrefixCodedLongs(termsEnum);
////                            while ((text = numericsEnum.next()) != null) {
////                                counter.offer(NumericUtils.prefixCodedToLong(text));
////                            }
////                        }
////                        else {
////                            while ((text = termsEnum.next()) != null) {
////                                counter.offer(text);
////                            }
////                        }
//                        FieldType ft = fieldMapper.fieldType();
//
//                    }
//                }
//            }
//                return new ShardGeoResponse(request.shardId());
//            } catch (IOException ex) {
//                throw new ElasticsearchException(ex.getMessage(), ex);
//            } finally {
//                searcher.close();
//            }

        try {
            BytesReference source = request.source();
            geoParser.parseSource(context, source);
            GeoCollector geoCollector = new GeoCollector(context);
            context.preProcess();
            try {
//                if (context.explain()) {
//                    return new ShardGeoResponse(request.shardId());
//                } else {
////                    Exporter.Result res = exporter.execute(context);
//                    return new ShardGeoResponse(request.shardId());
//                }
                context.searcher().search(context.query(), geoCollector);
                return new ShardGeoResponse(request.shardId());

            } catch (Exception e) {
                throw new QueryPhaseExecutionException(context, "failed to execute export", e);
            }
        } finally {
            // this will also release the index searcher
            context.close();
            SearchContext.removeCurrent();
        }
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, GeoRequest request, String[] concreteIndices) {
        Map<String, Set<String>> routingMap = clusterState.metaData().resolveSearchRouting(request.routing(), request.indices());
        return clusterService.operationRouting().searchShards(clusterState, request.indices(), concreteIndices, routingMap, request.preference());
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, GeoRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, GeoRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }


    //    @Inject
//    public TransportGeoAction(Settings settings, ThreadPool threadPool,
//                                 TransportService transportService, ClusterService clusterService, ActionFilters actionFilters) {
//        super(settings, GeoAction.NAME, threadPool, actionFilters);
//        this.clusterService = clusterService;
//        transportService.registerHandler(GeoAction.NAME, new TransportHandler());
//    }

//    @Override
//    protected void doExecute(GeoRequest request, ActionListener<SearchResponse> listener) {
//        SearchRequest searchRequest = request.getSearchRequest();
//        listener.onResponse(new SearchResponse());
//    }


//    private class TransportHandler extends BaseTransportRequestHandler<GeoRequest> {
//        @Override
//        public GeoRequest newInstance() {
//            return new GeoRequest();
//        }
//
//        @Override
//        public void messageReceived(GeoRequest request, final TransportChannel channel) throws Exception {
//            request.listenerThreaded(false);
//            execute(request, new ActionListener<SearchResponse>() {
//                @Override
//                public void onResponse(SearchResponse response) {
//                    try {
//                        channel.sendResponse(response);
//                    } catch (Throwable e) {
//                        onFailure(e);
//                    }
//                }
//
//                @Override
//                public void onFailure(Throwable e) {
//                    try {
//                        channel.sendResponse(e);
//                    } catch (Exception e1) {
//                        logger.warn("Failed to send response for search", e1);
//                    }
//                }
//            });
//        }
//
//        @Override
//        public String executor() {
//            return ThreadPool.Names.SAME;
//        }
//    }
}
