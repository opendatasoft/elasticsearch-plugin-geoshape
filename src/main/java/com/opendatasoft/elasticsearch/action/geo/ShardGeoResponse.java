package com.opendatasoft.elasticsearch.action.geo;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.index.shard.ShardId;

/**
 * Created with IntelliJ IDEA.
 * User: clement
 * Date: 05/12/14
 * Time: 10:47
 * To change this template use File | Settings | File Templates.
 */
public class ShardGeoResponse extends BroadcastShardOperationResponse {

    ShardGeoResponse(){
    }

    ShardGeoResponse(ShardId shardId) {
        super(shardId);
    }
}
