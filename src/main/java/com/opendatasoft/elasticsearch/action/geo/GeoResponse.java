package com.opendatasoft.elasticsearch.action.geo;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: clement
 * Date: 03/12/14
 * Time: 18:02
 * To change this template use File | Settings | File Templates.
 */
public class GeoResponse extends BroadcastOperationResponse {

    GeoResponse() {
    }

    public GeoResponse(int totalShards, int successfulShards, int failedShards,
                       List<ShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
