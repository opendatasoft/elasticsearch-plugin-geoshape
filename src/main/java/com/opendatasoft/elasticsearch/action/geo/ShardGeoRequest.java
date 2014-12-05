package com.opendatasoft.elasticsearch.action.geo;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class ShardGeoRequest extends BroadcastShardOperationRequest {

    private BytesReference source;
    private String[] types = Strings.EMPTY_ARRAY;


    private long nowInMillis;

    @Nullable
    private String[] filteringAliases;

    ShardGeoRequest(){
    }

    public ShardGeoRequest(ShardId shardId, @Nullable String[] filteringAliases, GeoRequest request) {
        super(shardId, request);
        this.source = request.source();
        this.types = request.types();
        this.filteringAliases = filteringAliases;
        this.nowInMillis = request.nowInMillis;
    }

    public BytesReference source() {
        return source;
    }

    public String[] types() {
        return this.types;
    }

    public String[] filteringAliases() {
        return filteringAliases;
    }

    public long nowInMillis() {
        return nowInMillis;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        source = in.readBytesReference();
        int typesSize = in.readVInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readString();
            }
        }
        int aliasesSize = in.readVInt();
        if (aliasesSize > 0) {
            filteringAliases = new String[aliasesSize];
            for (int i = 0; i < aliasesSize; i++) {
                filteringAliases[i] = in.readString();
            }
        }
        nowInMillis = in.readVLong();
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(source);

        out.writeVInt(types.length);
        for (String type : types) {
            out.writeString(type);
        }
        if (filteringAliases != null) {
            out.writeVInt(filteringAliases.length);
            for (String alias : filteringAliases) {
                out.writeString(alias);
            }
        } else {
            out.writeVInt(0);
        }
        out.writeVLong(nowInMillis);
    }
}
