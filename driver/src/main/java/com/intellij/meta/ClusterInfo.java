package com.intellij.meta;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.manager.GenericManagerRequest;
import com.couchbase.client.core.msg.manager.GenericManagerResponse;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class ClusterInfo {
    private final Cluster cluster;
    private JsonObject lazyClusterInfo = null;

    public ClusterInfo(Cluster cluster) {
        this.cluster = cluster;
    }

    public JsonObject getClusterInfo() throws SQLException {
        if (lazyClusterInfo == null) {
            Core core = cluster.core();
            GetClusterStatus request = new GetClusterStatus(core.context());
            core.send(request);
            GenericManagerResponse response;
            try {
                response = request.response().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new SQLException("Failed to retrieve cluster information");
            }
            if (!response.status().equals(ResponseStatus.SUCCESS)) {
                throw new SQLException("Failed to retrieve cluster information: "
                        + "Response status=" + response.status() + " "
                        + "Response body=" + new String(response.content(), StandardCharsets.UTF_8));
            }
            lazyClusterInfo = JsonObject.fromJson(response.content());
        }
        return lazyClusterInfo;
    }

    private static class GetClusterStatus extends GenericManagerRequest {
        public GetClusterStatus(CoreContext ctx) {
            super(ctx, () -> new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/pools"), false);
        }
    }
}
