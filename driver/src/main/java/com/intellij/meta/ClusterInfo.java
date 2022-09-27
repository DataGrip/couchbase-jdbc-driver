package com.intellij.meta;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.http.HttpPath;
import com.couchbase.client.java.http.HttpResponse;
import com.couchbase.client.java.http.HttpTarget;
import com.couchbase.client.java.json.JsonObject;

import java.sql.SQLException;

public class ClusterInfo {
    private final Cluster cluster;
    private volatile JsonObject lazyClusterInfo = null;

    public ClusterInfo(Cluster cluster) {
        this.cluster = cluster;
    }

    public JsonObject getClusterInfo() throws SQLException {
        if (lazyClusterInfo == null) {
            try {
                HttpResponse response = cluster.httpClient().get(HttpTarget.manager(), HttpPath.of("/pools"));
                if (!response.success()) {
                    throw new SQLException("Failed to retrieve cluster information: "
                        + "Response status=" + response.statusCode() + " "
                        + "Response body=" + response.contentAsString());
                }
                lazyClusterInfo = JsonObject.fromJson(response.content());
            } catch (SQLException e) {
                throw e;
            } catch (Exception e) {
                throw new SQLException("Failed to retrieve cluster information", e);
            }
        }
        return lazyClusterInfo;
    }
}
