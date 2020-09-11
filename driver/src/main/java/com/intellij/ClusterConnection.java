package com.intellij;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.env.ClusterEnvironment;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ClusterConnection {
    private final Cluster cluster;
    private final ClusterEnvironment clusterEnvironment;

    public ClusterConnection(@NotNull Cluster cluster, @NotNull ClusterEnvironment clusterEnvironment) {
        this.cluster = cluster;
        this.clusterEnvironment = clusterEnvironment;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void close() {
        cluster.disconnect();
        clusterEnvironment.shutdown();
    }

    void initConnection(@Nullable String defaultBucket) {
        if (defaultBucket != null && !defaultBucket.isEmpty()) {
            cluster.bucket(defaultBucket);
        }
    }
}
