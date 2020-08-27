package com.intellij;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.env.ClusterEnvironment;

public class ClusterConnection {
    private final Cluster cluster;
    private final ClusterEnvironment clusterEnvironment;

    public ClusterConnection(Cluster cluster, ClusterEnvironment clusterEnvironment) {
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
}
