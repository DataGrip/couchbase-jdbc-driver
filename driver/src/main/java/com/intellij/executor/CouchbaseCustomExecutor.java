package com.intellij.executor;

import com.couchbase.client.java.Cluster;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class CouchbaseCustomExecutor {
    private static final List<CustomDdlExecutor> CUSTOM_EXECUTORS =
            Arrays.asList(new CreateBucketExecutor(), new DropBucketExecutor());

    public static boolean tryExecuteDdlStatement(Cluster cluster, String sql, boolean isReadOnly) throws SQLException {
        System.out.println("tryExecute: " + sql);
        sql = sql.trim();
        for (CustomDdlExecutor executor : CUSTOM_EXECUTORS) {
            if (executor.mayAccept(sql)) {
                if (isReadOnly) {
                    throw new SQLException("The server or request is read-only and cannot accept this write statement.");
                }
                return executor.execute(cluster, sql);
            }
        }
        return false;
    }
}
