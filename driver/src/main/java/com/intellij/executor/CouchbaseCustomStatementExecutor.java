package com.intellij.executor;

import com.intellij.CouchbaseConnection;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class CouchbaseCustomStatementExecutor {
    private static final List<CustomDdlExecutor> CUSTOM_EXECUTORS =
            Arrays.asList(new CreateBucketExecutor(), new DropBucketExecutor(), new DescribeBucketExecutor(),
                    new DescribeIndexExecutor());

    public static boolean mayExecute(String sql) {
        String trimmedSql = sql.trim();
        return CUSTOM_EXECUTORS.stream()
                .anyMatch(executor -> executor.mayAccept(trimmedSql));
    }

    public static ExecutionResult tryExecuteDdlStatement(CouchbaseConnection connection, String sql)
            throws SQLException {
        sql = sql.trim();
        for (CustomDdlExecutor executor : CUSTOM_EXECUTORS) {
            if (executor.mayAccept(sql)) {
                if (connection.isReadOnly() && executor.isRequireWriteAccess()) {
                    throw new SQLException(
                            "The server or request is read-only and cannot accept this write statement.");
                }
                return executor.execute(connection, sql);
            }
        }
        return new ExecutionResult(false);
    }
}
