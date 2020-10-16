package com.intellij.executor;

import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.IndexFailureException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.query.DropPrimaryQueryIndexOptions;
import com.intellij.CouchbaseConnection;
import com.intellij.CouchbaseError;
import com.intellij.EscapingUtil;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;

class DropBucketExecutor implements CustomDdlExecutor {
    private static final Pattern DROP_BUCKET_PATTERN = Pattern.compile(
        "DROP\\s+(BUCKET|TABLE)\\s+" + BUCKET_NAME, CASE_INSENSITIVE);

    public boolean mayAccept(@NotNull String sql) {
        return DROP_BUCKET_PATTERN.matcher(sql).matches();
    }

    public ExecutionResult execute(@NotNull CouchbaseConnection connection, @NotNull String sql) throws SQLException {
        Matcher matcher = DROP_BUCKET_PATTERN.matcher(sql);
        if (matcher.matches()) {
            Cluster cluster = connection.getCluster();
            String name = EscapingUtil.stripBackquotes(matcher.group("name"));
            String schema = matcher.group("schema");
            if (SYSTEM_SCHEMA_COLON.equals(schema)) {
                throw new SQLException("Cannot drop bucket in system schema");
            }
            try {
                try {
                    cluster.queryIndexes().dropPrimaryIndex(name,
                            DropPrimaryQueryIndexOptions.dropPrimaryQueryIndexOptions().ignoreIfNotExists(true));
                } catch (IndexFailureException ex) {
                    if (CouchbaseError.create(ex).getErrorEntries().stream()
                            .noneMatch(e -> "12003".equals(e.getErrorCode()))) {
                        throw ex; // Bucket not found error
                    }
                }
                cluster.buckets().dropBucket(name);
                IndexCommons.waitUntilOffline(cluster, name, Duration.ofSeconds(30));
            } catch (BucketNotFoundException ignore) { }
            return new ExecutionResult(true);
        }
        return new ExecutionResult(false);
    }

    @Override
    public boolean isRequireWriteAccess() {
        return true;
    }
}
