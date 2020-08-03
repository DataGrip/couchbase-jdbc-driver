package com.intellij.executor;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.bucket.BucketSettings;
import com.intellij.CouchbaseConnection;
import com.intellij.CouchbaseDocumentsSampler;
import com.intellij.meta.TableInfo;
import com.intellij.resultset.CouchbaseListResultSet;
import com.intellij.resultset.CouchbaseResultSetMetaData;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.intellij.executor.CustomDdlExecutor.startsWithIgnoreCase;
import static com.intellij.resultset.CouchbaseResultSetMetaData.createColumn;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.regex.Pattern.CASE_INSENSITIVE;

public class DescribeBucketExecutor implements CustomDdlExecutor {
    private static final Pattern DESCRIBE_BUCKET_PATTERN = Pattern.compile(
            "^DESCRIBE\\s+BUCKET\\s+(?<type>(?:(COLUMNS|SETTINGS)\\s+)?)" +
                    "(?<schema>(?:[a-zA-Z]+:)?)`(?<name>[0-9a-zA-Z_.%\\-]+)`\\s*;?\\s*",
            CASE_INSENSITIVE);
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE =
            new TypeReference<Map<String, Object>>() {};
    private static final String ROW_NAME = "result";

    @Override
    public boolean mayAccept(@NotNull String sql) {
        return startsWithIgnoreCase(sql, "DESCRIBE BUCKET");
    }

    @Override
    public boolean isRequireWriteAccess() {
        return false;
    }

    @Override
    public ExecutionResult execute(@NotNull CouchbaseConnection connection, @NotNull String sql) throws SQLException {
        Matcher matcher = DESCRIBE_BUCKET_PATTERN.matcher(sql);
        if (matcher.matches()) {
            String name = matcher.group("name");
            String type = matcher.group("type").trim();
            if (type.isEmpty() || type.equalsIgnoreCase("SETTINGS")) {
                return describeSettings(connection.getCluster(), name);
            } else if (type.equalsIgnoreCase("COLUMNS")) {
                String schema = matcher.group("schema");
                if (schema.isEmpty()) {
                    schema = "default:";
                }
                return describeColumns(connection, schema.substring(0, schema.length() - 1), name);
            } else {
                throw new SQLException("Unknown describe type: " + type);
            }
        }
        return new ExecutionResult(false);
    }

    private static ExecutionResult describeSettings(Cluster cluster, String name) {
        BucketSettings bucketSettings = cluster.buckets().getBucket(name);
        Map<String, Object> map = Mapper.convertValue(
                BucketSettingsDto.extractBucketSettings(bucketSettings), MAP_TYPE_REFERENCE);
        CouchbaseListResultSet resultSet = new CouchbaseListResultSet(singletonList(singletonMap(ROW_NAME, map)));
        resultSet.setMetadata(new CouchbaseResultSetMetaData(singletonList(createColumn(ROW_NAME, "map"))));
        return new ExecutionResult(true, resultSet);
    }

    private static ExecutionResult describeColumns(CouchbaseConnection connection, String schema, String name)
            throws SQLException {
        CouchbaseDocumentsSampler documentsSampler = new CouchbaseDocumentsSampler(connection);
        List<Map<String, Object>> infos = documentsSampler.sample(new TableInfo(name, schema))
                .stream()
                .map(it -> Mapper.convertValue(it, MAP_TYPE_REFERENCE))
                .map(it -> singletonMap(ROW_NAME, (Object) it))
                .collect(Collectors.toList());
        CouchbaseListResultSet resultSet = new CouchbaseListResultSet(infos);
        resultSet.setMetadata(new CouchbaseResultSetMetaData(singletonList(createColumn(ROW_NAME, "map"))));
        return new ExecutionResult(true, resultSet);
    }
}
