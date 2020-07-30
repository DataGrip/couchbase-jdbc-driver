package com.intellij.executor;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.bucket.BucketSettings;
import com.intellij.resultset.CouchbaseListResultSet;
import com.intellij.resultset.CouchbaseResultSetMetaData;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.intellij.executor.CustomDdlExecutor.startsWithIgnoreCase;
import static com.intellij.resultset.CouchbaseResultSetMetaData.createColumn;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.regex.Pattern.CASE_INSENSITIVE;

public class DescribeBucketExecutor implements CustomDdlExecutor {
    private static final Pattern DESCRIBE_BUCKET_PATTERN = Pattern.compile(
            "^DESCRIBE\\s+BUCKET\\s+`(?<name>[0-9a-zA-Z_.%\\-]+)`\\s*;?\\s*", CASE_INSENSITIVE);
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE =
            new TypeReference<Map<String, Object>>() {};
    private static final String ROW_NAME = "description";

    @Override
    public boolean mayAccept(String sql) {
        return startsWithIgnoreCase(sql, "DESCRIBE BUCKET");
    }

    @Override
    public boolean isRequireWriteAccess() {
        return false;
    }

    @Override
    public ExecutionResult execute(Cluster cluster, String sql) {
        Matcher matcher = DESCRIBE_BUCKET_PATTERN.matcher(sql);
        if (matcher.matches()) {
            String name = matcher.group("name");
            BucketSettings bucketSettings = cluster.buckets().getBucket(name);
            Map<String, Object> map = Mapper.convertValue(
                    BucketSettingsDto.extractBucketSettings(bucketSettings), MAP_TYPE_REFERENCE);
            CouchbaseListResultSet resultSet = new CouchbaseListResultSet(singletonList(singletonMap(ROW_NAME, map)));
            resultSet.setMetadata(new CouchbaseResultSetMetaData(singletonList(createColumn(ROW_NAME, "map"))));
            return new ExecutionResult(true, resultSet);
        }
        return new ExecutionResult(false);
    }
}
