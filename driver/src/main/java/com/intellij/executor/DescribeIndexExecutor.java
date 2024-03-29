package com.intellij.executor;

import com.couchbase.client.java.http.CouchbaseHttpClient;
import com.intellij.CouchbaseConnection;
import com.intellij.resultset.CouchbaseListResultSet;
import com.intellij.resultset.CouchbaseResultSetMetaData;
import org.jetbrains.annotations.NotNull;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.intellij.executor.IndexCommons.getIndexes;
import static com.intellij.resultset.CouchbaseResultSetMetaData.createColumn;
import static java.util.Collections.singletonList;
import static java.util.regex.Pattern.CASE_INSENSITIVE;

class DescribeIndexExecutor implements CustomDdlExecutor {
    private static final Pattern DESCRIBE_INDEX_PATTERN = Pattern.compile(
            "DESCRIBE\\s+INDEXES", CASE_INSENSITIVE);
    private static final String ROW_NAME = "result";

    @Override
    public boolean mayAccept(@NotNull String sql) {
        return DESCRIBE_INDEX_PATTERN.matcher(sql).matches();
    }

    @Override
    public boolean isRequireWriteAccess() {
        return false;
    }

    @Override
    public ExecutionResult execute(@NotNull CouchbaseConnection connection, @NotNull String sql) throws SQLException {
        Matcher matcher = DESCRIBE_INDEX_PATTERN.matcher(sql);
        if (matcher.matches()) {
            return new ExecutionResult(true, doRequest(connection.getCluster().httpClient()));
        }
        return new ExecutionResult(false);
    }

    private static ResultSet doRequest(CouchbaseHttpClient httpClient) throws SQLException {
        List<Map<String, Object>> rows = getIndexes(httpClient).stream()
                .map(item -> Collections.singletonMap(ROW_NAME, item))
                .collect(Collectors.toList());
        CouchbaseListResultSet resultSet = new CouchbaseListResultSet(rows);
        resultSet.setMetadata(new CouchbaseResultSetMetaData(singletonList(createColumn(ROW_NAME, "map"))));
        return resultSet;
    }
}
