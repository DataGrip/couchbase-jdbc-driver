package com.intellij.executor;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.manager.GenericManagerRequest;
import com.couchbase.client.core.msg.manager.GenericManagerResponse;
import com.couchbase.client.java.json.JsonObject;
import com.intellij.CouchbaseConnection;
import com.intellij.resultset.CouchbaseListResultSet;
import com.intellij.resultset.CouchbaseResultSetMetaData;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.intellij.executor.CustomDdlExecutor.startsWithIgnoreCase;
import static com.intellij.resultset.CouchbaseResultSetMetaData.createColumn;
import static java.util.Collections.singletonList;
import static java.util.regex.Pattern.CASE_INSENSITIVE;

class DescribeIndexExecutor implements CustomDdlExecutor {
    private static final Pattern DESCRIBE_INDEX_PATTERN = Pattern.compile(
            "^DESCRIBE\\s+INDEXES\\s*;?\\s*", CASE_INSENSITIVE);
    private static final String ROW_NAME = "result";

    @Override
    public boolean mayAccept(@NotNull String sql) {
        return startsWithIgnoreCase(sql, "DESCRIBE INDEX");
    }

    @Override
    public boolean isRequireWriteAccess() {
        return false;
    }

    @Override
    public ExecutionResult execute(@NotNull CouchbaseConnection connection, @NotNull String sql) throws SQLException {
        Matcher matcher = DESCRIBE_INDEX_PATTERN.matcher(sql);
        if (matcher.matches()) {
            return new ExecutionResult(true, doRequest(connection.getCluster().core()));
        }
        return new ExecutionResult(false);
    }

    private static ResultSet doRequest(Core core) throws SQLException {
        try {
            GenericManagerRequest request = new GetIndexDdlRequest(core.context());
            core.send(request);
            GenericManagerResponse response = request.response().get();
            if (!response.status().equals(ResponseStatus.SUCCESS)) {
                throw new SQLException("Failed to retrieve index information: "
                        + "Response status=" + response.status() + " "
                        + "Response body=" + new String(response.content(), StandardCharsets.UTF_8));
            }
            List<Map<String, Object>> rows = JsonObject.fromJson(response.content())
                    .getArray("indexes")
                    .toList()
                    .stream()
                    .map(item -> Collections.singletonMap(ROW_NAME, item))
                    .collect(Collectors.toList());
            CouchbaseListResultSet resultSet = new CouchbaseListResultSet(rows);
            resultSet.setMetadata(new CouchbaseResultSetMetaData(singletonList(createColumn(ROW_NAME, "map"))));
            return resultSet;
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
    }

    public static class GetIndexDdlRequest extends GenericManagerRequest {
        public GetIndexDdlRequest(CoreContext ctx) {
            super(ctx, () -> new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/indexStatus"), false);
        }
    }
}
