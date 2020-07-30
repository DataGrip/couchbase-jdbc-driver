package com.intellij.executor;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.msg.manager.GenericManagerRequest;
import com.couchbase.client.core.msg.manager.GenericManagerResponse;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.intellij.resultset.CouchbaseListResultSet;
import com.intellij.resultset.CouchbaseResultSetMetaData;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.intellij.executor.CustomDdlExecutor.startsWithIgnoreCase;
import static com.intellij.resultset.CouchbaseResultSetMetaData.createColumn;
import static java.util.Collections.singletonList;
import static java.util.regex.Pattern.CASE_INSENSITIVE;

class DescribeIndexExecutor implements CustomDdlExecutor {
    private static final Pattern DESCRIBE_INDEX_PATTERN = Pattern.compile(
            "^DESCRIBE\\s+INDEXES\\s*;?\\s*", CASE_INSENSITIVE);

    @Override
    public boolean mayAccept(String sql) {
        return startsWithIgnoreCase(sql, "DESCRIBE INDEX");
    }

    @Override
    public boolean isRequireWriteAccess() {
        return false;
    }

    @Override
    public ExecutionResult execute(Cluster cluster, String sql) throws SQLException {
        Matcher matcher = DESCRIBE_INDEX_PATTERN.matcher(sql);
        if (matcher.matches()) {
            return new ExecutionResult(true, doRequest(cluster.core()));
        }
        return new ExecutionResult(false);
    }

    private static ResultSet doRequest(Core core) throws SQLException {
        try {
            GenericManagerRequest request = new GetIndexDdlRequest(core.context());
            core.send(request);
            GenericManagerResponse response = request.response().get();
            CouchbaseListResultSet resultSet = new CouchbaseListResultSet(
                    singletonList(JsonObject.fromJson(response.content()).toMap()));
            resultSet.setMetadata(new CouchbaseResultSetMetaData(singletonList(createColumn("result", "map"))));
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
