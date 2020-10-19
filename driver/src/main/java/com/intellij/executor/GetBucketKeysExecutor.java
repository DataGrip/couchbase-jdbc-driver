package com.intellij.executor;

import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.raw.RawManager;
import com.couchbase.client.java.manager.raw.RawManagerRequest;
import com.couchbase.client.java.manager.raw.RawManagerResponse;
import com.intellij.CouchbaseConnection;
import com.intellij.EscapingUtil;
import com.intellij.resultset.CouchbaseSimpleResultSet;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;

class GetBucketKeysExecutor implements CustomDdlExecutor {
    private static final @Language("regexp")
    String LIMIT = "(\\s*limit (?<limit>\\d+))?";
    private static final @Language("regexp")
    String OFFSET = "(\\s*offset (?<offset>\\d+))?";
    private static final Pattern GET_KEYS_PATTERN = Pattern.compile(
        "GET\\s+KEYS\\s+FROM\\s+" + BUCKET_NAME + LIMIT + OFFSET, CASE_INSENSITIVE);

    @Override
    public boolean mayAccept(@NotNull String sql) {
        return GET_KEYS_PATTERN.matcher(sql).matches();
    }

    @Override
    public boolean isRequireWriteAccess() {
        return false;
    }

    @Override
    public ExecutionResult execute(@NotNull CouchbaseConnection connection, @NotNull String sql) throws SQLException {
        Matcher matcher = GET_KEYS_PATTERN.matcher(sql);
        if (!matcher.matches()) {
            return new ExecutionResult(false);
        }
        String name = EscapingUtil.stripBackquotes(matcher.group("name"));
        if (name == null) throw new SQLException("Bucket name is not specified");
        String schema = matcher.group("schema");
        if (SYSTEM_SCHEMA_COLON.equals(schema)) {
            throw new SQLException("Cannot get keys from bucket in system schema");
        }
        String limit = matcher.group("limit");
        String offset = matcher.group("offset");
        String url = "/pools/default/buckets/" + URLEncoder.encode(name, StandardCharsets.UTF_8) + "/docs?include_docs=false";
        if (limit != null) url += "&limit=" + limit;
        if (offset != null) url += "&skip=" + offset;
        RawManagerResponse response = RawManager.call(connection.getCluster(), RawManagerRequest.get(ServiceType.MANAGER, url)).block();
        if (response != null && response.httpStatus() == 404) throw BucketNotFoundException.forBucket(name);
        else if (response == null || response.httpStatus() != 200) throw new SQLException("Request did not succeed. Http status: " + (response == null ? null : response.httpStatus()));
        JsonObject jsonObject = response.contentAs(JsonObject.class);
        Object rows = jsonObject.get("rows");
        if (!(rows instanceof JsonArray)) {
            throw new SQLException("Result does not contain rows: " + jsonObject.toString());
        }
        return new ExecutionResult(true, new CouchbaseSimpleResultSet(((JsonArray) rows).toList()));
    }
}
