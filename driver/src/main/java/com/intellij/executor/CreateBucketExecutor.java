package com.intellij.executor;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonParser;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.MapperFeature;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.json.JsonMapper;
import com.couchbase.client.core.error.BucketExistsException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.error.InternalServerFailureException;
import com.couchbase.client.core.retry.reactor.Retry;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.http.HttpPath;
import com.couchbase.client.java.http.HttpResponse;
import com.couchbase.client.java.http.HttpTarget;
import com.couchbase.client.java.manager.bucket.BucketSettings;
import com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions;
import com.couchbase.client.java.manager.query.WatchQueryIndexesOptions;
import com.intellij.CouchbaseConnection;
import com.intellij.CouchbaseError;
import com.intellij.EscapingUtil;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.couchbase.client.core.util.CbThrowables.findCause;
import static com.couchbase.client.core.util.CbThrowables.hasCause;
import static com.intellij.CouchbaseMetaData.SYSTEM_SCHEMA;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.DOTALL;

public class CreateBucketExecutor implements CustomDdlExecutor {
    private static final String WITH_INDEX = "(?<index>\\s+WITH\\s+PRIMARY\\s+INDEX)?";
    private static final String WAIT_UNTIL_READY = "(?<wait>\\s+WAIT\\s+UNTIL\\s+READY)?";
    private static final Pattern CREATE_BUCKET_PATTERN = Pattern.compile(
        "CREATE\\s+(BUCKET|TABLE)" + WITH_INDEX + "\\s+" + BUCKET_NAME + WAIT_UNTIL_READY +
            "(?<params>(?:\\s+WITH\\s+\\{.*})?)", CASE_INSENSITIVE | DOTALL);
    private static final WatchQueryIndexesOptions WATCH_PRIMARY = WatchQueryIndexesOptions
        .watchQueryIndexesOptions()
        .watchPrimary(true);
    private static final JsonMapper MAPPER = JsonMapper.builder()
        .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
        .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
        .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
        .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true)
        .build();
    private static final String DEFAULT_INDEX_NAME = "#primary";

    @Override
    public boolean mayAccept(@NotNull String sql) {
        return CREATE_BUCKET_PATTERN.matcher(sql).matches();
    }

    @Override
    public boolean isRequireWriteAccess() {
        return true;
    }

    @Override
    public ExecutionResult execute(@NotNull CouchbaseConnection connection, @NotNull String sql) throws SQLException {
        Matcher matcher = CREATE_BUCKET_PATTERN.matcher(sql);
        if (matcher.matches()) {
            Cluster cluster = connection.getCluster();
            String name = EscapingUtil.stripBackquotes(matcher.group("bucket"));
            String schema = matcher.group("schema");
            if (SYSTEM_SCHEMA.equals(schema)) {
                throw new SQLException("Cannot create bucket in system schema");
            }
            try {
                BucketSettings bucketSettings = createBucketSettings(matcher, name);
                cluster.buckets().createBucket(bucketSettings);
            } catch (BucketExistsException ignore) {
                // ignore
            }
            if (matcher.group("index") != null) {
                Mono.fromRunnable(() -> createIndex(cluster, name))
                        .retryWhen(Retry.onlyIf(ctx ->
                                findCause(ctx.exception(), InternalServerFailureException.class)
                                    .filter(exception -> CouchbaseError.create(exception)
                                        .getErrorEntries().stream()
                                        .anyMatch(err -> err.getMessage().contains("GSI")))
                                    .isPresent())
                                .exponentialBackoff(Duration.ofMillis(50), Duration.ofSeconds(3))
                                .timeout(Duration.ofSeconds(60))
                                .toReactorRetry())
                        .block();
                Mono.fromRunnable(() -> waitForIndex(cluster, name))
                        .retryWhen(Retry.onlyIf(ctx -> hasCause(ctx.exception(), IndexNotFoundException.class))
                                .exponentialBackoff(Duration.ofMillis(50), Duration.ofSeconds(3))
                                .timeout(Duration.ofSeconds(30))
                                .toReactorRetry())
                        .block();
                IndexCommons.waitUntilReady(cluster, name, Duration.ofSeconds(60));
            } else if (matcher.group("wait") != null) {
                waitForBucketSetup(name, connection.getCluster());
            }
            return new ExecutionResult(true);
        }
        return new ExecutionResult(false);
    }

    private void waitForBucketSetup(String name, Cluster cluster) {
        Mono.fromRunnable(() -> {
            HttpResponse response;
            try {
                response = cluster.httpClient().get(HttpTarget.manager(), HttpPath.of("/pools/default/buckets/{}/docs?include_docs=false", name));
            } catch (Exception e) {
                throw new CouchbaseException("Failed to request keys from " + name, e);
            }
            if (!response.success()) {
                throw new GetKeysException("Failed to to request keys from " + name + ": "
                    + "Response status=" + response.statusCode() + " "
                    + "Response body=" + response.contentAsString());
            }
        }).retryWhen(Retry.any()
            .exponentialBackoff(Duration.ofMillis(50), Duration.ofSeconds(3))
            .timeout(Duration.ofSeconds(30))
            .toReactorRetry())
            .block();
    }

    private void createIndex(Cluster cluster, String bucketName) {
        cluster.queryIndexes().createPrimaryIndex(bucketName,
                CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions()
                        .ignoreIfExists(true)
                        .numReplicas(0));
    }

    private void waitForIndex(Cluster cluster, String bucketName) {
        cluster.queryIndexes().watchIndexes(bucketName, Collections.singletonList(DEFAULT_INDEX_NAME),
                Duration.ofSeconds(10), WATCH_PRIMARY);
    }

    private static BucketSettings createBucketSettings(Matcher matcher, String name) throws SQLException {
        BucketSettings bucketSettings = BucketSettings.create(name);
        String paramsGroup = matcher.group("params");
        if (!paramsGroup.isEmpty()) {
            String params = paramsGroup.substring(paramsGroup.indexOf("{"), paramsGroup.lastIndexOf("}") + 1);
            try {
                MAPPER.readValue(params, BucketSettingsDto.class)
                    .injectToBucketSettings(bucketSettings);
            } catch (JsonProcessingException e) {
                throw new SQLException("Could not decode from JSON: " + params, e);
            }
        }
        return bucketSettings;
    }

    private static class GetKeysException extends RuntimeException {
        GetKeysException(String message) {
            super(message);
        }
    }
}
