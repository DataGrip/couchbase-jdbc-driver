package com.intellij.executor;

import com.couchbase.client.core.error.*;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.retry.reactor.Retry;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.bucket.BucketSettings;
import com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions;
import com.couchbase.client.java.manager.query.WatchQueryIndexesOptions;
import com.intellij.CouchbaseConnection;
import com.intellij.CouchbaseError;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.couchbase.client.core.util.CbThrowables.findCause;
import static com.couchbase.client.core.util.CbThrowables.hasCause;
import static com.intellij.executor.CustomDdlExecutor.startsWithIgnoreCase;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.DOTALL;

public class CreateBucketExecutor implements CustomDdlExecutor {
    private static final Pattern CREATE_BUCKET_PATTERN = Pattern.compile(
            "^CREATE\\s+BUCKET\\s+(?<index>(?:WITH\\s+PRIMARY\\s+INDEX\\s+)?)" +
                    "`(?<name>[0-9a-zA-Z_.%\\-]+)`" +
                    "(?<params>(?:\\s+WITH\\s+\\{.*})?)" +
                    "\\s*;?\\s*", CASE_INSENSITIVE | DOTALL);
    private static final WatchQueryIndexesOptions WATCH_PRIMARY = WatchQueryIndexesOptions
            .watchQueryIndexesOptions()
            .watchPrimary(true);

    @Override
    public boolean mayAccept(@NotNull String sql) {
        return startsWithIgnoreCase(sql, "CREATE BUCKET");
    }

    @Override
    public boolean isRequireWriteAccess() {
        return true;
    }

    @Override
    public ExecutionResult execute(@NotNull CouchbaseConnection connection, @NotNull String sql) {
        Matcher matcher = CREATE_BUCKET_PATTERN.matcher(sql);
        if (matcher.matches()) {
            Cluster cluster = connection.getCluster();
            String name = matcher.group("name");
            try {
                BucketSettings bucketSettings = createBucketSettings(matcher, name);
                cluster.buckets().createBucket(bucketSettings);
            } catch (BucketExistsException ignore) {
                // ignore
            }
            if (!matcher.group("index").isEmpty()) {
                Mono.fromRunnable(() -> createIndex(cluster, name))
                        .retryWhen(Retry.onlyIf(ctx ->
                                findCause(ctx.exception(), InternalServerFailureException.class)
                                    .filter(exception -> CouchbaseError.create(exception)
                                        .getErrorEntries().stream()
                                        .anyMatch(err -> err.getMessage().contains("GSI")))
                                    .isPresent())
                                .exponentialBackoff(Duration.ofMillis(50), Duration.ofSeconds(3))
                                .timeout(Duration.ofSeconds(15)))
                        .block();
                Mono.fromRunnable(() -> waitForIndex(cluster, name))
                        .retryWhen(Retry.onlyIf(ctx -> hasCause(ctx.exception(), IndexNotFoundException.class))
                                .exponentialBackoff(Duration.ofMillis(50), Duration.ofSeconds(3))
                                .timeout(Duration.ofSeconds(15)))
                        .block();
                IndexCommons.waitUntilReady(cluster, name, Duration.ofSeconds(60));
            }
            return new ExecutionResult(true);
        }
        return new ExecutionResult(false);
    }

    private void createIndex(Cluster cluster, String bucketName) {
        cluster.queryIndexes().createPrimaryIndex(bucketName,
                CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions()
                        .ignoreIfExists(true)
                        .numReplicas(0));
    }

    private void waitForIndex(Cluster cluster, String bucketName) {
        cluster.queryIndexes().watchIndexes(bucketName, Collections.singletonList("#primary"),
                Duration.ofSeconds(10), WATCH_PRIMARY);
    }

    private static BucketSettings createBucketSettings(Matcher matcher, String name) {
        BucketSettings bucketSettings = BucketSettings.create(name);
        String paramsGroup = matcher.group("params");
        if (!paramsGroup.isEmpty()) {
            String params = paramsGroup.substring(paramsGroup.indexOf("{"), paramsGroup.lastIndexOf("}") + 1);
            params = params.replace("'", "\"");
            Mapper.decodeInto(params, BucketSettingsDto.class)
                    .injectToBucketSettings(bucketSettings);
        }
        return bucketSettings;
    }
}
