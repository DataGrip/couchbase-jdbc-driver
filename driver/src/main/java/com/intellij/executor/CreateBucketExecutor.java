package com.intellij.executor;

import com.couchbase.client.core.error.BucketExistsException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.diagnostics.WaitUntilReadyOptions;
import com.couchbase.client.java.manager.bucket.BucketSettings;
import com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions;
import com.couchbase.client.java.manager.query.WatchQueryIndexesOptions;
import com.intellij.CouchbaseConnection;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.intellij.executor.CustomDdlExecutor.startsWithIgnoreCase;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.DOTALL;

public class CreateBucketExecutor implements CustomDdlExecutor {
    private static final Pattern CREATE_BUCKET_PATTERN = Pattern.compile(
            "^CREATE\\s+BUCKET\\s+(?<index>(?:WITH\\s+PRIMARY\\s+INDEX\\s+)?)" +
                    "`(?<name>[0-9a-zA-Z_.%\\-]+)`" +
                    "(?<params>(?:\\s+WITH\\s+\\{.*})?)" +
                    "\\s*;?\\s*", CASE_INSENSITIVE | DOTALL);
    private static final WaitUntilReadyOptions OPTIONS = WaitUntilReadyOptions
            .waitUntilReadyOptions()
            .serviceTypes(new HashSet<>(Arrays.asList(ServiceType.KV, ServiceType.QUERY)));
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
                cluster.waitUntilReady(Duration.ofSeconds(10), OPTIONS);
                cluster.bucket(name).waitUntilReady(Duration.ofSeconds(10), OPTIONS);
            } catch (BucketExistsException ignore) {
                // ignore
            }
            if (!matcher.group("index").isEmpty()) {
                cluster.queryIndexes().createPrimaryIndex(name,
                        CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions()
                                .ignoreIfExists(true)
                                .numReplicas(0));
                cluster.queryIndexes().watchIndexes(name, Collections.emptyList(), Duration.ofSeconds(10),
                        WATCH_PRIMARY);
            }
            return new ExecutionResult(true);
        }
        return new ExecutionResult(false);
    }

    private static BucketSettings createBucketSettings(Matcher matcher, String name) {
        BucketSettings bucketSettings;
        String paramsGroup = matcher.group("params");
        if (!paramsGroup.isEmpty()) {
            String params = paramsGroup.substring(paramsGroup.indexOf("{"), paramsGroup.lastIndexOf("}") + 1);
            params = params.replace("'", "\"");
            bucketSettings = Mapper.decodeInto(params, BucketSettingsDto.class)
                    .injectToBucketSettings(BucketSettings.create(name));
        } else {
            bucketSettings = getDefaultBucketSettings(name);
        }
        return bucketSettings;
    }

    private static BucketSettings getDefaultBucketSettings(String name) {
        return BucketSettings.create(name)
                .numReplicas(0);
    }
}
