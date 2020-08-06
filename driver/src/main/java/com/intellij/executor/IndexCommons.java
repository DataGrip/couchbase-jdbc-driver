package com.intellij.executor;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.error.IndexesNotReadyException;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.manager.GenericManagerRequest;
import com.couchbase.client.core.msg.manager.GenericManagerResponse;
import com.couchbase.client.core.retry.reactor.Retry;
import com.couchbase.client.core.retry.reactor.RetryExhaustedException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.intellij.meta.IndexInfo;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.CbThrowables.findCause;
import static com.couchbase.client.core.util.CbThrowables.hasCause;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

class IndexCommons {
    static void waitUntilReady(Cluster cluster, String bucketName, Duration timeout) {
        waitInner(timeout, () -> failIfIndexesOffline(cluster.core(), bucketName));
    }

    static void waitUntilOffline(Cluster cluster, String bucketName, Duration timeout) {
        waitInner(timeout, () -> failIfIndexesPresent(cluster.core(), bucketName));
    }

    private static void waitInner(Duration timeout, Runnable runnable) {
        Mono.fromRunnable(runnable)
                .retryWhen(Retry.onlyIf(ctx -> hasCause(ctx.exception(), IndexesNotReadyException.class))
                        .exponentialBackoff(Duration.ofMillis(50), Duration.ofSeconds(3))
                        .timeout(timeout))
                .onErrorMap(t -> t instanceof RetryExhaustedException ? toWatchTimeoutException(t, timeout) : t)
                .block();
    }

    private static TimeoutException toWatchTimeoutException(Throwable t, Duration timeout) {
        final StringBuilder msg = new StringBuilder("A requested index is still not ready after " + timeout + ".");

        findCause(t, IndexesNotReadyException.class).ifPresent(cause ->
                msg.append(" Unready index name -> state: ").append(redactMeta(cause.indexNameToState())));

        return new TimeoutException(msg.toString());
    }

    private static void failIfIndexesPresent(Core core, String bucketName) {
        List<Object> list;
        try {
            list = getIndexes(core);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        Map<String, String> matchingIndexes = list.stream()
                .filter(i -> i instanceof Map<?, ?>)
                .map(i -> IndexInfo.create((Map<?, ?>) i))
                .filter(i -> bucketName.equals(i.bucket) && "#primary".equals(i.name))
                .collect(toMap(IndexInfo::getQualified, IndexInfo::getStatus));

        if (!matchingIndexes.isEmpty()) {
            throw new IndexesNotReadyException(matchingIndexes);
        }
    }

    private static void failIfIndexesOffline(Core core, String bucketName)
            throws IndexesNotReadyException {
        List<Object> list;
        try {
            list = getIndexes(core);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        List<IndexInfo> matchingIndexes = list.stream()
                .filter(i -> i instanceof Map<?, ?>)
                .map(i -> IndexInfo.create((Map<?, ?>) i))
                .filter(i -> bucketName.equals(i.bucket) && "#primary".equals(i.name))
                .collect(toList());

        if (matchingIndexes.isEmpty()) {
            throw new IndexesNotReadyException(singletonMap("#primary", "notFound"));
        }

        final Map<String, String> offlineIndexNameToState = matchingIndexes.stream()
                .filter(idx -> !"Ready".equals(idx.status))
                .collect(toMap(IndexInfo::getQualified, IndexInfo::getStatus));

        if (!offlineIndexNameToState.isEmpty()) {
            throw new IndexesNotReadyException(offlineIndexNameToState);
        }
    }

    static List<Object> getIndexes(Core core) throws SQLException {
        try {
            GenericManagerRequest request = new GetIndexDdlRequest(core.context());
            core.send(request);
            GenericManagerResponse response = request.response().get();
            if (!response.status().equals(ResponseStatus.SUCCESS)) {
                throw new SQLException("Failed to retrieve index information: "
                        + "Response status=" + response.status() + " "
                        + "Response body=" + new String(response.content(), StandardCharsets.UTF_8));
            }
            return JsonObject.fromJson(response.content())
                    .getArray("indexes")
                    .toList();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
    }

    static class GetIndexDdlRequest extends GenericManagerRequest {
        public GetIndexDdlRequest(CoreContext ctx) {
            super(ctx, () -> new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/indexStatus"), false);
        }
    }
}
