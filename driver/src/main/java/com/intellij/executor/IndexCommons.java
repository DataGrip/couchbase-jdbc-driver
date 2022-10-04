package com.intellij.executor;

import com.couchbase.client.core.error.IndexesNotReadyException;
import com.couchbase.client.core.retry.reactor.Retry;
import com.couchbase.client.core.retry.reactor.RetryExhaustedException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.http.CouchbaseHttpClient;
import com.couchbase.client.java.http.HttpPath;
import com.couchbase.client.java.http.HttpResponse;
import com.couchbase.client.java.http.HttpTarget;
import com.couchbase.client.java.json.JsonObject;
import com.intellij.meta.IndexInfo;
import reactor.core.publisher.Mono;

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
        waitInner(timeout, () -> failIfIndexesOffline(cluster.httpClient(), bucketName));
    }

    static void waitUntilOffline(Cluster cluster, String bucketName, Duration timeout) {
        waitInner(timeout, () -> failIfIndexesPresent(cluster.httpClient(), bucketName));
    }

    private static void waitInner(Duration timeout, Runnable runnable) {
        Mono.fromRunnable(runnable)
                .retryWhen(Retry.onlyIf(ctx -> hasCause(ctx.exception(), IndexesNotReadyException.class))
                    .exponentialBackoff(Duration.ofMillis(50), Duration.ofSeconds(3))
                    .timeout(timeout)
                    .toReactorRetry())
                .onErrorMap(t -> t instanceof RetryExhaustedException ? toWatchTimeoutException(t, timeout) : t)
                .block();
    }

    private static TimeoutException toWatchTimeoutException(Throwable t, Duration timeout) {
        final StringBuilder msg = new StringBuilder("A requested index is still not ready after " + timeout + ".");

        findCause(t, IndexesNotReadyException.class).ifPresent(cause ->
                msg.append(" Unready index name -> state: ").append(redactMeta(cause.indexNameToState())));

        return new TimeoutException(msg.toString());
    }

    private static void failIfIndexesPresent(CouchbaseHttpClient httpClient, String bucketName) {
        Map<String, String> matchingIndexes = getMatchingIndexInfo(httpClient, bucketName)
                .stream()
                .collect(toMap(IndexInfo::getQualified, IndexInfo::getStatus));

        if (!matchingIndexes.isEmpty()) {
            throw new IndexesNotReadyException(matchingIndexes);
        }
    }

    private static void failIfIndexesOffline(CouchbaseHttpClient httpClient, String bucketName)
            throws IndexesNotReadyException {
        List<IndexInfo> matchingIndexes = getMatchingIndexInfo(httpClient, bucketName);
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

    static List<Object> getIndexes(CouchbaseHttpClient httpClient) throws SQLException {
        try {
            HttpResponse response = httpClient.get(HttpTarget.manager(), HttpPath.of("/indexStatus"));
            if (!response.success()) {
                throw new SQLException("Failed to retrieve index information: "
                        + "Response status=" + response.statusCode() + " "
                        + "Response body=" + response.contentAsString());
            }
            return JsonObject.fromJson(response.content())
                    .getArray("indexes")
                    .toList();
        } catch (SQLException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
    }

    private static List<IndexInfo> getMatchingIndexInfo(CouchbaseHttpClient httpClient, String bucketName) {
        List<Object> list;
        try {
            list = getIndexes(httpClient);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return list.stream()
                .filter(i -> i instanceof Map<?, ?>)
                .map(i -> IndexInfo.create((Map<?, ?>) i))
                .filter(i -> bucketName.equals(i.bucket) && "#primary".equals(i.name))
                .collect(toList());
    }
}
