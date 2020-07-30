package com.intellij.executor;

import com.couchbase.client.core.error.BucketExistsException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.diagnostics.WaitUntilReadyOptions;
import com.couchbase.client.java.manager.bucket.*;
import com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions;
import com.couchbase.client.java.manager.query.WatchQueryIndexesOptions;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.intellij.executor.CustomDdlExecutor.startsWithIgnoreCase;
import static java.util.regex.Pattern.CASE_INSENSITIVE;

public class CreateBucketExecutor implements CustomDdlExecutor {
    private static final Pattern CREATE_BUCKET_PATTERN = Pattern.compile(
            "^CREATE\\s+BUCKET\\s+(?<index>(?:WITH\\s+PRIMARY\\s+INDEX\\s+)?)" +
                    "`(?<name>[0-9a-zA-Z_.%\\-]+)`" +
                    "(?<params>(?:\\s+WITH\\s+\\{.*})?)" +
                    "\\s*;?\\s*", CASE_INSENSITIVE);
    private static final WaitUntilReadyOptions OPTIONS = WaitUntilReadyOptions
            .waitUntilReadyOptions()
            .serviceTypes(new HashSet<>(Arrays.asList(ServiceType.KV, ServiceType.QUERY)));
    private static final WatchQueryIndexesOptions WATCH_PRIMARY = WatchQueryIndexesOptions
            .watchQueryIndexesOptions()
            .watchPrimary(true);

    @Override
    public boolean mayAccept(String sql) {
        return startsWithIgnoreCase(sql, "CREATE BUCKET");
    }

    @Override
    public boolean execute(Cluster cluster, String sql) {
        Matcher matcher = CREATE_BUCKET_PATTERN.matcher(sql);
        if (matcher.matches()) {
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
            return true;
        }
        return false;
    }

    private static BucketSettings createBucketSettings(Matcher matcher, String name) {
        BucketSettings bucketSettings;
        String paramsGroup = matcher.group("params");
        if (!paramsGroup.isEmpty()) {
            String params = paramsGroup.substring(paramsGroup.indexOf("{"), paramsGroup.lastIndexOf("}") + 1);
            bucketSettings = Mapper.decodeInto(params, CreateBucketSettings.class)
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

    @SuppressWarnings("unused")
    private static class CreateBucketSettings {
        private Boolean flushEnabled;
        private Long ramQuotaMB;
        private Integer replicaNumber;
        private Boolean replicaIndexes;
        private Integer maxTTL;
        private CompressionMode compressionMode;
        private BucketType bucketType;
        private ConflictResolutionType conflictResolutionType;
        private EjectionPolicy evictionPolicy;

        BucketSettings injectToBucketSettings(BucketSettings bucketSettings) {
            if (flushEnabled != null) {
                bucketSettings.flushEnabled(flushEnabled);
            }
            if (ramQuotaMB != null) {
                bucketSettings.ramQuotaMB(ramQuotaMB);
            }
            if (replicaNumber != null) {
                bucketSettings.numReplicas(replicaNumber);
            }
            if (replicaIndexes != null) {
                bucketSettings.replicaIndexes(replicaIndexes);
            }
            if (maxTTL != null) {
                bucketSettings.maxTTL(maxTTL);
            }
            if (compressionMode != null) {
                bucketSettings.compressionMode(compressionMode);
            }
            if (bucketType != null) {
                bucketSettings.bucketType(bucketType);
            }
            if (conflictResolutionType != null) {
                bucketSettings.conflictResolutionType(conflictResolutionType);
            }
            if (evictionPolicy != null) {
                bucketSettings.ejectionPolicy(evictionPolicy);
            }
            return bucketSettings;
        }

        public Boolean getFlushEnabled() {
            return flushEnabled;
        }

        public void setFlushEnabled(Boolean flushEnabled) {
            this.flushEnabled = flushEnabled;
        }

        public Long getRamQuotaMB() {
            return ramQuotaMB;
        }

        public void setRamQuotaMB(Long ramQuotaMB) {
            this.ramQuotaMB = ramQuotaMB;
        }

        public Integer getReplicaNumber() {
            return replicaNumber;
        }

        public void setReplicaNumber(Integer replicaNumber) {
            this.replicaNumber = replicaNumber;
        }

        public Boolean getReplicaIndexes() {
            return replicaIndexes;
        }

        public void setReplicaIndexes(Boolean replicaIndexes) {
            this.replicaIndexes = replicaIndexes;
        }

        public Integer getMaxTTL() {
            return maxTTL;
        }

        public void setMaxTTL(Integer maxTTL) {
            this.maxTTL = maxTTL;
        }

        public CompressionMode getCompressionMode() {
            return compressionMode;
        }

        public void setCompressionMode(CompressionMode compressionMode) {
            this.compressionMode = compressionMode;
        }

        public BucketType getBucketType() {
            return bucketType;
        }

        public void setBucketType(BucketType bucketType) {
            this.bucketType = bucketType;
        }

        public ConflictResolutionType getConflictResolutionType() {
            return conflictResolutionType;
        }

        public void setConflictResolutionType(ConflictResolutionType conflictResolutionType) {
            this.conflictResolutionType = conflictResolutionType;
        }

        public EjectionPolicy getEvictionPolicy() {
            return evictionPolicy;
        }

        public void setEvictionPolicy(EjectionPolicy evictionPolicy) {
            this.evictionPolicy = evictionPolicy;
        }

        @Override
        public String toString() {
            return "CreateBucketSettings{" +
                    "flushEnabled=" + flushEnabled +
                    ", ramQuotaMB=" + ramQuotaMB +
                    ", replicaNumber=" + replicaNumber +
                    ", replicaIndexes=" + replicaIndexes +
                    ", maxTTL=" + maxTTL +
                    ", compressionMode=" + compressionMode +
                    ", bucketType=" + bucketType +
                    ", conflictResolutionType=" + conflictResolutionType +
                    ", evictionPolicy=" + evictionPolicy +
                    '}';
        }
    }
}
