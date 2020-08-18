package com.intellij.executor;

import com.couchbase.client.java.manager.bucket.*;

import java.time.Duration;

class BucketSettingsDto {
    public Boolean flushEnabled;
    public Long ramQuotaMB;
    public Integer replicaNumber;
    public Boolean replicaIndexes;
    public Integer maxTTL;
    public CompressionMode compressionMode;
    public BucketType bucketType;
    public ConflictResolutionType conflictResolutionType;
    public EvictionPolicyType evictionPolicy;

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
            bucketSettings.maxExpiry(Duration.ofSeconds(maxTTL));
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
            bucketSettings.evictionPolicy(evictionPolicy);
        }
        return bucketSettings;
    }

    static BucketSettingsDto extractBucketSettings(BucketSettings bucketSettings) {
        BucketSettingsDto dto = new BucketSettingsDto();
        dto.flushEnabled = bucketSettings.flushEnabled();
        dto.ramQuotaMB = bucketSettings.ramQuotaMB();
        dto.replicaNumber = bucketSettings.numReplicas();
        dto.replicaIndexes = bucketSettings.replicaIndexes();
        dto.maxTTL = (int) bucketSettings.maxExpiry().getSeconds();
        dto.compressionMode = bucketSettings.compressionMode();
        dto.bucketType = bucketSettings.bucketType();
        dto.conflictResolutionType = bucketSettings.conflictResolutionType();
        dto.evictionPolicy = bucketSettings.evictionPolicy();
        return dto;
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
