package com.intellij.meta;

import java.util.Map;

import static com.intellij.ObjectUtil.tryCast;

public class IndexInfo {
    public final String name;
    public final String bucket;
    public final String status;
    public final String lastScanTime;

    public IndexInfo(String name, String bucket, String status, String lastScanTime) {
        this.name = name;
        this.bucket = bucket;
        this.status = status;
        this.lastScanTime = lastScanTime;
    }

    public static IndexInfo create(Map<?, ?> asMap) {
        String name = tryCast(asMap.get("index"), String.class);
        String bucket = tryCast(asMap.get("bucket"), String.class);
        String status = tryCast(asMap.get("status"), String.class);
        String lastScanTime = tryCast(asMap.get("lastScanTime"), String.class);
        return new IndexInfo(name, bucket, status, lastScanTime);
    }

    public String getName() {
        return name;
    }

    public String getBucket() {
        return bucket;
    }

    public String getStatus() {
        return status;
    }

    public String getQualified() {
        return bucket + ":" + name;
    }

    @Override
    public String toString() {
        return "IndexInfo{" +
                "name='" + name + '\'' +
                ", bucket='" + bucket + '\'' +
                ", status='" + status + '\'' +
                ", lastScanTime='" + lastScanTime + '\'' +
                '}';
    }
}
