package com.intellij.executor;

import com.couchbase.client.java.Cluster;
import org.jetbrains.annotations.NotNull;

interface CustomDdlExecutor {
    boolean mayAccept(String sql);
    boolean execute(Cluster cluster, String sql);

    static boolean startsWithIgnoreCase(@NotNull String str, @NotNull String prefix) {
        int stringLength = str.length();
        int prefixLength = prefix.length();
        return stringLength >= prefixLength && str.regionMatches(true, 0, prefix, 0, prefixLength);
    }
}
