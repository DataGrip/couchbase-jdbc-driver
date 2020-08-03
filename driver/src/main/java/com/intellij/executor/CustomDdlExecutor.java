package com.intellij.executor;

import com.intellij.CouchbaseConnection;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;

interface CustomDdlExecutor {
    boolean mayAccept(@NotNull String sql);
    boolean isRequireWriteAccess();
    ExecutionResult execute(@NotNull CouchbaseConnection connection, @NotNull String sql) throws SQLException;

    static boolean startsWithIgnoreCase(@NotNull String str, @NotNull String prefix) {
        int stringLength = str.length();
        int prefixLength = prefix.length();
        return stringLength >= prefixLength && str.regionMatches(true, 0, prefix, 0, prefixLength);
    }
}
