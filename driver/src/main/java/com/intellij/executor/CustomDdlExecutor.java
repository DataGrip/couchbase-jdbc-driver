package com.intellij.executor;

import com.intellij.CouchbaseConnection;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;

interface CustomDdlExecutor {
    @Language("regexp") String IDENTIFIER = "`[0-9a-zA-Z_.%\\-]+`|[a-zA-Z_][a-zA-Z_\\d]*";
    @Language("regexp") String BUCKET_NAME = "(?:(?<schema>[a-zA-Z]+):)?(?<bucket>" + IDENTIFIER + ")";

    boolean mayAccept(@NotNull String sql);
    boolean isRequireWriteAccess();
    ExecutionResult execute(@NotNull CouchbaseConnection connection, @NotNull String sql) throws SQLException;
}
