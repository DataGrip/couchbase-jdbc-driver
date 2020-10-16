package com.intellij.executor;

import com.intellij.CouchbaseConnection;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;

import static com.intellij.CouchbaseMetaData.SYSTEM_SCHEMA;

interface CustomDdlExecutor {
    @Language("regexp") String BUCKET_NAME = "(?<schema>(?:[a-zA-Z]+:)?)(?<name>(?:`[0-9a-zA-Z_.%\\-]+`)|(?:[a-zA-Z_][a-zA-Z_\\d]*))";
    String SYSTEM_SCHEMA_COLON = SYSTEM_SCHEMA + ":";

    boolean mayAccept(@NotNull String sql);
    boolean isRequireWriteAccess();
    ExecutionResult execute(@NotNull CouchbaseConnection connection, @NotNull String sql) throws SQLException;
}
