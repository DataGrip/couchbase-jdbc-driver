package com.intellij;

import com.intellij.meta.ColumnInfo;
import com.intellij.types.ColumnTypeHelper;
import org.jetbrains.annotations.NotNull;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.intellij.DriverPropertyInfoHelper.META_SAMPLING_SIZE;
import static com.intellij.DriverPropertyInfoHelper.META_SAMPLING_SIZE_DEFAULT;
import static com.intellij.EscapingUtil.escapeChars;

public class CouchbaseDocumentsSampler {
    private final CouchbaseConnection connection;
    private final int sampleSize;

    CouchbaseDocumentsSampler(@NotNull CouchbaseConnection connection) {
        this.connection = connection;
        int sampleSize = META_SAMPLING_SIZE_DEFAULT;
        try {
            sampleSize = Integer.parseInt(connection.getProperties().getProperty(META_SAMPLING_SIZE));
        } catch (NumberFormatException ignore) { }
        this.sampleSize = sampleSize;
    }

    @SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection", "unchecked"})
    public Collection<ColumnInfo> sample(String table) throws SQLException {
        Map<String, ColumnInfo> columns = new HashMap<>();
        String sql = "SELECT * FROM `" + table + "`";
        try (CouchbaseStatement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                int iteration = 0;
                while (iteration < sampleSize && resultSet.next()) {
                    iteration++;
                    Map<String, Object> resultMap = (Map<String, Object>) resultSet.getObject(1);
                    extractColumns(columns, (Map<String, Object>) resultMap.get(table), null);
                }
            }
        }
        return columns.values();
    }

    @SuppressWarnings("unchecked")
    private void extractColumns(Map<String, ColumnInfo> columns, Map<String, Object> row, String parentName) {
        row.forEach((key, value) -> {
            String name = fullyQualifyName(parentName, key);
            columns.merge(name, createColumn(name, value), (oldValue, newValue) -> {
                if (oldValue.getType() == Types.NULL) {
                    return newValue;
                }
                return oldValue;
            });
            if (value instanceof Map<?, ?>) {
                extractColumns(columns, (Map<String, Object>) value, name);
            }
        });
    }

    private static String fullyQualifyName(String parentName, String name) {
        String qualifier = parentName != null ? (parentName + ".") : "";
        return qualifier + escapeChars(name, '\\', '.');
    }

    private static ColumnInfo createColumn(String name, Object object) {
        String typeName = "string";
        if (object == null) {
            typeName = "null";
        } else if (object instanceof  Map<?, ?>) {
            typeName = "object";
        } else if (object instanceof List<?>) {
            typeName = "array";
        } else if (object instanceof Boolean) {
            typeName = "boolean";
        } else if (object instanceof Float) {
            typeName = "float";
        } else if (object instanceof Double) {
            typeName = "double";
        } else if (object instanceof Number) {
            typeName = "numeric";
        }
        return new ColumnInfo(name, ColumnTypeHelper.getJavaType(typeName), typeName);
    }
}
