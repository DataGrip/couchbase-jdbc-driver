package com.dbschema;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class CouchbaseResultSetMetaData implements ResultSetMetaData {

    private static final String NOT_APPLICABLE_TABLE_NAME = "";
    private static final String NOT_APPLICABLE_CATALOG = "";
    private static final int NOT_APPLICABLE_SCALE = 0;
    private static final int NOT_APPLICABLE_PRECISION = 0;

    private final List<ColumnMetaData> columnMetaData;

    CouchbaseResultSetMetaData(List<ColumnMetaData> columnMetaData) {
        this.columnMetaData = columnMetaData;
    }

    public int findColumn(String columnLabel) {
        for (int i = 0; i < columnMetaData.size(); i++) {
            if (columnMetaData.get(i).name.equals(columnLabel)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getColumnCount() {
        return this.columnMetaData.size();
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int isNullable(int column) {
        return ResultSetMetaData.columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getColumnLabel(int column) {
        return columnMetaData.get(column - 1).name;
    }

    @Override
    public String getColumnName(int column) {
        return columnMetaData.get(column - 1).name;
    }

    @Override
    public String getSchemaName(int column) {
        return getCatalogName(column);
    }

    @Override
    public int getPrecision(int column) {
        return NOT_APPLICABLE_PRECISION;
    }

    @Override
    public int getScale(int column) {
        return NOT_APPLICABLE_SCALE;
    }

    @Override
    public String getTableName(int column) {
        return NOT_APPLICABLE_TABLE_NAME;
    }

    @Override
    public String getCatalogName(int column) {
        return NOT_APPLICABLE_CATALOG;
    }

    @Override
    public int getColumnType(int column) {
        return columnMetaData.get(column - 1).getJavaType();
    }

    @Override
    public String getColumnTypeName(int column) {
        return columnMetaData.get(column - 1).typeName;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getColumnClassName(int column) {
        return columnMetaData.get(column - 1).getClassName();
    }

    static class ColumnMetaData {
        private static final Map<String, Integer> javaTypeMap = new HashMap<>();
        private static final Map<String, String> typeNameMap = new HashMap<>();

        static {
            javaTypeMap.put("map", Types.JAVA_OBJECT);
            typeNameMap.put("map", "java.util.Map");
        }

        private final String name;
        private final String typeName;

        ColumnMetaData(String name, String typeName) {
            this.name = name;
            this.typeName = typeName;
        }

        int getJavaType() {
            String lower = toLowerCase(typeName);
            if (javaTypeMap.containsKey(lower)) return javaTypeMap.get(lower);
            throw new IllegalArgumentException("Type name is not known: " + lower);
        }

        String getClassName() {
            String lower = toLowerCase(typeName);
            if (typeNameMap.containsKey(lower)) return typeNameMap.get(lower);
            throw new IllegalArgumentException("Type name is not known: " + lower);
        }

        private String toLowerCase(String value) {
            return value.toLowerCase(Locale.ENGLISH);
        }
    }
}
