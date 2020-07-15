package com.intellij.meta;

public class TableInfo {
    private final String name;
    private final String schema;

    public TableInfo(String name, String schema) {
        this.name = name;
        this.schema = schema;
    }

    public String getSchema() {
        return schema;
    }

    public String getName() {
        return name;
    }
}
