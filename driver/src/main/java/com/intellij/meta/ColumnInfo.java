package com.intellij.meta;

import java.util.Objects;

public class ColumnInfo {
    private final int type;
    private final String typeName;
    private final String name;

    public ColumnInfo(String name, int type, String typeName) {
        this.type = type;
        this.typeName = typeName;
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnInfo)) {
            return false;
        }
        ColumnInfo that = (ColumnInfo) o;
        return Objects.equals(getName(), that.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName());
    }
}
