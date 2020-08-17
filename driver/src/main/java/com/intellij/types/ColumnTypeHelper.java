package com.intellij.types;

import java.sql.Types;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class ColumnTypeHelper {
    private static final Map<String, Integer> javaTypeMap = new HashMap<>();
    private static final Map<String, String> typeNameMap = new HashMap<>();

    static {
        javaTypeMap.put("map", Types.JAVA_OBJECT);
        javaTypeMap.put("object", Types.JAVA_OBJECT);
        javaTypeMap.put("array", Types.ARRAY);
        javaTypeMap.put("numeric", Types.NUMERIC);
        javaTypeMap.put("int", Types.INTEGER);
        javaTypeMap.put("integer", Types.INTEGER);
        javaTypeMap.put("long", Types.BIGINT);
        javaTypeMap.put("short", Types.INTEGER);
        javaTypeMap.put("boolean", Types.BOOLEAN);
        javaTypeMap.put("string", Types.VARCHAR);
        javaTypeMap.put("null", Types.NULL);
        javaTypeMap.put("float", Types.FLOAT);
        javaTypeMap.put("double", Types.DOUBLE);


        typeNameMap.put("map", "java.util.Map");
        typeNameMap.put("object", "java.util.Map");
        typeNameMap.put("array", "java.util.List");
        typeNameMap.put("numeric", "java.lang.Long");
        typeNameMap.put("int", "java.lang.Integer");
        typeNameMap.put("integer", "java.lang.Integer");
        typeNameMap.put("long", "java.lang.Long");
        typeNameMap.put("short", "java.lang.Short");
        typeNameMap.put("boolean", "java.lang.Boolean");
        typeNameMap.put("string", "java.lang.String");
        typeNameMap.put("null", "java.lang.Object");
        typeNameMap.put("float", "java.lang.Float");
        typeNameMap.put("double", "java.lang.Double");
    }

    public static int getJavaType(String typeName) {
        String lower = toLowerCase(typeName);
        if (javaTypeMap.containsKey(lower)) {
            return javaTypeMap.get(lower);
        }
        throw new IllegalArgumentException("Type name is not known: " + lower);
    }

    public static String getClassName(String typeName) {
        String lower = toLowerCase(typeName);
        if (typeNameMap.containsKey(lower)) {
            return typeNameMap.get(lower);
        }
        throw new IllegalArgumentException("Type name is not known: " + lower);
    }

    private static String toLowerCase(String value) {
        return value.toLowerCase(Locale.ENGLISH);
    }
}
