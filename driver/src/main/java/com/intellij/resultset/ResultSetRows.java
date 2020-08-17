package com.intellij.resultset;

import java.util.Iterator;

public interface ResultSetRows<T> extends Iterator<T> {
    void close();
}
